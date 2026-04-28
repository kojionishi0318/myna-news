"""
マイナ保険証 ニュース取得スクリプト
Google News RSS から最新ニュースを取得して news_data.json に保存します。
"""

import sys
import os
import re
import json
import base64
import datetime
import time
import argparse
import urllib.parse
import html as html_module
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    _USE_REQUESTS = True
except ImportError:
    import urllib.request
    _USE_REQUESTS = False

# ─── 設定 ────────────────────────────────────────────────────────────────────
SEARCH_QUERY    = "マイナ保険証"

# ── Google News RSS：複数クエリで網羅的に取得 ──────────────────────────────
GOOGLE_NEWS_QUERIES = [
    "マイナ保険証",
    "マイナンバーカード 保険証",
    "健康保険証 廃止",
    "マイナカード 医療",
]

def _gnews_url(q: str) -> str:
    return ("https://news.google.com/rss/search?q="
            + urllib.parse.quote(q) + "&hl=ja&gl=JP&ceid=JP:ja")

# ── 直接 RSS フィード（キーワードフィルタあり） ────────────────────────────
FILTER_KEYWORDS = [
    "マイナ保険証", "マイナンバー", "健康保険証", "マイナカード",
    "マイナ", "保険証", "医療保険", "社会保険",
]

DIRECT_RSS_FEEDS = [
    # (URL, ソース名表示)
    ("https://www3.nhk.or.jp/rss/news/cat0.xml",           "NHKニュース"),
    ("https://www3.nhk.or.jp/rss/news/cat4.xml",           "NHK政治"),
    ("https://news.yahoo.co.jp/rss/topics/domestic.xml",   "Yahoo!ニュース"),
    ("https://www.mhlw.go.jp/stf/news.rdf",                 "厚生労働省"),
    ("https://www.digital.go.jp/rss/news.xml",             "デジタル庁"),
]

CONNECT_TIMEOUT  = 5    # TCP接続タイムアウト（秒）
READ_TIMEOUT     = 20   # レスポンス受信タイムアウト（秒）
RESOLVE_TIMEOUT  = 6    # URL解決タイムアウト（秒/記事）
RESOLVE_WORKERS  = 20   # 並列ワーカー数
SUMMARY_TIMEOUT  = 8    # 要約取得タイムアウト（秒/記事）
SUMMARY_WORKERS  = 10   # 要約取得並列ワーカー数

# ─── Bing News RSS（実際の記事スニペットを取得するため） ─────────────────────
BING_RSS_URL = (
    "https://www.bing.com/news/search?q="
    + urllib.parse.quote(SEARCH_QUERY)
    + "&format=rss&setlang=ja-JP&cc=JP"
)
BING_SNIPPET_DELAY = 0.5  # Bingスニペット取得間隔（秒）
BING_SNIPPET_WORKERS = 5  # 並列ワーカー数（レート制限対策）
FRESHNESS_SECS   = 3600 # 1時間以内なら再取得をスキップ
MAX_ARTICLES     = 500  # 保持する最大記事数
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
# ─────────────────────────────────────────────────────────────────────────────

def _now_jst():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

def _iso_jst(dt=None):
    dt = dt or _now_jst()
    return dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")


# ── 鮮度チェック ──────────────────────────────────────────────────────────────
def is_fresh(json_path: str) -> bool:
    if not os.path.exists(json_path):
        return False
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        updated = datetime.datetime.fromisoformat(data.get("updated", ""))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=datetime.timezone.utc)
        return (_now_jst() - updated).total_seconds() < FRESHNESS_SECS
    except Exception:
        return False


# ── Google News URLのBase64デコード ──────────────────────────────────────────
def decode_gnews_url(url: str) -> str:
    """
    Google News RSS の記事URLからBase64デコードで実記事URLを抽出する。
    フォーマット: https://news.google.com/rss/articles/<base64>
    デコード結果はProtobuf風バイナリで、その中にhttps://が埋め込まれている。
    """
    try:
        parsed = urllib.parse.urlparse(url)
        if "news.google.com" not in parsed.netloc:
            return url
        path_parts = [p for p in parsed.path.split("/") if p]
        if not path_parts or path_parts[-2] not in ("articles", "read"):
            return url
        article_id = path_parts[-1]
        # パディング補完
        article_id += "=" * ((4 - len(article_id) % 4) % 4)
        decoded = base64.urlsafe_b64decode(article_id)
        # バイナリ内のhttps?://を探す（UTF-8/ASCII混在）
        match = re.search(rb"https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+", decoded)
        if match:
            candidate = match.group(0).decode("ascii", errors="replace")
            # google.com 以外なら実記事URL
            if "google.com" not in candidate and len(candidate) > 15:
                return candidate
    except Exception:
        pass
    return url



# ── URL解決（Google News リダイレクト → 実記事URL） ──────────────────────────
def _resolve_one(url: str, session: "requests.Session") -> str:
    """
    記事 URL を実際の記事 URL に解決する。
    - Google News URL: Base64 デコードを試みる（高速・失敗しても即返却）
    - Bing redirect URL: HTTP リダイレクトを追跡（確実に解決できる）
    """
    if "news.google.com" in url:
        # Base64 デコードのみ試みる（HTTPフォールバックはしない：遅すぎるため）
        return decode_gnews_url(url)

    if "bing.com/news/apiclick" in url:
        # Bing のリダイレクトは単純な HTTP redirect で追跡可能
        try:
            resp = session.get(
                url,
                timeout=RESOLVE_TIMEOUT,
                allow_redirects=True,
                stream=True,
                headers={"User-Agent": UA},
            )
            final_url = resp.url
            resp.close()
            if "bing.com" not in final_url and "msn.com" not in final_url:
                return final_url
            # MSN/Bing のまま → そのまま使用（MSN版は有効なURL）
            return final_url
        except Exception:
            return url

    return url


def resolve_urls(articles: list[dict], session: "requests.Session", label: str = "") -> list[dict]:
    """
    Google News URL を持つ記事を並列で解決する。
    各記事に _glink（元の Google URL）を保存し、link を実記事 URL に更新する。
    """
    targets = [
        (i, a) for i, a in enumerate(articles)
        if "news.google.com" in a.get("link", "")
    ]
    if not targets:
        return articles

    t0 = time.perf_counter()
    resolved = ok = 0

    with ThreadPoolExecutor(max_workers=RESOLVE_WORKERS) as ex:
        future_map = {
            ex.submit(_resolve_one, a["link"], session): (i, a["link"])
            for i, a in targets
        }
        for future in as_completed(future_map):
            idx, orig = future_map[future]
            result = future.result()
            articles[idx]["_glink"]        = orig    # 元の Google URL を保持
            articles[idx]["link"]          = result  # 実記事 URL で上書き
            articles[idx]["_resolved_url"] = result  # サマリー取得用にも保持
            resolved += 1
            if "news.google.com" not in result:
                ok += 1

    elapsed = time.perf_counter() - t0
    tag = f"  [{label}]" if label else "  [URL解決]"
    print(f"{tag} {elapsed:.1f} s  {ok}/{resolved} 件解決")
    return articles


# ── Bing News RSS 取得・エンリッチ ──────────────────────────────────────────
def _norm_title(title: str) -> str:
    """タイトル正規化（マッチング用）: 先頭30文字・小文字・空白/句読点除去"""
    # " - 媒体名" " | 媒体名" を末尾から除去
    t = re.sub(r"\s*[-|｜]\s*[^\s].{0,20}$", "", title)
    # 小文字化・空白類除去・句読点除去
    t = t.lower()
    t = re.sub(r"[\s\u3000\u00a0\.\。\、\,\!\?\！\？]", "", t)
    return t[:30]


def fetch_bing_descriptions(session: "requests.Session") -> dict:
    """
    Bing News RSS から記事説明文を取得し、{正規化タイトル: description} の辞書を返す。
    Google News 記事の description エンリッチに使用する。
    """
    t0 = time.perf_counter()
    try:
        resp = session.get(BING_RSS_URL, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        if channel is None:
            return {}

        desc_map: dict[str, str] = {}
        count = 0
        for item in channel.findall("item"):
            def g(tag: str) -> str:
                e = item.find(tag)
                return e.text.strip() if e is not None and e.text else ""

            title = g("title")
            raw_desc = g("description")
            clean_desc = re.sub(
                r"\s+", " ",
                html_module.unescape(re.sub(r"<[^>]+>", "", raw_desc))
            ).strip()

            # タイトルと同内容の説明は除外（Google News と同じ問題を回避）
            if clean_desc and clean_desc[:20] not in title[:20]:
                key = _norm_title(title)
                if key:
                    desc_map[key] = clean_desc[:400]
                    count += 1

        elapsed = time.perf_counter() - t0
        print(f"  [Bing RSS] {elapsed*1000:.0f} ms  {count} 件（説明文あり）")
        return desc_map

    except Exception as e:
        print(f"  [Bing RSS] スキップ: {e}")
        return {}


def enrich_descriptions(articles: list[dict], desc_map: dict) -> list[dict]:
    """
    Bing の description map を使って Google News 記事の description を補完する。
    既に description がある記事はスキップ。
    """
    enriched = 0
    for a in articles:
        existing_desc = (a.get("description") or "").strip()
        title_text    = (a.get("title") or "")
        # 意味のある説明文がある場合はスキップ（タイトルの繰り返しは除外）
        if existing_desc and len(existing_desc) > len(title_text) + 15:
            continue
        key = _norm_title(a.get("title", ""))
        if key in desc_map:
            a["description"] = desc_map[key]
            enriched += 1
        else:
            # 部分一致も試みる（先頭20文字で比較）
            short_key = key[:20]
            for bkey, bdesc in desc_map.items():
                if short_key and short_key in bkey or bkey[:20] in key:
                    a["description"] = bdesc
                    enriched += 1
                    break
    if enriched:
        print(f"  [エンリッチ] {enriched} 件の記事に説明文を追加")
    return articles


# ── Bingスニペット取得（説明文なし記事の補完） ───────────────────────────────
def _fetch_one_snippet(args):
    """1件の記事タイトルに対してBing検索スニペットを取得する（並列実行用）"""
    session, title = args
    query = urllib.parse.quote(title[:50])
    url = f"https://www.bing.com/news/search?q={query}&setlang=ja-JP&cc=JP"
    try:
        r = session.get(url, timeout=10)
        snippets = re.findall(
            r'<div[^>]*class="[^"]*snippet[^"]*"[^>]*>([^<]{30,})</div>', r.text
        )
        if snippets:
            return snippets[0].strip()
    except Exception:
        pass
    return ""


def fetch_bing_snippets_for_empty(articles: list, session) -> list:
    """
    説明文がない記事に対してBing検索スニペットを並列取得して補完する。
    毎日のfetch_news.py実行時に新規記事の説明文を自動補完する。
    """
    targets = [
        (i, a) for i, a in enumerate(articles)
        if not (a.get("description") or "").strip()
        and (a.get("title") or "").strip()
    ]
    if not targets:
        return articles

    print(f"  [Bingスニペット] {len(targets)} 件の説明文を取得中...")
    t0 = time.perf_counter()
    filled = 0

    import threading
    lock = threading.Lock()

    def fetch_with_delay(args):
        idx, article = args
        result = _fetch_one_snippet((session, article.get("title", "")))
        time.sleep(BING_SNIPPET_DELAY)
        return idx, result

    with ThreadPoolExecutor(max_workers=BING_SNIPPET_WORKERS) as ex:
        futures = {ex.submit(fetch_with_delay, t): t for t in targets}
        for future in as_completed(futures):
            idx, snippet = future.result()
            if snippet:
                articles[idx]["description"] = snippet
                with lock:
                    filled += 1

    elapsed = time.perf_counter() - t0
    print(f"  [Bingスニペット] {elapsed:.1f}s  {filled}/{len(targets)} 件取得")
    return articles


# ── RSS 取得 ─────────────────────────────────────────────────────────────────
def fetch_rss_bytes(session: "requests.Session", url: str = None) -> bytes:
    if url is None:
        url = _gnews_url(GOOGLE_NEWS_QUERIES[0])
    t0 = time.perf_counter()
    resp = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
    resp.raise_for_status()
    content = resp.content
    print(f"  [RSS取得] {(time.perf_counter()-t0)*1000:.0f} ms  ({len(content)/1024:.1f} KB)")
    return content


# ── RSS パース ───────────────────────────────────────────────────────────────
def parse_rss(content: bytes) -> list[dict]:
    t0 = time.perf_counter()
    root = ET.fromstring(content)
    channel = root.find("channel")
    if channel is None:
        raise ValueError("RSS チャンネルが見つかりません")

    articles = []
    for item in channel.findall("item"):
        def g(tag):
            e = item.find(tag)
            return e.text.strip() if e is not None and e.text else ""

        raw_date = g("pubDate")
        iso_date = raw_date
        try:
            dt = datetime.datetime.strptime(raw_date, "%a, %d %b %Y %H:%M:%S %Z")
            iso_date = (dt + datetime.timedelta(hours=9)).strftime("%Y-%m-%dT%H:%M:%S+09:00")
        except ValueError:
            pass

        title_text   = g("title")
        raw_desc     = g("description")
        cleaned_desc = re.sub(r"\s+", " ",
                       html_module.unescape(re.sub(r"<[^>]+>", "", raw_desc))).strip()

        # Google News RSS の description はタイトルの繰り返しなので空に統一
        # 判定: タイトル先頭20文字で始まる、またはタイトルより実質短い
        title_prefix = re.sub(r"\s+", "", title_text[:20])
        desc_prefix  = re.sub(r"\s+", "", cleaned_desc[:20])
        if title_prefix and desc_prefix and (
            desc_prefix in title_prefix or title_prefix in desc_prefix
            or len(cleaned_desc) < len(title_text) + 10
        ):
            cleaned_desc = ""

        src_el = item.find("source")
        articles.append({
            "title":       title_text,
            "link":        g("link"),
            "pub_date":    iso_date,
            "source":      src_el.text.strip() if src_el is not None and src_el.text else "",
            "description": cleaned_desc,
        })

    print(f"  [RSS解析] {time.perf_counter()-t0:.3f} s  {len(articles)} 件")
    return articles


# ── 直接 RSS フィード取得（キーワードフィルタ付き） ──────────────────────────
def _is_relevant(title: str, desc: str) -> bool:
    """タイトルまたは説明文にキーワードが含まれるか確認"""
    text = (title + " " + desc).lower()
    return any(kw in text for kw in FILTER_KEYWORDS)


def fetch_direct_rss(session: "requests.Session") -> list:
    """NHK・Yahoo・厚労省・デジタル庁などの直接RSSを取得してキーワードフィルタする"""
    all_articles = []
    for feed_url, source_name in DIRECT_RSS_FEEDS:
        t0 = time.perf_counter()
        try:
            resp = session.get(feed_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            # RSS 2.0 / Atom / RSS 1.0(RDF) 全対応
            ns = {"atom": "http://www.w3.org/2005/Atom",
                  "rss1": "http://purl.org/rss/1.0/",
                  "dc":   "http://purl.org/dc/elements/1.1/"}
            items = (root.findall(".//item")
                     or root.findall(".//atom:entry", ns)
                     or root.findall(".//rss1:item", ns))

            count = matched = 0
            for item in items:
                count += 1
                def g(*tags):
                    for tag in tags:
                        try:
                            e = item.find(tag, ns)
                            if e is not None and e.text:
                                return e.text.strip()
                        except Exception:
                            pass
                    return ""

                title    = g("title", "atom:title", "rss1:title")
                raw_desc = (g("description", "rss1:description")
                            or g("atom:summary", "atom:content"))
                link     = g("link", "rss1:link")
                # Atom <link href="..."> 対応
                if not link:
                    for tag in ("atom:link", "link"):
                        link_el = item.find(tag, ns)
                        if link_el is not None:
                            link = link_el.get("href", "") or (link_el.text or "").strip()
                            if link:
                                break
                pub_date = (g("pubDate") or g("dc:date")
                            or g("atom:published", "atom:updated"))

                cleaned_desc = re.sub(r"\s+", " ",
                    html_module.unescape(re.sub(r"<[^>]+>", "", raw_desc))).strip()

                if not _is_relevant(title, cleaned_desc):
                    continue

                # 日付を JST ISO 形式に変換
                iso_date = pub_date
                for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
                            "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
                    try:
                        dt = datetime.datetime.strptime(pub_date, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                        dt_jst = dt.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
                        iso_date = dt_jst.strftime("%Y-%m-%dT%H:%M:%S+09:00")
                        break
                    except ValueError:
                        continue

                # タイトルと同内容の説明は空に
                title_prefix = re.sub(r"\s+", "", title[:20])
                desc_prefix  = re.sub(r"\s+", "", cleaned_desc[:20])
                if title_prefix and desc_prefix and (
                    desc_prefix in title_prefix or title_prefix in desc_prefix
                    or len(cleaned_desc) < len(title) + 10
                ):
                    cleaned_desc = ""

                all_articles.append({
                    "title":       title,
                    "link":        link,
                    "pub_date":    iso_date,
                    "source":      source_name,
                    "description": cleaned_desc,
                })
                matched += 1

            elapsed = (time.perf_counter() - t0) * 1000
            print(f"  [直接RSS] {source_name}: {elapsed:.0f}ms  {count}件中{matched}件マッチ")
        except Exception as e:
            print(f"  [直接RSS] {source_name}: スキップ ({e})")

    return all_articles


# ── 重複排除（URL・タイトルベース） ──────────────────────────────────────────
def dedup_articles(articles: list) -> list:
    """URLとタイトル正規化で重複を除去。より詳細な説明文がある方を優先する"""
    seen_urls   = {}  # url -> index
    seen_titles = {}  # norm_title -> index
    result = []

    for a in articles:
        url   = (a.get("_glink") or a.get("link") or "").split("?")[0]
        ntitle = _norm_title(a.get("title", ""))

        dup_idx = seen_urls.get(url) if url else None
        if dup_idx is None and ntitle:
            dup_idx = seen_titles.get(ntitle)

        if dup_idx is not None:
            # 説明文が長い方を採用
            existing = result[dup_idx]
            if len(a.get("description") or "") > len(existing.get("description") or ""):
                result[dup_idx] = a
        else:
            idx = len(result)
            result.append(a)
            if url:
                seen_urls[url] = idx
            if ntitle:
                seen_titles[ntitle] = idx

    return result


# ── 既存データ読込 ────────────────────────────────────────────────────────────
def load_existing(json_path: str) -> list[dict]:
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f).get("articles", [])
        except Exception:
            pass
    # 旧形式 .js からマイグレーション
    js_path = json_path.replace(".json", ".js")
    if os.path.exists(js_path):
        try:
            with open(js_path, "r", encoding="utf-8") as f:
                raw = f.read()
            m = re.search(r"const newsData\s*=\s*(\{.*\});", raw, re.DOTALL)
            if m:
                print("  [移行] 旧形式 news_data.js からデータを引き継ぎます")
                return json.loads(m.group(1)).get("articles", [])
        except Exception:
            pass
    return []


def _gkey(article: dict) -> str:
    """重複チェック用キー（_glink があればそれ、なければ link）"""
    return article.get("_glink") or article.get("link", "")


# ── 保存 ─────────────────────────────────────────────────────────────────────
def save_news(new_articles: list[dict], json_path: str,
              bing_desc_map: dict | None = None) -> None:
    t0 = time.perf_counter()
    existing = load_existing(json_path)

    # 既存記事の Google URL セットで重複排除
    existing_keys = {_gkey(a) for a in existing}
    fresh = [a for a in new_articles if _gkey(a) not in existing_keys]

    all_articles = fresh + existing

    # 説明文がない記事に Bing の説明文を補完（新規・既存どちらも対象）
    if bing_desc_map:
        all_articles = enrich_descriptions(all_articles, bing_desc_map)

    try:
        all_articles.sort(key=lambda a: a.get("pub_date", ""), reverse=True)
    except Exception:
        pass
    all_articles = all_articles[:MAX_ARTICLES]

    data = {"updated": _iso_jst(), "articles": all_articles}
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(json_path) / 1024
    print(f"  [保存]  {time.perf_counter()-t0:.3f} s  "
          f"新規 {len(fresh)} 件追加  合計 {len(all_articles)} 件  ({size_kb:.1f} KB)")


# ── 既存データの URL を一括解決 ───────────────────────────────────────────────
def resolve_existing(json_path: str, session: "requests.Session") -> None:
    """保存済み記事の Google News URL を実記事 URL に解決して上書き保存する。"""
    existing = load_existing(json_path)
    need = [a for a in existing if "news.google.com" in a.get("link", "")]
    if not need:
        print("  [既存URL解決] 解決が必要な URL はありません")
        return

    print(f"  [既存URL解決] {len(need)} 件の Google News URL を解決します...")
    existing = resolve_urls(existing, session, label="既存URL解決")

    data = {"updated": _iso_jst(), "articles": existing}
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(json_path) / 1024
    print(f"  [既存URL解決] 保存完了  ({size_kb:.1f} KB)")


# ── メイン ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="マイナ保険証ニュース取得")
    parser.add_argument("--force", "-f", action="store_true",
                        help="鮮度チェックをスキップして強制取得")
    parser.add_argument("--resolve-existing", "-r", action="store_true",
                        help="保存済み記事の Google News URL を一括解決する")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path  = os.path.join(script_dir, "news_data.json")

    print("=" * 52)
    print(f"  マイナ保険証ニュース取得  {_iso_jst()}")
    print("=" * 52)

    if not _USE_REQUESTS:
        print("  [警告] requests が未インストールです。URL解決をスキップします。")
        print("         pip install requests で導入してください。")

    # セッションを再利用（接続プール）
    session = requests.Session() if _USE_REQUESTS else None
    if session:
        session.headers.update({"User-Agent": UA})

    # 既存 URL を一括解決
    if args.resolve_existing and session:
        resolve_existing(json_path, session)
        if not args.force:
            print("=" * 52)
            return

    # 鮮度チェック
    if not args.force and is_fresh(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        age_min = int((_now_jst() - datetime.datetime.fromisoformat(
            data["updated"])).total_seconds() / 60)
        print(f"  [スキップ] 前回更新から {age_min} 分しか経過していません。")
        print(f"             強制取得するには --force を付けてください。")
        print(f"             記事数: {len(data.get('articles', []))} 件")
        print("=" * 52)
        return

    total_t0 = time.perf_counter()

    # Bing News RSS から説明文マップを取得
    bing_desc_map: dict = {}
    if session:
        bing_desc_map = fetch_bing_descriptions(session)

    all_articles = []

    # ── Google News RSS：複数クエリ ──────────────────────────────────────────
    for query in GOOGLE_NEWS_QUERIES:
        url = _gnews_url(query)
        try:
            content = fetch_rss_bytes(session, url) if session else _fetch_urllib()
            parsed  = parse_rss(content)
            # ソース名にクエリを補足
            for a in parsed:
                if not a.get("source"):
                    a["source"] = "Google News"
            all_articles.extend(parsed)
            print(f"    クエリ: 「{query}」 → {len(parsed)} 件")
        except Exception as e:
            print(f"  [エラー] Google News RSS 取得失敗 ({query}): {e}", file=sys.stderr)

    # ── 直接 RSS フィード（NHK・Yahoo・厚労省・デジタル庁） ──────────────────
    if session:
        direct = fetch_direct_rss(session)
        all_articles.extend(direct)
        print(f"  [直接RSS合計] {len(direct)} 件")

    if not all_articles:
        print("  [エラー] 記事を1件も取得できませんでした", file=sys.stderr)
        sys.exit(1)

    # ── 重複排除 ─────────────────────────────────────────────────────────────
    before = len(all_articles)
    all_articles = dedup_articles(all_articles)
    print(f"  [重複排除] {before} → {len(all_articles)} 件")

    # Bing の説明文で補完
    if bing_desc_map:
        all_articles = enrich_descriptions(all_articles, bing_desc_map)

    # URL 解決（Google News → 実記事 URL、Base64デコードのみ）
    if session:
        all_articles = resolve_urls(all_articles, session, label="新規URL解決")

    # 説明文なし記事にBingスニペットで補完
    if session:
        all_articles = fetch_bing_snippets_for_empty(all_articles, session)

    # 保存（Bing説明文マップも渡して既存記事を含めエンリッチ）
    save_news(all_articles, json_path, bing_desc_map=bing_desc_map)

    print(f"  [合計]  {time.perf_counter()-total_t0:.2f} s")
    print("=" * 52)


def _fetch_urllib() -> bytes:
    import urllib.request as ur
    req = ur.Request(RSS_URL, headers={"User-Agent": UA})
    with ur.urlopen(req, timeout=CONNECT_TIMEOUT + READ_TIMEOUT) as r:
        return r.read()


if __name__ == "__main__":
    main()
