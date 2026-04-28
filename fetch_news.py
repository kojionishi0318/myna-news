"""
マイナ保険証 ニュース取得スクリプト
Google News RSS から最新ニュースを取得して news_data.json に保存します。
"""

import sys
import os
import re
import json
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
RSS_URL         = (
    "https://news.google.com/rss/search?q="
    + urllib.parse.quote(SEARCH_QUERY)
    + "&hl=ja&gl=JP&ceid=JP:ja"
)
CONNECT_TIMEOUT  = 5    # TCP接続タイムアウト（秒）
READ_TIMEOUT     = 20   # レスポンス受信タイムアウト（秒）
RESOLVE_TIMEOUT  = 6    # URL解決タイムアウト（秒/記事）
RESOLVE_WORKERS  = 20   # 並列ワーカー数
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


# ── URL解決（Google News リダイレクト → 実記事URL） ──────────────────────────
def _resolve_one(google_url: str, session: "requests.Session") -> str:
    """
    Google News のリダイレクトを追跡して実際の記事 URL を返す。
    失敗した場合は元の URL をそのまま返す。
    stream=True + resp.close() でレスポンスボディをダウンロードせずに済む。
    """
    try:
        resp = session.get(
            google_url,
            timeout=RESOLVE_TIMEOUT,
            allow_redirects=True,
            stream=True,
            headers={"User-Agent": UA},
        )
        final_url = resp.url
        resp.close()
        return final_url
    except Exception:
        return google_url


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
            articles[idx]["_glink"] = orig        # 元の Google URL を保持
            articles[idx]["link"]   = result      # 実記事 URL で上書き
            resolved += 1
            if "news.google.com" not in result:
                ok += 1

    elapsed = time.perf_counter() - t0
    tag = f"  [{label}]" if label else "  [URL解決]"
    print(f"{tag} {elapsed:.1f} s  {ok}/{resolved} 件解決")
    return articles


# ── RSS 取得 ─────────────────────────────────────────────────────────────────
def fetch_rss_bytes(session: "requests.Session") -> bytes:
    t0 = time.perf_counter()
    resp = session.get(RSS_URL, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
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

        src_el = item.find("source")
        articles.append({
            "title":       g("title"),
            "link":        g("link"),
            "pub_date":    iso_date,
            "source":      src_el.text.strip() if src_el is not None and src_el.text else "",
            "description": re.sub(r"\s+", " ",
                           html_module.unescape(re.sub(r"<[^>]+>", "", g("description")))).strip(),
        })

    print(f"  [RSS解析] {time.perf_counter()-t0:.3f} s  {len(articles)} 件")
    return articles


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
def save_news(new_articles: list[dict], json_path: str) -> None:
    t0 = time.perf_counter()
    existing = load_existing(json_path)

    # 既存記事の Google URL セットで重複排除
    existing_keys = {_gkey(a) for a in existing}
    fresh = [a for a in new_articles if _gkey(a) not in existing_keys]

    all_articles = fresh + existing
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

    # RSS 取得
    try:
        content = fetch_rss_bytes(session) if session else _fetch_urllib()
    except Exception as e:
        print(f"  [エラー] RSS 取得失敗: {e}", file=sys.stderr)
        sys.exit(1)

    # RSS 解析
    try:
        articles = parse_rss(content)
    except Exception as e:
        print(f"  [エラー] RSS 解析失敗: {e}", file=sys.stderr)
        sys.exit(1)

    # URL 解決（Google News → 実記事 URL）
    if session:
        articles = resolve_urls(articles, session, label="新規URL解決")

    # 保存
    save_news(articles, json_path)

    print(f"  [合計]  {time.perf_counter()-total_t0:.2f} s")
    print("=" * 52)


def _fetch_urllib() -> bytes:
    import urllib.request as ur
    req = ur.Request(RSS_URL, headers={"User-Agent": UA})
    with ur.urlopen(req, timeout=CONNECT_TIMEOUT + READ_TIMEOUT) as r:
        return r.read()


if __name__ == "__main__":
    main()
