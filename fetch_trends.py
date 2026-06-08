"""
Google トレンドデータ取得スクリプト
pytrends（非公式 Google Trends API）を使用して
指定キーワードの検索数推移を取得し trends_data.json に保存します。

対象キーワード:
  - マイナ保険証
  - マイナンバーカード
  - 健康保険証
  - マイナ保険証 使えない
  - 健康保険証 廃止
  - オンライン資格確認

【データ正規化について】
Google Trends は一度のリクエストに含まれるキーワードを
相対比較して 0-100 でスコアリングします。
キーワードを個別に取得すると各自が独立した 0-100 スケールになるため
キーワード間の比較が不正確になります。

対応策:
  - バッチ1: KEYWORDS[0:5] を一括取得（5件が API 上限）
  - バッチ2: KEYWORDS[5:] + アンカーキーワード (KEYWORDS[0]) を一括取得
  - バッチ2 のアンカー値をバッチ1 のアンカー値と比較してスケール係数を算出し
    バッチ2 の各キーワードをバッチ1 のスケールに揃える
"""

import sys
import os
import json
import time
import random
import datetime
import argparse

try:
    import pandas as pd
    from pytrends.request import TrendReq
    _HAS_PYTRENDS = True
except ImportError:
    _HAS_PYTRENDS = False

try:
    import anthropic as _anthropic_module
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False

# ─── 設定 ────────────────────────────────────────────────────────────────────
JST             = datetime.timezone(datetime.timedelta(hours=9))
FRESHNESS_SECS  = 3300   # 55分（1時間ごと実行で二重取得防止）
SPIKE_THRESHOLD = 1.5    # 週平均の1.5倍超でアラート
MIN_ALERT_VAL   = 5      # この値未満は件数が少なすぎてアラートしない
SLEEP_BETWEEN   = 10     # バッチ間の待機秒（レート制限対策）
SLEEP_INIT_MIN  = 5      # 初回リクエスト前の最小待機秒
SLEEP_INIT_MAX  = 15     # 初回リクエスト前の最大待機秒

KEYWORDS = [
    "マイナ保険証",        # ← アンカーキーワード（バッチ1・2 両方に含まれる）
    "マイナンバーカード",
    "健康保険証",
    "マイナ保険証 使えない",
    "健康保険証 廃止",
    "オンライン資格確認",  # ← バッチ2 でアンカーとペアで取得
    "資格確認書",          # ← バッチ2 でアンカーとペアで取得
]

ANCHOR_KW = KEYWORDS[0]   # "マイナ保険証"
BATCH1    = KEYWORDS[:5]   # 一括取得できる上限5件
BATCH2    = [ANCHOR_KW] + KEYWORDS[5:]   # アンカー + 残り

# ─── 急上昇考察（AI）設定 ─────────────────────────────────────────────────────
ANALYSIS_MODEL       = "claude-haiku-4-5"  # 考察生成モデル
ANALYSIS_NEWS_WINDOW = 3    # 急上昇日の何日前までのニュースを参照するか
ANALYSIS_NEWS_LIMIT  = 8    # 考察に渡す関連ニュースの最大件数
ANALYSIS_WEB_SEARCH  = 3    # AIのWeb検索の最大回数（0で無効）


# ─── ユーティリティ ───────────────────────────────────────────────────────────
def _now_jst():
    return datetime.datetime.now(JST)

def _iso_jst(dt=None):
    dt = dt or _now_jst()
    return dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")

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


# ─── バッチ取得 ───────────────────────────────────────────────────────────────
def fetch_batch(pytrends, keywords: list[str]) -> pd.DataFrame | None:
    """
    複数キーワードを一括取得して日次 DataFrame を返す。
    失敗時は None を返す。
    """
    try:
        pytrends.build_payload(keywords, timeframe="today 3-m", geo="JP")
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            print(f"    [スキップ] データなし: {keywords}")
            return None
        # isPartial 列を除外
        cols = [c for c in keywords if c in df.columns]
        if not cols:
            return None
        df_daily = df[cols].resample("D").mean().round(2)
        df_daily = df_daily.dropna(how="all")
        return df_daily
    except Exception as e:
        print(f"    [エラー] バッチ取得失敗 {keywords}: {e}")
        return None


# ─── スケール係数算出 ─────────────────────────────────────────────────────────
def calc_scale_factor(df1: pd.DataFrame, df2: pd.DataFrame, anchor: str) -> float:
    """
    バッチ1 と バッチ2 の共通期間におけるアンカーキーワードの平均値比を返す。
    バッチ2 の値にこのスケール係数を掛けることでスケールを揃える。
    """
    common_idx = df1.index.intersection(df2.index)
    if len(common_idx) == 0:
        return 1.0
    mean1 = df1.loc[common_idx, anchor].mean()
    mean2 = df2.loc[common_idx, anchor].mean()
    if mean2 == 0:
        return 1.0
    factor = mean1 / mean2
    return round(float(factor), 6)


# ─── Series → KPI 辞書 ───────────────────────────────────────────────────────
def series_to_kpi(keyword: str, series: pd.Series) -> dict | None:
    df_daily = series.resample("D").mean().round(1)
    df_daily = df_daily.dropna()

    if len(df_daily) < 2:
        print(f"    [スキップ] データが2日未満: {keyword}")
        return None

    history = [
        {"date": str(d.date()), "value": round(float(v), 1)}
        for d, v in df_daily.items()
    ]

    # KPIは直近7日分で算出
    recent       = history[-7:] if len(history) >= 7 else history
    today_val    = recent[-1]["value"]
    yesterday_val = recent[-2]["value"] if len(recent) >= 2 else today_val
    avg_7day     = round(sum(h["value"] for h in recent) / len(recent), 1)

    change_day  = round((today_val - yesterday_val) / yesterday_val * 100, 1) \
                  if yesterday_val > 0 else 0.0
    change_week = round((today_val - avg_7day) / avg_7day * 100, 1) \
                  if avg_7day > 0 else 0.0

    alert = (today_val >= avg_7day * SPIKE_THRESHOLD) and (today_val >= MIN_ALERT_VAL)

    return {
        "keyword":     keyword,
        "current":     today_val,
        "yesterday":   yesterday_val,
        "change_day":  change_day,
        "avg_7day":    avg_7day,
        "change_week": change_week,
        "alert":       alert,
        "history":     history,
    }


# ─── 急上昇考察（AI）─────────────────────────────────────────────────────────
def _load_prev_analysis(json_path: str) -> dict:
    """前回の trends_data.json から keyword -> analysis を読む（キャッシュ用）"""
    if not os.path.exists(json_path):
        return {}
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k["keyword"]: k["analysis"]
                for k in data.get("keywords", []) if k.get("analysis")}
    except Exception:
        return {}


def _related_news(keyword: str, spike_date: str, news_path: str) -> list[dict]:
    """急上昇日周辺（±数日）の関連ニュースを news_data.json から収集する"""
    if not os.path.exists(news_path):
        return []
    try:
        with open(news_path, "r", encoding="utf-8") as f:
            arts = json.load(f).get("articles", [])
    except Exception:
        return []

    try:
        sd = datetime.date.fromisoformat(spike_date)
    except Exception:
        return []
    lo = sd - datetime.timedelta(days=ANALYSIS_NEWS_WINDOW)
    hi = sd + datetime.timedelta(days=1)

    out = []
    for a in arts:
        pub = (a.get("pub_date") or "")[:10]
        try:
            pd_ = datetime.date.fromisoformat(pub)
        except Exception:
            continue
        if lo <= pd_ <= hi:
            out.append(a)
    out.sort(key=lambda a: a.get("pub_date", ""), reverse=True)
    return out[:ANALYSIS_NEWS_LIMIT]


def _extract_text_and_citations(resp) -> tuple[str, list[dict]]:
    """Claudeレスポンスから本文テキストとWeb検索引用を抽出する"""
    text_parts, citations = [], []
    for block in getattr(resp, "content", []) or []:
        if getattr(block, "type", "") == "text":
            text_parts.append(getattr(block, "text", "") or "")
            for c in (getattr(block, "citations", None) or []):
                url = getattr(c, "url", None)
                if url:
                    citations.append({"title": getattr(c, "title", None) or url, "url": url})
    return "".join(text_parts).strip(), citations


def generate_analysis(keyword: str, kpi: dict, news_path: str) -> dict | None:
    """
    急上昇キーワードについて、関連ニュース＋AIのWeb検索をもとに
    『なぜ急上昇したか』の考察を生成する。失敗時は None。
    """
    if not _HAS_ANTHROPIC:
        print("    [考察] anthropic 未インストール → スキップ")
        return None
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("    [考察] ANTHROPIC_API_KEY 未設定 → スキップ")
        return None

    history   = kpi.get("history", [])
    spike_date = history[-1]["date"] if history else str(_now_jst().date())
    news = _related_news(keyword, spike_date, news_path)

    news_block = "\n".join(
        f"- [{(a.get('pub_date') or '')[:10]}] {a.get('title','')}"
        f"（{a.get('source','')}） {a.get('link','')}"
        for a in news
    ) or "（サイト内に該当期間の関連ニュースなし）"

    prompt = (
        f"あなたは政策モニタリングのアナリストです。\n"
        f"Google検索トレンドで「{keyword}」の検索インタレストが急上昇しました。\n\n"
        f"【数値】急上昇日: {spike_date} / 当日値: {kpi.get('current')} "
        f"/ 前日比: {kpi.get('change_day')}% / 週平均: {kpi.get('avg_7day')} "
        f"（週平均比 {kpi.get('change_week')}%）\n\n"
        f"【同時期のサイト内ニュース】\n{news_block}\n\n"
        f"この急上昇が「なぜ起きたか」を推察してください。必要に応じてWeb検索で"
        f"直近の出来事を確認して構いません。出力要件:\n"
        f"1. 最も可能性が高い要因を2〜3個、それぞれ1〜2文で簡潔に。\n"
        f"2. 各要因には根拠（上記ニュースまたはWeb検索結果）を示す。\n"
        f"3. 確証がない場合は必ず『推測』と明記する。\n"
        f"4. 全体で300文字程度。前置き不要、要因の箇条書きから始める。\n"
    )

    tools = []
    if ANALYSIS_WEB_SEARCH > 0:
        tools = [{"type": "web_search_20250305", "name": "web_search",
                  "max_uses": ANALYSIS_WEB_SEARCH}]

    try:
        client = _anthropic_module.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=ANALYSIS_MODEL,
            max_tokens=1024,
            tools=tools,
            messages=[{"role": "user", "content": prompt}],
        )
        text, web_citations = _extract_text_and_citations(resp)
        if not text:
            return None

        # 根拠ソース: サイト内ニュース + Web検索引用（URL重複排除）
        sources, seen = [], set()
        for a in news:
            u = a.get("link") or ""
            if u and u not in seen:
                seen.add(u)
                sources.append({"title": a.get("title", ""), "url": u,
                                "source": a.get("source", ""), "type": "news"})
        for c in web_citations:
            if c["url"] not in seen:
                seen.add(c["url"])
                sources.append({"title": c["title"], "url": c["url"], "type": "web"})

        return {
            "generated_at": _iso_jst(),
            "spike_date":   spike_date,
            "text":         text,
            "sources":      sources[:10],
            "web_used":     bool(web_citations),
        }
    except Exception as e:
        print(f"    [考察] 生成失敗: {e}")
        return None


def attach_analyses(results: list[dict], json_path: str, news_path: str) -> int:
    """
    急上昇（alert=True）キーワードに考察を付与する。
    同じ急上昇日の考察が前回分にあれば再利用（API節約）。
    """
    prev = _load_prev_analysis(json_path)
    generated = 0
    for kpi in results:
        if not kpi.get("alert"):
            continue
        kw = kpi["keyword"]
        history = kpi.get("history", [])
        spike_date = history[-1]["date"] if history else None

        cached = prev.get(kw)
        if cached and cached.get("spike_date") == spike_date:
            kpi["analysis"] = cached          # 同日スパイク → キャッシュ再利用
            print(f"    [考察] {kw}: キャッシュ再利用（{spike_date}）")
            continue

        print(f"    [考察] {kw}: 生成中...（急上昇日 {spike_date}）")
        analysis = generate_analysis(kw, kpi, news_path)
        if analysis:
            kpi["analysis"] = analysis
            generated += 1
            print(f"    [考察] {kw}: 生成完了（Web検索={'有' if analysis['web_used'] else '無'}）")
    return generated


# ─── メイン ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Googleトレンドデータ取得")
    parser.add_argument("--force", "-f", action="store_true",
                        help="鮮度チェックをスキップして強制取得")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path  = os.path.join(script_dir, "trends_data.json")

    print("=" * 52)
    print(f"  Googleトレンド取得  {_iso_jst()}")
    print("=" * 52)

    if not _HAS_PYTRENDS:
        print("  [エラー] pytrends がインストールされていません")
        print("           pip install pytrends pandas")
        sys.exit(1)

    if not args.force and is_fresh(json_path):
        print("  [スキップ] 前回更新から55分以内です")
        print("             強制取得するには --force を付けてください。")
        print("=" * 52)
        return

    # GitHub Actions の IP はレート制限を受けやすいため
    # ブラウザ風 User-Agent を設定し、リトライも強化
    pytrends = TrendReq(
        hl="ja-JP", tz=-540,
        timeout=(15, 60),
        retries=3,
        backoff_factor=2.0,
        requests_args={
            "headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        },
    )

    # 初回リクエスト前にランダム待機（レート制限回避）
    wait_sec = random.uniform(SLEEP_INIT_MIN, SLEEP_INIT_MAX)
    print(f"  [待機] {wait_sec:.1f}秒待機中...")
    time.sleep(wait_sec)

    # ── バッチ1: 最初の5キーワードを一括取得 ──────────────────────────────────
    print(f"  [バッチ1] {BATCH1}")
    df1 = fetch_batch(pytrends, BATCH1)
    if df1 is None:
        print("  [警告] バッチ1 の取得に失敗しました（レート制限の可能性）")
        print("  [スキップ] 前回のデータを保持します。次回の自動更新をお待ちください。")
        print("=" * 52)
        sys.exit(0)  # CIを壊さないよう exit 0 で終了

    wait2 = random.uniform(SLEEP_BETWEEN, SLEEP_BETWEEN + 5)
    print(f"  [待機] バッチ間 {wait2:.1f}秒待機中...")
    time.sleep(wait2)

    # ── バッチ2: アンカー + 残りキーワードを取得（スケール正規化用）────────────
    scale_factor = 1.0
    df2_scaled   = {}   # keyword -> pd.Series (スケール調整済み)

    extra_kws = KEYWORDS[5:]   # バッチ2 にしか含まれないキーワード
    if extra_kws:
        print(f"  [バッチ2] {BATCH2}  （アンカー: {ANCHOR_KW}）")
        df2 = fetch_batch(pytrends, BATCH2)
        if df2 is not None and ANCHOR_KW in df2.columns:
            scale_factor = calc_scale_factor(df1, df2, ANCHOR_KW)
            print(f"    スケール係数: {scale_factor:.4f}  "
                  f"（1.0 に近いほどバッチ間のスケール差が小さい）")
            for kw in extra_kws:
                if kw in df2.columns:
                    df2_scaled[kw] = (df2[kw] * scale_factor).round(1)
        else:
            print("  [警告] バッチ2 取得失敗。追加キーワードはスキップします。")

    # ── KPI 算出 ───────────────────────────────────────────────────────────────
    results = []

    for kw in BATCH1:
        if kw not in df1.columns:
            continue
        print(f"  [{kw}] KPI算出...")
        kpi = series_to_kpi(kw, df1[kw])
        if kpi:
            results.append(kpi)
            mark = "🔴" if kpi["alert"] else "🟢"
            print(f"    {mark} 現在値: {kpi['current']} / "
                  f"前日比: {kpi['change_day']:+.1f}% / "
                  f"週平均: {kpi['avg_7day']}")

    for kw, series in df2_scaled.items():
        print(f"  [{kw}] KPI算出（スケール調整済み）...")
        kpi = series_to_kpi(kw, series)
        if kpi:
            results.append(kpi)
            mark = "🔴" if kpi["alert"] else "🟢"
            print(f"    {mark} 現在値: {kpi['current']} / "
                  f"前日比: {kpi['change_day']:+.1f}% / "
                  f"週平均: {kpi['avg_7day']}")

    # ── KEYWORDS 順に並べ直す ─────────────────────────────────────────────────
    order = {kw: i for i, kw in enumerate(KEYWORDS)}
    results.sort(key=lambda r: order.get(r["keyword"], 99))

    # ── アラートレベル集計 ─────────────────────────────────────────────────────
    alert_count = sum(1 for r in results if r["alert"])
    if alert_count == 0:
        alert_level = "green"
    elif alert_count == 1:
        alert_level = "yellow"
    else:
        alert_level = "red"

    # ── 急上昇キーワードに「考察」を付与（ニュース＋Web検索） ──────────────────
    news_path = os.path.join(script_dir, "news_data.json")
    if alert_count > 0:
        print(f"  [考察] 急上昇 {alert_count} 件の考察を準備します")
        try:
            gen = attach_analyses(results, json_path, news_path)
            print(f"  [考察] 新規生成 {gen} 件")
        except Exception as e:
            print(f"  [考察] スキップ（{e}）")

    output = {
        "updated":     _iso_jst(),
        "alert_level": alert_level,
        "keywords":    results,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(json_path) / 1024
    print(f"  [アラートレベル] {alert_level}  （急上昇: {alert_count}件）")
    print(f"  [保存] {json_path}  ({size_kb:.1f} KB)")
    print("=" * 52)


if __name__ == "__main__":
    main()
