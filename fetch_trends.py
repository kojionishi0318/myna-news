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
import datetime
import argparse

try:
    import pandas as pd
    from pytrends.request import TrendReq
    _HAS_PYTRENDS = True
except ImportError:
    _HAS_PYTRENDS = False

# ─── 設定 ────────────────────────────────────────────────────────────────────
JST             = datetime.timezone(datetime.timedelta(hours=9))
FRESHNESS_SECS  = 3300   # 55分（1時間ごと実行で二重取得防止）
SPIKE_THRESHOLD = 1.5    # 週平均の1.5倍超でアラート
MIN_ALERT_VAL   = 5      # この値未満は件数が少なすぎてアラートしない
SLEEP_BETWEEN   = 6      # バッチ間の待機秒（レート制限対策）

KEYWORDS = [
    "マイナ保険証",        # ← アンカーキーワード（バッチ1・2 両方に含まれる）
    "マイナンバーカード",
    "健康保険証",
    "マイナ保険証 使えない",
    "健康保険証 廃止",
    "オンライン資格確認",  # ← バッチ2 でアンカーとペアで取得
]

ANCHOR_KW = KEYWORDS[0]   # "マイナ保険証"
BATCH1    = KEYWORDS[:5]   # 一括取得できる上限5件
BATCH2    = [ANCHOR_KW] + KEYWORDS[5:]   # アンカー + 残り


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

    pytrends = TrendReq(
        hl="ja-JP", tz=-540,
        timeout=(10, 30),
        retries=2,
        backoff_factor=0.5,
    )

    # ── バッチ1: 最初の5キーワードを一括取得 ──────────────────────────────────
    print(f"  [バッチ1] {BATCH1}")
    df1 = fetch_batch(pytrends, BATCH1)
    if df1 is None:
        print("  [エラー] バッチ1 の取得に失敗しました")
        sys.exit(1)

    time.sleep(SLEEP_BETWEEN)

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
