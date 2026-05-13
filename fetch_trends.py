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
SLEEP_BETWEEN   = 4      # キーワード間の待機秒（レート制限対策）

KEYWORDS = [
    "マイナ保険証",
    "マイナンバーカード",
    "健康保険証",
    "マイナ保険証 使えない",
    "健康保険証 廃止",
    "オンライン資格確認",
]


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


# ─── 1キーワード取得 ──────────────────────────────────────────────────────────
def fetch_keyword(pytrends, keyword: str) -> dict | None:
    """
    キーワードのトレンドデータを取得して辞書を返す。
    失敗時は None を返す。
    """
    try:
        # 直近7日分の時間別データを取得
        pytrends.build_payload([keyword], timeframe="now 7-d", geo="JP")
        df = pytrends.interest_over_time()

        if df is None or df.empty or keyword not in df.columns:
            print(f"    [スキップ] データなし")
            return None

        # isPartial 列を除外して日次集計
        series = df[keyword]
        df_daily = series.resample("D").mean().round(1)
        df_daily = df_daily[df_daily.notna()]

        if len(df_daily) < 2:
            print(f"    [スキップ] データが2日未満")
            return None

        history = [
            {"date": str(d.date()), "value": round(float(v), 1)}
            for d, v in df_daily.items()
        ]

        today_val      = history[-1]["value"]
        yesterday_val  = history[-2]["value"] if len(history) >= 2 else today_val
        avg_7day       = round(sum(h["value"] for h in history) / len(history), 1)

        change_day  = round((today_val - yesterday_val) / yesterday_val * 100, 1) \
                      if yesterday_val > 0 else 0.0
        change_week = round((today_val - avg_7day) / avg_7day * 100, 1) \
                      if avg_7day > 0 else 0.0

        alert = (today_val >= avg_7day * SPIKE_THRESHOLD) and (today_val >= MIN_ALERT_VAL)

        return {
            "keyword":      keyword,
            "current":      today_val,
            "yesterday":    yesterday_val,
            "change_day":   change_day,
            "avg_7day":     avg_7day,
            "change_week":  change_week,
            "alert":        alert,
            "history":      history,
        }

    except Exception as e:
        print(f"    [エラー] {e}")
        return None


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

    results = []
    for kw in KEYWORDS:
        print(f"  [{kw}] 取得中...")
        result = fetch_keyword(pytrends, kw)
        if result:
            results.append(result)
            mark = "🔴" if result["alert"] else "🟢"
            print(f"    {mark} 現在値: {result['current']} / "
                  f"前日比: {result['change_day']:+.1f}% / "
                  f"週平均: {result['avg_7day']}")
        time.sleep(SLEEP_BETWEEN)

    # アラートレベル集計
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
