"""
通知レポート生成スクリプト（Teams / Power Automate 連携用）

Teams の「Webhook 受信」トリガーは Accenture テナントの DLP ポリシー
（Secure Data Access – Productivity / TeamsWebhookRequestReceived）で
ブロックされているため、本スクリプトは Teams を直接叩かない。

代わりに、GitHub Pages 上に「通知レポート」を公開し、
Power Automate（Recurrence または RSS トリガー：いずれも標準コネクタ）が
それを読み取って Teams に投稿する“プル型”の構成をとる。

出力:
  - notify_report.json : 構造化サマリ（HTTP 読み取り用 / Adaptive Card 同梱）
  - notify_feed.xml    : RSS フィード（RSS トリガー用 / 毎朝＋急上昇を item 追加）

モード:
  --mode digest : 毎朝の日次ダイジェスト（既定）
  --mode alert  : 急上昇の即時アラート（spike があるときのみ item 追加）
"""

import os
import re
import json
import html
import datetime
import argparse
import xml.etree.ElementTree as ET

# ─── 設定 ────────────────────────────────────────────────────────────────────
JST        = datetime.timezone(datetime.timedelta(hours=9))
SITE_URL   = "https://myna-news-jp.github.io/"
NEWS_TOP_N = 6     # ダイジェストに載せるニュース件数
FEED_KEEP  = 30    # RSS フィードに保持する最大 item 数

# index.html / fetch_news.py と同じ保険証トピック判定キーワード
INSURANCE_KW = [
    "保険証", "資格確認書", "オンライン資格確認", "オン資", "被保険者証",
    "電子処方箋", "レセプト", "医療機関", "受診", "診療", "窓口負担",
    "健康保険", "医療DX", "マイナ保険証", "医療費", "薬剤情報",
]

ALERT_LABEL = {
    "green":  ("🟢", "平常範囲内"),
    "yellow": ("🟡", "要注意：一部キーワードが上昇中"),
    "red":    ("🔴", "アラート：複数キーワードが急上昇中"),
}


# ─── ユーティリティ ───────────────────────────────────────────────────────────
def _now_jst():
    return datetime.datetime.now(JST)

def _iso_jst(dt=None):
    dt = dt or _now_jst()
    return dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")

def _load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _classify_topic(a):
    if a.get("topic"):
        return a["topic"]
    text = (a.get("title") or "") + " " + (a.get("description") or "")
    return "insurance" if any(kw in text for kw in INSURANCE_KW) else "mynumber"

def _arrow(v):
    return "▲" if v > 0 else "▼" if v < 0 else "─"

def _esc(s):
    return html.escape(s or "", quote=True)


# ─── サマリ構築 ───────────────────────────────────────────────────────────────
def build_trends_summary(trends: dict) -> list[dict]:
    """検索量（current）降順で並べたトレンドサマリを返す"""
    kws = list((trends or {}).get("keywords", []))
    kws.sort(key=lambda k: k.get("current", 0), reverse=True)
    out = []
    for k in kws:
        out.append({
            "keyword":     k.get("keyword", ""),
            "current":     k.get("current", 0),
            "change_day":  k.get("change_day", 0),
            "change_week": k.get("change_week", 0),
            "alert":       bool(k.get("alert")),
            "analysis":    (k.get("analysis") or {}).get("text") if k.get("alert") else None,
        })
    return out

def build_news_summary(news: dict, top_n: int = NEWS_TOP_N) -> list[dict]:
    """マイナ保険証関連を優先して上位 top_n 件のニュースを返す"""
    arts = list((news or {}).get("articles", []))
    # pub_date 降順
    arts.sort(key=lambda a: a.get("pub_date", ""), reverse=True)
    insurance = [a for a in arts if _classify_topic(a) == "insurance"]
    others    = [a for a in arts if _classify_topic(a) != "insurance"]
    picked    = (insurance + others)[:top_n]
    out = []
    for a in picked:
        out.append({
            "title":    a.get("title", ""),
            "url":      a.get("link", ""),
            "source":   a.get("source", ""),
            "category": a.get("category", "一般"),
            "topic":    _classify_topic(a),
            "date":     (a.get("pub_date") or "")[:10],
        })
    return out


# ─── HTML 本文（RSS item / カード用） ────────────────────────────────────────
def render_html(trends_sum, news_sum, alert_level, mode) -> str:
    icon, msg = ALERT_LABEL.get(alert_level, ALERT_LABEL["green"])
    parts = []
    parts.append(f"<p><b>{icon} 総合ステータス：{_esc(msg)}</b></p>")

    parts.append("<p><b>📈 検索トレンド（検索量順）</b></p><ul>")
    for t in trends_sum:
        a = _arrow(t["change_week"])
        spike = " 🔴急上昇" if t["alert"] else ""
        parts.append(
            f"<li>{_esc(t['keyword'])}：{t['current']} "
            f"（週平均比 {a}{t['change_week']:+.0f}%）{spike}</li>"
        )
        if t.get("analysis"):
            parts.append(f"<li style='margin-left:1em'>💡 考察：{_esc(t['analysis'])}</li>")
    parts.append("</ul>")

    parts.append("<p><b>📰 注目ニュース（🩺保険証関連を優先）</b></p><ul>")
    for n in news_sum:
        badge = "🩺" if n["topic"] == "insurance" else "💳"
        cat = f"[{_esc(n['category'])}] " if n["category"] and n["category"] != "一般" else ""
        parts.append(
            f"<li>{badge} {cat}<a href=\"{_esc(n['url'])}\">{_esc(n['title'])}</a>"
            f"（{_esc(n['source'])} / {_esc(n['date'])}）</li>"
        )
    parts.append("</ul>")

    parts.append(f'<p>▶ <a href="{SITE_URL}">サイトで全件を見る</a></p>')
    return "\n".join(parts)


def render_text(trends_sum, news_sum, alert_level) -> str:
    """プレーンテキスト版（カードのフォールバック）"""
    icon, msg = ALERT_LABEL.get(alert_level, ALERT_LABEL["green"])
    lines = [f"{icon} 総合ステータス：{msg}", "", "📈 検索トレンド（検索量順）"]
    for t in trends_sum:
        a = _arrow(t["change_week"])
        spike = " 🔴急上昇" if t["alert"] else ""
        lines.append(f"・{t['keyword']}：{t['current']}（週平均比 {a}{t['change_week']:+.0f}%）{spike}")
        if t.get("analysis"):
            lines.append(f"　💡考察：{t['analysis']}")
    lines += ["", "📰 注目ニュース（🩺保険証関連を優先）"]
    for n in news_sum:
        badge = "🩺" if n["topic"] == "insurance" else "💳"
        lines.append(f"・{badge} {n['title']}（{n['source']}）")
    lines += ["", f"▶ サイトで全件: {SITE_URL}"]
    return "\n".join(lines)


def build_adaptive_card(title, html_body, text_body) -> dict:
    """Power Automate がそのまま Teams に投稿できる Adaptive Card"""
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": [
            {"type": "TextBlock", "size": "Large", "weight": "Bolder",
             "text": title, "wrap": True},
            {"type": "TextBlock", "text": text_body, "wrap": True},
        ],
        "actions": [
            {"type": "Action.OpenUrl", "title": "サイトを開く", "url": SITE_URL}
        ],
    }


# ─── RSS フィード ─────────────────────────────────────────────────────────────
def _rfc822(dt=None):
    dt = dt or _now_jst()
    # RSS pubDate は英語ロケール固定フォーマット
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    mons = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return (f"{days[dt.weekday()]}, {dt.day:02d} {mons[dt.month-1]} {dt.year} "
            f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d} +0900")


def update_rss_feed(feed_path, item_title, item_html, guid):
    """既存フィードを読み、新 item を先頭に追加して保存（最大 FEED_KEEP 件）"""
    items = []
    if os.path.exists(feed_path):
        try:
            tree = ET.parse(feed_path)
            ch = tree.getroot().find("channel")
            if ch is not None:
                for it in ch.findall("item"):
                    g = it.find("guid")
                    if g is not None and g.text == guid:
                        continue  # 同一 guid はスキップ（重複防止）
                    items.append(it)
        except Exception:
            items = []

    # 新 item を ElementTree で組み立て
    new = ET.Element("item")
    ET.SubElement(new, "title").text = item_title
    ET.SubElement(new, "link").text = SITE_URL
    ET.SubElement(new, "guid", {"isPermaLink": "false"}).text = guid
    ET.SubElement(new, "pubDate").text = _rfc822()
    desc = ET.SubElement(new, "description")
    desc.text = item_html  # CDATA は後段で文字列処理

    all_items = [new] + items
    all_items = all_items[:FEED_KEEP]

    rss = ET.Element("rss", {"version": "2.0"})
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = "マイナ保険証メディア 通知フィード"
    ET.SubElement(ch, "link").text = SITE_URL
    ET.SubElement(ch, "description").text = "ニュース・検索トレンドの日次サマリと急上昇アラート"
    ET.SubElement(ch, "language").text = "ja"
    ET.SubElement(ch, "lastBuildDate").text = _rfc822()
    for it in all_items:
        ch.append(it)

    xml_bytes = ET.tostring(rss, encoding="utf-8")
    # description を CDATA で包む（HTMLをそのまま渡すため）
    text = xml_bytes.decode("utf-8")
    text = re.sub(
        r"<description>(.*?)</description>",
        lambda m: "<description><![CDATA[" +
                  html.unescape(m.group(1)) + "]]></description>",
        text, flags=re.DOTALL,
    )
    text = '<?xml version="1.0" encoding="utf-8"?>\n' + text
    with open(feed_path, "w", encoding="utf-8") as f:
        f.write(text)


# ─── メイン ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Teams通知レポート生成")
    parser.add_argument("--mode", choices=["digest", "alert"], default="digest",
                        help="digest=日次サマリ / alert=急上昇のみ")
    args = parser.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    trends = _load_json(os.path.join(here, "trends_data.json")) or {}
    news   = (_load_json(os.path.join(here, "news_latest.json"))
              or _load_json(os.path.join(here, "news_data.json")) or {})

    alert_level = trends.get("alert_level", "green")
    trends_sum  = build_trends_summary(trends)
    news_sum    = build_news_summary(news)
    spikes      = [t for t in trends_sum if t["alert"]]

    print("=" * 52)
    print(f"  通知レポート生成  mode={args.mode}  {_iso_jst()}")
    print(f"  急上昇: {len(spikes)} 件 / アラートレベル: {alert_level}")
    print("=" * 52)

    # alert モードで spike が無ければ何もしない（フィードを汚さない）
    if args.mode == "alert" and not spikes:
        print("  [スキップ] 急上昇なし。alertフィードは更新しません。")
        return

    today = _now_jst().strftime("%-m/%-d" if os.name != "nt" else "%m/%d")
    if args.mode == "alert":
        title = f"🔴 急上昇アラート：{ '・'.join(t['keyword'] for t in spikes) }"
        guid  = "alert-" + _now_jst().strftime("%Y%m%d-%H")
    else:
        icon = ALERT_LABEL.get(alert_level, ALERT_LABEL['green'])[0]
        title = f"{icon} マイナ保険証メディア 日次サマリ {today}"
        guid  = "digest-" + _now_jst().strftime("%Y%m%d")

    html_body = render_html(trends_sum, news_sum, alert_level, args.mode)
    text_body = render_text(trends_sum, news_sum, alert_level)
    card      = build_adaptive_card(title, html_body, text_body)

    # 1) JSON レポート
    report = {
        "generated_at": _iso_jst(),
        "mode":         args.mode,
        "alert_level":  alert_level,
        "title":        title,
        "trends":       trends_sum,
        "news":         news_sum,
        "text":         text_body,
        "html":         html_body,
        "card":         card,
    }
    report_path = os.path.join(here, "notify_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  [保存] notify_report.json ({os.path.getsize(report_path)/1024:.1f} KB)")

    # 2) RSS フィード
    feed_path = os.path.join(here, "notify_feed.xml")
    update_rss_feed(feed_path, title, html_body, guid)
    print(f"  [保存] notify_feed.xml ({os.path.getsize(feed_path)/1024:.1f} KB)")
    print(f"  [タイトル] {title}")
    print("=" * 52)


if __name__ == "__main__":
    main()
