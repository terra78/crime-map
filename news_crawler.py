"""
ニュース自動収集クローラー

RSSフィードから事件・犯罪関連ニュースを収集し、
Claude Haiku APIで構造化情報を抽出してbotとして自動投稿する。

実行: APScheduler (6時間おき) または手動
  python news_crawler.py --run-once
"""

import os
import asyncio
import json
import re
import argparse
from datetime import date, datetime
from typing import Optional

import httpx
import feedparser
from bs4 import BeautifulSoup
import anthropic
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from database import SessionLocal
from models import Report, SiteType
from crime_types import (
    INCIDENT_TYPES_FOR_PROMPT,
    get_crime_category,
    get_crime_law,
)

load_dotenv()

# ── 設定 ──────────────────────────────────────────────────────────────────────

# RSSフィード一覧
RSS_FEEDS = [
    # ── NHK ──
    {
        "name": "NHK 社会・事件",
        "url": "https://www.nhk.or.jp/rss/news/cat3.xml",
    },
    # ── Yahoo! Japan ──
    {
        "name": "Yahoo!ニュース 国内",
        "url": "https://news.yahoo.co.jp/rss/categories/domestic.xml",
    },
    {
        "name": "Yahoo!ニュース 事件・事故",
        "url": "https://news.yahoo.co.jp/rss/categories/incident.xml",
    },
    # ── livedoor ──
    {
        "name": "livedoor NEWS 国内",
        "url": "https://news.livedoor.com/topics/rss/dom.xml",
    },
    # ── Google News（地方紙含む全国記事を横断収集）──
    # Google News RSS は国内の地方紙・ブロック紙も含む数百媒体をカバーする
    {
        "name": "Google News: 逮捕・摘発",
        "url": "https://news.google.com/rss/search?q=%E9%80%AE%E6%8D%95+%E6%91%98%E7%99%BA&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 殺人・強盗・放火",
        "url": "https://news.google.com/rss/search?q=%E6%AE%BA%E4%BA%BA+%E5%BC%B7%E7%9B%97+%E6%94%BE%E7%81%AB&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 窃盗・空き巣",
        "url": "https://news.google.com/rss/search?q=%E7%AA%83%E7%9B%97+%E7%A9%BA%E3%81%8D%E5%B7%A3&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 詐欺・特殊詐欺",
        "url": "https://news.google.com/rss/search?q=%E8%A9%90%E6%AC%BA+%E7%89%B9%E6%AE%8A%E8%A9%90%E6%AC%BA&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 覚醒剤・薬物",
        "url": "https://news.google.com/rss/search?q=%E8%A6%9A%E9%86%92%E5%89%A4+%E8%96%AC%E7%89%A9&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 性犯罪・わいせつ",
        "url": "https://news.google.com/rss/search?q=%E6%80%A7%E7%8A%AF%E7%BD%AA+%E3%82%8F%E3%81%84%E3%81%9B%E3%81%A4&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: DV・ストーカー・虐待",
        "url": "https://news.google.com/rss/search?q=DV+%E3%82%B9%E3%83%88%E3%83%BC%E3%82%AB%E3%83%BC+%E8%99%90%E5%BE%85&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 不法滞在・入管",
        "url": "https://news.google.com/rss/search?q=%E4%B8%8D%E6%B3%95%E6%BB%9E%E5%9C%A8+%E5%85%A5%E7%AE%A1&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: サイバー犯罪",
        "url": "https://news.google.com/rss/search?q=%E3%82%B5%E3%82%A4%E3%83%90%E3%83%BC%E7%8A%AF%E7%BD%AA+%E4%B8%8D%E6%AD%A3%E3%82%A2%E3%82%AF%E3%82%BB%E3%82%B9&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 組織犯罪・暴力団",
        "url": "https://news.google.com/rss/search?q=%E7%B5%84%E7%B9%94%E7%8A%AF%E7%BD%AA+%E6%9A%B4%E5%8A%9B%E5%9B%A3&hl=ja&gl=JP&ceid=JP:ja",
    },
    {
        "name": "Google News: 送検・起訴",
        "url": "https://news.google.com/rss/search?q=%E9%80%81%E6%A4%9C+%E8%B5%B7%E8%A8%B4&hl=ja&gl=JP&ceid=JP:ja",
    },
]

# 事件関連キーワード（このうちどれかがタイトル/概要に含まれれば処理対象）
CRIME_KEYWORDS = [
    "逮捕", "送検", "起訴", "書類送検", "容疑者", "被告",
    "摘発", "事件", "犯罪", "詐欺", "窃盗", "暴行", "傷害",
    "殺人", "強盗", "薬物", "覚醒剤", "不法滞在", "入管",
    "性犯罪", "強制性交", "わいせつ", "交通死亡事故", "ひき逃げ",
    "器物損壊", "住居侵入", "放火", "恐喝", "脅迫", "横領",
    "偽造", "サイバー", "DV", "ストーカー", "児童虐待", "特殊詐欺",
]

# Claude Haiku クライアント（コスト効率重視）
_client: Optional[anthropic.Anthropic] = None


def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


# ── RSS取得 ───────────────────────────────────────────────────────────────────

async def fetch_rss(url: str) -> list[dict]:
    """RSSフィードを取得してエントリーリストを返す"""
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.get(
                url,
                headers={"User-Agent": "CrimeMapBot/1.0 (RSS reader)"},
                follow_redirects=True,
            )
            r.raise_for_status()
        feed = feedparser.parse(r.text)
        return [
            {
                "title":     e.get("title", ""),
                "url":       e.get("link", ""),
                "published": e.get("published", ""),
                "summary":   e.get("summary", ""),
            }
            for e in feed.entries
        ]
    except Exception as e:
        print(f"[Crawler] RSS取得失敗 {url}: {e}")
        return []


# ── 記事本文スクレイピング ─────────────────────────────────────────────────────

async def fetch_article(url: str) -> tuple[str, str]:
    """
    記事本文をスクレイピングして返す（最大3000文字）。
    Google News などのリダイレクト URL は最終 URL を返す。
    戻り値: (本文テキスト, 最終URL)
    """
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as http:
            r = await http.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CrimeMapBot/1.0)"},
            )
            r.raise_for_status()
            final_url = str(r.url)  # リダイレクト後の実際の URL
        soup = BeautifulSoup(r.text, "lxml")
        # ナビ・広告・スクリプト等を除去
        for tag in soup(["nav", "header", "footer", "script", "style", "aside",
                          "noscript", "iframe", "form"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return text[:3000], final_url
    except Exception as e:
        return f"取得失敗: {e}", url


# ── フィルタリング ────────────────────────────────────────────────────────────

def is_crime_related(title: str, summary: str) -> bool:
    """犯罪・事件関連のニュースかどうかを簡易キーワードチェック"""
    text = title + " " + summary
    return any(kw in text for kw in CRIME_KEYWORDS)


# ── AI情報抽出 ────────────────────────────────────────────────────────────────

NATIONALITY_TYPES = (
    "日本|中国|韓国|朝鮮|ベトナム|フィリピン|タイ|インドネシア|ミャンマー|"
    "カンボジア|ネパール|インド|パキスタン|バングラデシュ|スリランカ|"
    "イラン|イラク|トルコ|シリア|クルド（民族）|ナイジェリア|ガーナ|エチオピア|その他アフリカ|"
    "アメリカ|ブラジル|メキシコ|ペルー|ロシア|ウクライナ|その他ヨーロッパ|その他|不明"
)


async def extract_info(article_text: str, title: str) -> Optional[dict]:
    """
    Claude Haikuで記事から構造化情報を抽出する。
    戻り値: dict（skip=True の場合はスキップ）または None（API失敗）
    """
    prompt = f"""以下のニュース記事から事件情報を抽出してJSON形式で返してください。

抽出できない場合はnullにしてください。
事件・犯罪に無関係な記事（政治・経済・スポーツ等）は {{"skip": true}} を返してください。

incident_typeには以下のいずれかを選んでください（日本の警察庁分類に準拠）:
{INCIDENT_TYPES_FOR_PROMPT}

{{
  "skip": false,
  "title": "30文字以内の簡潔なタイトル",
  "address": "都道府県から始まる住所（例: 東京都新宿区西新宿）、不明ならnull",
  "incident_type": "上記リストのいずれか",
  "occurred_at": "YYYY-MM-DD形式（不明ならnull）",
  "nationality_type": "{NATIONALITY_TYPES}",
  "description": "100文字以内の事件概要"
}}

記事タイトル: {title}

記事本文:
{article_text}

JSONのみを返してください（説明文・コードブロック不要）。"""

    try:
        message = get_client().messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        print(f"[Crawler] AI抽出失敗: {e}")
    return None


# ── ジオコーディング ──────────────────────────────────────────────────────────

async def geocode(address: str) -> Optional[tuple[float, float]]:
    """
    住所から緯度経度を Nominatim で取得する。
    Nominatim 利用規約: 1リクエスト/秒以下、User-Agent必須。
    """
    if not address:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            r = await http.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "format":          "json",
                    "q":               address,
                    "accept-language": "ja",
                    "limit":           1,
                    "countrycodes":    "jp",
                },
                headers={"User-Agent": "CrimeMapBot/1.0 (contact: admin@example.com)"},
            )
            data = r.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"[Crawler] ジオコード失敗 '{address}': {e}")
    return None


# ── 重複チェック ──────────────────────────────────────────────────────────────

def source_url_exists(db: Session, url: str) -> bool:
    """同じ source_url の投稿が既に存在するか確認"""
    return db.query(Report).filter(Report.source_url == url).first() is not None


# ── 記事1件の処理 ─────────────────────────────────────────────────────────────

async def process_entry(entry: dict, site_type_id: int, db: Session) -> bool:
    """
    1記事を処理してDBに投稿する。
    戻り値: True=投稿成功 / False=スキップ
    """
    url = entry.get("url", "")
    if not url:
        return False

    # キーワードフィルタ（高速スクリーニング ─ 本文取得前に弾く）
    if not is_crime_related(entry["title"], entry.get("summary", "")):
        return False

    # 記事本文取得（Google News リダイレクトも解決して final_url を取得）
    article_text, final_url = await fetch_article(url)

    # 重複チェック（リダイレクト解決後の URL で判定）
    if source_url_exists(db, final_url):
        return False

    # AI構造化情報抽出
    info = await extract_info(article_text, entry["title"])
    if not info or info.get("skip"):
        return False

    # 住所がないとマップに表示できないためスキップ
    if not info.get("address"):
        print(f"[Crawler] 住所なしスキップ: {entry['title'][:50]}")
        return False

    # ジオコーディング（Nominatim: 1秒インターバルはcaller側で保証）
    coords = await geocode(info["address"])
    if not coords:
        print(f"[Crawler] ジオコード失敗スキップ: {info['address']}")
        return False

    lat, lng = coords

    # 発生日パース
    occurred_at: Optional[date] = None
    if info.get("occurred_at"):
        try:
            occurred_at = date.fromisoformat(info["occurred_at"])
        except ValueError:
            pass

    # crime_types.py から第2階層・第1階層を自動付与
    incident_type = info.get("incident_type") or "その他刑法犯"
    crime_category = get_crime_category(incident_type)
    crime_law      = get_crime_law(incident_type)

    # DB登録
    report = Report(
        site_type_id = site_type_id,
        title        = (info.get("title") or entry["title"])[:256],
        description  = info.get("description") or "",
        location     = f"SRID=4326;POINT({lng} {lat})",
        address      = info["address"],
        occurred_at  = occurred_at,
        data         = {
            "incident_type":    incident_type,
            "crime_category":   crime_category,
            "crime_law":        crime_law,
            "nationality_type": info.get("nationality_type", "不明"),
        },
        source_url   = final_url,
        status       = "pending",
        submitted_by = "bot",
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    print(f"[Crawler] 投稿: [{report.id}] {report.title}")
    return True


# ── メインジョブ ──────────────────────────────────────────────────────────────

async def run_news_crawler():
    """
    ニュースクローラーのメインジョブ。
    APScheduler または手動実行から呼び出される。
    """
    if os.getenv("CRAWLER_ENABLED", "").lower() not in ("true", "1"):
        print("[Crawler] CRAWLER_ENABLED が未設定のためスキップ（有効にするには .env に CRAWLER_ENABLED=true を追加）")
        return

    print(f"[Crawler] 開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    db = SessionLocal()
    try:
        # site_type_id を取得（slug="crime" 優先、なければ最初のタイプ）
        site_type = (
            db.query(SiteType).filter(SiteType.slug == "crime").first()
            or db.query(SiteType).first()
        )
        if not site_type:
            print("[Crawler] SiteType が見つかりません。Supabase に site_types レコードを登録してください。")
            return

        posted = 0
        skipped = 0

        for i, feed_conf in enumerate(RSS_FEEDS):
            print(f"[Crawler] RSS取得: {feed_conf['name']}")
            entries = await fetch_rss(feed_conf["url"])
            print(f"[Crawler] {len(entries)} 件取得")

            for entry in entries:
                try:
                    result = await process_entry(entry, site_type.id, db)
                    if result:
                        posted += 1
                    else:
                        skipped += 1
                except Exception as e:
                    print(f"[Crawler] エントリー処理エラー '{entry.get('title', '')[:40]}': {e}")
                    skipped += 1

                # Nominatim 利用規約: 1秒以上のインターバル
                await asyncio.sleep(1.5)

            # Google News への連続アクセスを避けるため RSS 間に少し待機
            if "news.google.com" in feed_conf["url"]:
                await asyncio.sleep(3)

        print(f"[Crawler] 完了: 投稿={posted}件 / スキップ={skipped}件")

    finally:
        db.close()


# ── 手動実行 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ニュースクローラー手動実行")
    parser.add_argument("--run-once", action="store_true", help="1回だけ実行して終了")
    args = parser.parse_args()

    if args.run_once:
        # CRAWLER_ENABLED チェックをバイパスして強制実行
        os.environ["CRAWLER_ENABLED"] = "true"
        asyncio.run(run_news_crawler())
    else:
        parser.print_help()
