"""
過去記事一括取得スクリプト (バックフィル)

Google News の日付範囲指定検索を使って、RSSに載らない過去記事を
月ごとにさかのぼり収集する。既存の process_entry() を通すため重複は自動排除。

使い方:
  python backfill_crawler.py --months 3           # 過去3ヶ月分
  python backfill_crawler.py --from-date 2024-10-01  # 指定日から現在まで
  python backfill_crawler.py --months 6 --dry-run    # DBに保存せず件数のみ確認

注意:
  - Nominatim 利用規約により記事1件あたり約1.5秒かかる
  - 過去3ヶ月でおよそ500〜1500件処理（15〜40分程度）
  - 同一記事がヒットしても source_url でDB側が重複排除する
"""

import os
import asyncio
import argparse
from datetime import date, timedelta
from calendar import monthrange

import httpx

from database import SessionLocal
from models import SiteType
from news_crawler import (
    fetch_rss,
    process_entry,
    source_url_exists,
    is_crime_related,
)

# ── 月次バックフィル用の Google News 検索キーワード ──────────────────────────
# 異なるキーワードの組み合わせで Google News の異なる記事セットを収集する。
# 地方紙・ブロック紙を含む国内数百媒体が横断的にヒットする。
BACKFILL_SEARCH_TERMS = [
    "逮捕 事件",
    "送検 起訴 書類送検",
    "殺人 強盗 放火",
    "暴行 傷害 脅迫",
    "窃盗 空き巣 車上ねらい",
    "詐欺 特殊詐欺 横領",
    "覚醒剤 薬物 大麻",
    "性犯罪 わいせつ 強制性交",
    "DV ストーカー 児童虐待",
    "不法滞在 密入国 入管",
    "サイバー犯罪 不正アクセス フィッシング",
    "組織犯罪 暴力団 摘発",
    "住居侵入 器物損壊",
    "恐喝 脅迫 ゆすり",
]


# ── Google News 日付範囲 RSS ──────────────────────────────────────────────────

async def fetch_google_news_daterange(
    search_term: str,
    after: date,
    before: date,
) -> list[dict]:
    """
    Google News RSS を日付範囲指定で検索して記事リストを返す。
    Google News は `after:YYYY-MM-DD before:YYYY-MM-DD` 演算子をサポートする。
    """
    query = (
        f"{search_term} "
        f"after:{after.strftime('%Y-%m-%d')} "
        f"before:{before.strftime('%Y-%m-%d')}"
    )
    # URL エンコード（httpx の params= は日本語を適切にエンコードしないため手動）
    import urllib.parse
    encoded_q = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_q}&hl=ja&gl=JP&ceid=JP:ja"
    return await fetch_rss(url)


# ── 月リスト生成 ──────────────────────────────────────────────────────────────

def generate_monthly_ranges(start: date, end: date) -> list[tuple[date, date]]:
    """
    start〜end を月ごとの (month_start, month_end) タプルリストに分割する。
    例: 2024-11-01 〜 2025-01-31 → [(2024-11-01, 2024-11-30), (2024-12-01, 2024-12-31), ...]
    """
    ranges = []
    current = date(start.year, start.month, 1)
    while current <= end:
        last_day = monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)
        # end を超えないようにクリップ
        ranges.append((current, min(month_end, end)))
        # 翌月へ
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return ranges


# ── メイン ────────────────────────────────────────────────────────────────────

async def run_backfill(
    months: int = 3,
    from_date: date | None = None,
    dry_run: bool = False,
) -> None:
    """
    過去記事を月ごとに Google News 検索でさかのぼり収集する。

    Args:
        months:    さかのぼる月数（from_date が指定された場合は無視）
        from_date: 収集開始日（指定しない場合は今日 - months ヶ月）
        dry_run:   True の場合 DB に保存せず件数のみ出力する
    """
    # CRAWLER_ENABLED チェックをバイパス（バックフィルは明示的に実行するため）
    os.environ["CRAWLER_ENABLED"] = "true"

    today = date.today()
    start = from_date if from_date else (today - timedelta(days=months * 30))

    print("=" * 60)
    print(f"[Backfill] 対象期間   : {start} ～ {today}")
    print(f"[Backfill] 検索キーワード数: {len(BACKFILL_SEARCH_TERMS)}")
    print(f"[Backfill] ドライラン : {'ON（DB保存しない）' if dry_run else 'OFF（DB保存する）'}")
    print("=" * 60)

    monthly_ranges = generate_monthly_ranges(start, today)
    print(f"[Backfill] 月数: {len(monthly_ranges)} ヶ月分")

    db = SessionLocal()
    try:
        site_type = (
            db.query(SiteType).filter(SiteType.slug == "crime").first()
            or db.query(SiteType).first()
        )
        if not site_type:
            print("[Backfill] エラー: SiteType が見つかりません")
            return

        total_posted = 0
        total_skipped = 0

        for period_start, period_end in monthly_ranges:
            label = period_start.strftime("%Y年%m月")
            print(f"\n[Backfill] ━━━ {label} ({period_start} ～ {period_end}) ━━━")

            for term in BACKFILL_SEARCH_TERMS:
                print(f"[Backfill]   検索: '{term}'", end="", flush=True)
                entries = await fetch_google_news_daterange(term, period_start, period_end)
                print(f" → {len(entries)} 件ヒット")

                for entry in entries:
                    try:
                        if dry_run:
                            # ドライラン: DB保存せず件数のみカウント
                            url = entry.get("url", "")
                            if not source_url_exists(db, url):
                                if is_crime_related(entry["title"], entry.get("summary", "")):
                                    total_posted += 1
                                    print(f"    [DRY] {entry['title'][:60]}")
                                else:
                                    total_skipped += 1
                            else:
                                total_skipped += 1
                        else:
                            result = await process_entry(entry, site_type.id, db)
                            if result:
                                total_posted += 1
                            else:
                                total_skipped += 1
                    except Exception as e:
                        print(f"    [ERROR] {entry.get('title', '')[:40]}: {e}")
                        total_skipped += 1

                    # geocode() 内でレート制限を管理しているため短めに待機
                    await asyncio.sleep(0.3)

                # Google News への連続アクセスを避けるため少し待機
                await asyncio.sleep(3)

        print("\n" + "=" * 60)
        action = "投稿予定" if dry_run else "投稿"
        print(f"[Backfill] 完了: {action}={total_posted}件 / スキップ={total_skipped}件")
        print("=" * 60)

    finally:
        db.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="過去記事バックフィル（Google News 日付範囲検索）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python backfill_crawler.py --months 3
  python backfill_crawler.py --from-date 2024-10-01
  python backfill_crawler.py --months 6 --dry-run
        """,
    )
    parser.add_argument(
        "--months",
        type=int,
        default=3,
        help="さかのぼる月数（デフォルト: 3）",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        metavar="YYYY-MM-DD",
        help="収集開始日（指定した場合 --months より優先）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DBに保存せず件数のみ確認する",
    )
    args = parser.parse_args()

    from_date = None
    if args.from_date:
        try:
            from_date = date.fromisoformat(args.from_date)
        except ValueError:
            print(f"エラー: --from-date の日付フォーマットが不正です: {args.from_date}")
            print("  正しい形式: YYYY-MM-DD（例: 2024-10-01）")
            exit(1)

    asyncio.run(
        run_backfill(
            months=args.months,
            from_date=from_date,
            dry_run=args.dry_run,
        )
    )
