"""
WEB魚拓バックフィルスクリプト

reportsテーブルで source_url はあるが archive_url が NULL のレコードに対して
archive.org に魚拓を発行し、取得した URL で archive_url を更新する。

処理フロー（レコード1件ごと）:
  1. needs_archive() で公的ドメインを除外
  2. Wayback Machine Availability API で既存スナップショットを確認
     → あれば archive_url に即セット（保存リクエスト不要でコスト節約）
  3. 既存なければ /save/ エンドポイントで新規保存
     → 成功すれば archive_url をセット

使い方:
  python backfill_archive.py                 # 全件処理（遅い場合は --limit で分割）
  python backfill_archive.py --limit 200     # 最大200件
  python backfill_archive.py --delay 5       # リクエスト間隔 5秒（デフォルト: 3）
  python backfill_archive.py --skip-check    # 既存確認をスキップして全件 /save/ 発行
  python backfill_archive.py --dry-run       # DB保存せず対象件数のみ確認

注意:
  - archive.org への保存は1件あたり数秒〜数十秒かかる
  - 連続失敗が多い場合は --delay を増やす (10〜15秒推奨)
  - 数百件処理する場合は --limit 100 で分割実行を推奨
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime

import httpx

# SSH/pipe 環境でもリアルタイム表示
sys.stdout.reconfigure(line_buffering=True)

from database import SessionLocal
from models import Report
from archive import needs_archive, save_to_archive


# ── Wayback Availability API ──────────────────────────────────────────────────

async def check_already_archived(url: str) -> str | None:
    """
    Wayback Machine Availability API で既存の最新スナップショットURLを返す。
    見つからない場合は None。

    API: https://archive.org/wayback/available?url={url}
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://archive.org/wayback/available",
                params={"url": url},
                headers={"User-Agent": "CrimeMapArchiver/1.0"},
            )
            data = r.json()
            closest = data.get("archived_snapshots", {}).get("closest", {})
            if closest.get("available") and closest.get("url"):
                return closest["url"]
    except Exception:
        pass
    return None


# ── レコード1件の処理 ──────────────────────────────────────────────────────────

async def process_one(
    report: Report,
    db,
    delay: float,
    skip_check: bool,
    dry_run: bool,
) -> str:
    """
    戻り値:
      "saved"      … archive.org に新規保存して archive_url を更新
      "existing"   … 既存スナップショットを archive_url にセット
      "skipped"    … 公的ドメインのため魚拓不要
      "failed"     … archive.org への保存失敗
    """
    url = report.source_url

    # 公的ドメインはスキップ
    if not needs_archive(url):
        return "skipped"

    if dry_run:
        return "saved"  # ドライランはカウントのみ

    # ① 既存スナップショット確認（/save/ リクエストを節約）
    if not skip_check:
        existing = await check_already_archived(url)
        if existing:
            report.archive_url = existing
            db.commit()
            return "existing"

    # ② 新規保存（レート制限対策で delay 秒待機）
    await asyncio.sleep(delay)
    archive_url = await save_to_archive(url)
    if archive_url:
        report.archive_url = archive_url
        db.commit()
        return "saved"

    return "failed"


# ── メイン ────────────────────────────────────────────────────────────────────

async def run_backfill(
    limit: int = 0,
    delay: float = 3.0,
    skip_check: bool = False,
    dry_run: bool = False,
) -> None:
    """
    archive_url が NULL のレコードに魚拓 URL を付与する。
    """
    print("=" * 64)
    print(f"[Archive Backfill] 開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  ドライラン   : {'ON（DB更新しない）' if dry_run else 'OFF'}")
    print(f"  上限件数     : {limit if limit else '無制限'}")
    print(f"  リクエスト間隔: {delay} 秒")
    print(f"  既存確認スキップ: {'ON（全件 /save/ 発行）' if skip_check else 'OFF'}")
    print("=" * 64)

    if not os.getenv("DATABASE_URL"):
        print("[Archive Backfill] エラー: DATABASE_URL が設定されていません")
        return

    db = SessionLocal()
    try:
        query = (
            db.query(Report)
            .filter(Report.source_url.isnot(None))
            .filter(Report.archive_url.is_(None))
            .order_by(Report.id.asc())
        )
        if limit:
            query = query.limit(limit)

        targets = query.all()
        total = len(targets)
        print(f"[Archive Backfill] 対象レコード: {total} 件\n")

        if total == 0:
            print("[Archive Backfill] 対象なし。終了します。")
            return

        saved = existing_cnt = skipped = failed = 0

        for i, report in enumerate(targets, 1):
            status = await process_one(report, db, delay, skip_check, dry_run)

            if status == "saved":
                saved += 1
                tag = "💾 保存"
                detail = report.archive_url or "(dry-run)"
            elif status == "existing":
                existing_cnt += 1
                tag = "📦 既存"
                detail = report.archive_url or ""
            elif status == "skipped":
                skipped += 1
                tag = "⏭️  スキップ"
                detail = "公的ドメイン"
            else:
                failed += 1
                tag = "❌ 失敗"
                detail = ""

            # 進捗表示（URLは長いので先頭60文字）
            src = (report.source_url or "")[:60]
            arc = (detail[:50] + "…") if len(detail) > 50 else detail
            print(f"[{i:4d}/{total}] #{report.id:<6d} {tag}  {src}")
            if arc:
                print(f"             → {arc}")

        print()
        print("=" * 64)
        print(f"[Archive Backfill] 完了")
        print(f"  💾 新規保存  : {saved}")
        print(f"  📦 既存活用  : {existing_cnt}")
        print(f"  ⏭️  スキップ   : {skipped}（公的ドメイン）")
        print(f"  ❌ 失敗      : {failed}")
        if dry_run:
            print("  ※ ドライランのため DB は更新されていません")
        if failed > 0:
            print(f"  ⚠️  失敗が多い場合は --delay 10 以上に増やして再実行してください")
        print("=" * 64)

    finally:
        db.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="archive_url が NULL のレコードに WEB 魚拓 URL を付与するバックフィル",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python backfill_archive.py                  # 全件（時間がかかる）
  python backfill_archive.py --limit 100      # まず100件だけ試す
  python backfill_archive.py --dry-run        # 対象件数だけ確認
  python backfill_archive.py --delay 10       # archive.org に優しい間隔
  python backfill_archive.py --skip-check     # 既存確認せず全件新規保存
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="処理する最大件数（0=無制限、デフォルト: 0）",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=3.0,
        metavar="SEC",
        help="archive.org へのリクエスト間隔（秒、デフォルト: 3）",
    )
    parser.add_argument(
        "--skip-check",
        action="store_true",
        help="既存スナップショット確認をスキップして全件 /save/ を発行する",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB を更新せず対象件数のみ確認する",
    )
    args = parser.parse_args()

    asyncio.run(
        run_backfill(
            limit=args.limit,
            delay=args.delay,
            skip_check=args.skip_check,
            dry_run=args.dry_run,
        )
    )
