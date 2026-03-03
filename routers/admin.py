import os
import re
import asyncio
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db
from models import Report, ModerationLog, Admin

# バックフィルの実行状態を追跡
_backfill_task: asyncio.Task | None = None
_backfill_started_at: datetime | None = None

# 魚拓バックフィルの実行状態
_archive_task: asyncio.Task | None = None
_archive_started_at: datetime | None = None

router = APIRouter(prefix="/api/admin", tags=["admin"])


def verify_admin(x_admin_token: str = Header(...)):
    if x_admin_token != os.getenv("ADMIN_TOKEN"):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── 管理者プロフィール ─────────────────────────────────────────────────────────

class AdminProfileUpdate(BaseModel):
    email: Optional[str] = None


@router.get("/profile", dependencies=[Depends(verify_admin)])
def get_profile(db: Session = Depends(get_db)):
    admin = db.query(Admin).first()
    if not admin:
        raise HTTPException(404, "管理者アカウントが見つかりません")
    return {"id": admin.id, "email": admin.email, "created_at": str(admin.created_at)}


@router.patch("/profile", dependencies=[Depends(verify_admin)])
def update_profile(body: AdminProfileUpdate, db: Session = Depends(get_db)):
    admin = db.query(Admin).first()
    if not admin:
        raise HTTPException(404, "管理者アカウントが見つかりません")
    if body.email is not None:
        admin.email = body.email
    db.commit()
    return {"id": admin.id, "email": admin.email}


# ── 投稿物理削除（管理者専用・子テーブルをカスケード削除） ─────────────────────

@router.delete("/reports/{report_id}", dependencies=[Depends(verify_admin)])
def admin_delete_report(report_id: int, db: Session = Depends(get_db)):
    from sqlalchemy import text
    r = db.query(Report).filter(Report.id == report_id).first()
    if not r:
        raise HTTPException(404, "not found")
    # 1. コメントの自己参照FK(parent_id)を先にNULL化
    db.execute(text("UPDATE comments SET parent_id = NULL WHERE report_id = :rid"), {"rid": report_id})
    # 2. コメント削除
    db.execute(text("DELETE FROM comments WHERE report_id = :rid"), {"rid": report_id})
    # 3. モデレーションログ削除
    db.execute(text("DELETE FROM moderation_log WHERE report_id = :rid"), {"rid": report_id})
    # 4. 投稿本体削除
    db.execute(text("DELETE FROM reports WHERE id = :rid"), {"rid": report_id})
    db.commit()
    return {"status": "deleted", "id": report_id}


# ── 承認待ちキュー ────────────────────────────────────────────────────────────
@router.get("/queue", dependencies=[Depends(verify_admin)])
def get_queue(db: Session = Depends(get_db)):
    reports = (
        db.query(Report)
        .filter(Report.status == "pending")
        .order_by(Report.created_at.asc())
        .all()
    )
    return [
        {
            "id":          r.id,
            "title":       r.title,
            "description": r.description,
            "source_url":  r.source_url,
            "archive_url": r.archive_url,
            "ai_score":    r.ai_score,
            "ai_reason":   r.ai_reason,
            "data":        r.data,
            "created_at":  str(r.created_at),
        }
        for r in reports
    ]


# ── 承認待ちキュー インライン編集 ────────────────────────────────────────────
class QueueItemUpdate(BaseModel):
    nationality_type: Optional[str] = None


@router.patch("/queue/{report_id}", dependencies=[Depends(verify_admin)])
def update_queue_item(report_id: int, body: QueueItemUpdate, db: Session = Depends(get_db)):
    """承認待ちキューのデータフィールドをインライン編集（国籍など）"""
    r = db.query(Report).filter(Report.id == report_id).first()
    if not r:
        raise HTTPException(404, "not found")
    data = dict(r.data or {})
    if body.nationality_type is not None:
        data["nationality_type"] = body.nationality_type
    r.data = data
    db.commit()
    return {"status": "updated", "data": r.data}


# ── 承認 ──────────────────────────────────────────────────────────────────────
@router.post("/approve/{report_id}", dependencies=[Depends(verify_admin)])
def approve(report_id: int, db: Session = Depends(get_db)):
    r = db.query(Report).filter(Report.id == report_id).first()
    if not r:
        raise HTTPException(404, "not found")
    r.status      = "human_approved"
    r.approved_at = datetime.now()
    db.add(ModerationLog(report_id=report_id, action="human_approve", actor="admin"))

    # 訂正申請の場合、元投稿を "corrected" ステータスに変更
    original_id = (r.data or {}).get("original_report_id")
    if original_id:
        original = db.query(Report).filter(Report.id == int(original_id)).first()
        if original:
            original.status = "corrected"
            orig_data = dict(original.data or {})
            orig_data["corrected_by_report"] = report_id
            original.data = orig_data
            db.add(ModerationLog(
                report_id=int(original_id),
                action="corrected",
                actor="admin",
                reason=f"訂正申請 #{report_id} により訂正済み",
            ))

    db.commit()
    return {"status": "approved"}


# ── EXCLUDE_KEYWORDS 一括却下（※ /reject/{id} より前に定義する必要あり）────────
@router.post("/reject/exclude-keywords", dependencies=[Depends(verify_admin)])
def reject_by_exclude_keywords(db: Session = Depends(get_db)):
    """
    news_crawler.py の EXCLUDE_KEYWORDS に一致するタイトルの pending 記事を一括却下する。
    裁判・考察記事など、フィルター追加前に収集されてしまった記事を遡って処理する。
    """
    from news_crawler import EXCLUDE_KEYWORDS

    pending = (
        db.query(Report)
        .filter(Report.status == "pending")
        .all()
    )

    rejected_ids: list[int] = []
    for r in pending:
        if any(kw in (r.title or "") for kw in EXCLUDE_KEYWORDS):
            r.status = "rejected"
            db.add(ModerationLog(
                report_id=r.id,
                action="auto_reject_exclude_keywords",
                actor="admin",
            ))
            rejected_ids.append(r.id)

    db.commit()
    print(f"[Admin] EXCLUDE_KEYWORDS 一括却下: {len(rejected_ids)}件")
    return {
        "status":         "done",
        "rejected_count": len(rejected_ids),
    }


# ── 却下 ──────────────────────────────────────────────────────────────────────
@router.post("/reject/{report_id}", dependencies=[Depends(verify_admin)])
def reject(report_id: int, db: Session = Depends(get_db)):
    r = db.query(Report).filter(Report.id == report_id).first()
    if not r:
        raise HTTPException(404, "not found")
    r.status = "rejected"
    db.add(ModerationLog(report_id=report_id, action="human_reject", actor="admin"))
    db.commit()
    return {"status": "rejected"}


# ── 統計（サマリー） ──────────────────────────────────────────────────────────
@router.get("/stats", dependencies=[Depends(verify_admin)])
def stats(db: Session = Depends(get_db)):
    from sqlalchemy import func
    total    = db.query(func.count(Report.id)).scalar()
    approved = db.query(func.count(Report.id)).filter(
        Report.status.in_(["ai_approved", "human_approved"])).scalar()
    pending  = db.query(func.count(Report.id)).filter(Report.status == "pending").scalar()
    rejected = db.query(func.count(Report.id)).filter(Report.status == "rejected").scalar()
    return {
        "total": total, "approved": approved,
        "pending": pending, "rejected": rejected,
    }


# ── 月別統計（発生日基準） ────────────────────────────────────────────────────
@router.get("/stats/monthly", dependencies=[Depends(verify_admin)])
def stats_monthly(db: Session = Depends(get_db)):
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT TO_CHAR(occurred_at, 'YYYY-MM') AS month, COUNT(*) AS cnt
        FROM reports
        WHERE status IN ('ai_approved', 'human_approved')
          AND occurred_at IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 12
    """)).fetchall()
    return [{"month": r[0], "count": r[1]} for r in rows]


# ── バックフィル（過去記事一括取得）────────────────────────────────────────────

@router.post("/backfill", dependencies=[Depends(verify_admin)])
async def trigger_backfill(months: int = 1, from_date: str | None = None):
    """
    過去記事バックフィルを非同期バックグラウンドで開始する。
    SSH不要。Renderのログで進捗確認できる。

    - months: さかのぼる月数（デフォルト1）
    - from_date: 開始日 YYYY-MM-DD（指定した場合 months より優先）
    """
    global _backfill_task, _backfill_started_at

    if _backfill_task and not _backfill_task.done():
        elapsed = (datetime.now() - _backfill_started_at).seconds // 60
        return {
            "status": "already_running",
            "message": f"バックフィルはすでに実行中です（{elapsed}分経過）",
        }

    from backfill_crawler import run_backfill
    from datetime import date

    parsed_from = None
    if from_date:
        try:
            parsed_from = date.fromisoformat(from_date)
        except ValueError:
            raise HTTPException(400, f"from_date の形式が不正です（YYYY-MM-DD）: {from_date}")

    _backfill_started_at = datetime.now()
    _backfill_task = asyncio.create_task(
        run_backfill(months=months, from_date=parsed_from)
    )

    return {
        "status":  "started",
        "message": f"バックフィル開始（過去{months}ヶ月分）。Renderのログで進捗を確認してください。",
        "months":  months,
        "from_date": str(parsed_from) if parsed_from else None,
    }


@router.get("/backfill/status", dependencies=[Depends(verify_admin)])
async def backfill_status():
    """バックフィルの実行状態を返す"""
    global _backfill_task, _backfill_started_at

    if _backfill_task is None:
        return {"status": "not_started"}

    if _backfill_task.done():
        exc = _backfill_task.exception()
        if exc:
            return {"status": "error", "error": str(exc)}
        elapsed = (datetime.now() - _backfill_started_at).seconds // 60
        return {"status": "completed", "elapsed_minutes": elapsed}

    elapsed = (datetime.now() - _backfill_started_at).seconds // 60
    return {"status": "running", "elapsed_minutes": elapsed}


# ── 魚拓バックフィル ──────────────────────────────────────────────────────────

async def run_archive_backfill(limit: int) -> None:
    """archive_url 未設定の承認済み記事に魚拓URLをバックグラウンドで付与する"""
    from archive import needs_archive, save_to_archive
    from database import SessionLocal

    db = SessionLocal()
    try:
        reports = (
            db.query(Report)
            .filter(
                Report.status.in_(["ai_approved", "human_approved"]),
                Report.source_url.isnot(None),
                Report.archive_url.is_(None),
            )
            .order_by(Report.created_at.desc())
            .limit(limit)
            .all()
        )
        print(f"[Archive BF] 対象: {len(reports)}件")
        saved = skipped = failed = 0

        for r in reports:
            if not needs_archive(r.source_url):
                skipped += 1
                continue
            archive_url = await save_to_archive(r.source_url)
            if archive_url:
                r.archive_url = archive_url
                db.commit()
                saved += 1
                print(f"[Archive BF] [{r.id}] OK: {archive_url[:70]}")
            else:
                failed += 1
                print(f"[Archive BF] [{r.id}] 失敗: {r.source_url[:70]}")
            await asyncio.sleep(5)  # archive.org への負荷対策

        print(f"[Archive BF] 完了: 保存={saved} スキップ={skipped} 失敗={failed}")
    finally:
        db.close()


@router.post("/archive/backfill", dependencies=[Depends(verify_admin)])
async def trigger_archive_backfill(limit: int = 100):
    """
    archive_url 未設定の承認済み記事に魚拓URLを一括付与する。
    - limit: 1回あたりの処理件数（デフォルト100）
    """
    global _archive_task, _archive_started_at

    if _archive_task and not _archive_task.done():
        elapsed = (datetime.now() - _archive_started_at).seconds // 60
        return {"status": "already_running", "message": f"実行中です（{elapsed}分経過）"}

    _archive_started_at = datetime.now()
    _archive_task = asyncio.create_task(run_archive_backfill(limit=limit))

    return {
        "status":  "started",
        "message": f"魚拓バックフィル開始（最大{limit}件）",
        "limit":   limit,
    }


@router.get("/archive/backfill/status", dependencies=[Depends(verify_admin)])
async def archive_backfill_status():
    """魚拓バックフィルの実行状態を返す"""
    global _archive_task, _archive_started_at

    if _archive_task is None:
        return {"status": "not_started"}
    if _archive_task.done():
        exc = _archive_task.exception()
        if exc:
            return {"status": "error", "error": str(exc)}
        elapsed = (datetime.now() - _archive_started_at).seconds // 60
        return {"status": "completed", "elapsed_minutes": elapsed}
    elapsed = (datetime.now() - _archive_started_at).seconds // 60
    return {"status": "running", "elapsed_minutes": elapsed}


# ── dataフィールド別集計 ───────────────────────────────────────────────────────
@router.get("/stats/breakdown/{field}", dependencies=[Depends(verify_admin)])
def stats_breakdown(field: str, db: Session = Depends(get_db)):
    # フィールド名は英数字+アンダースコアのみ許可（SQLインジェクション対策）
    if not re.match(r'^[a-zA-Z0-9_]+$', field):
        raise HTTPException(400, "Invalid field name")
    from sqlalchemy import text
    rows = db.execute(text(f"""
        SELECT data->>'{field}' AS value, COUNT(*) AS cnt
        FROM reports
        WHERE status IN ('ai_approved', 'human_approved')
          AND data->>'{field}' IS NOT NULL
          AND data->>'{field}' <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 30
    """)).fetchall()
    return [{"value": r[0], "count": r[1]} for r in rows]
