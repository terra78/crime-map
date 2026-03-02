import os
import re
import asyncio
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Header, BackgroundTasks
from sqlalchemy.orm import Session
from database import get_db
from models import Report, ModerationLog

# バックフィルの実行状態を追跡
_backfill_task: asyncio.Task | None = None
_backfill_started_at: datetime | None = None

router = APIRouter(prefix="/api/admin", tags=["admin"])


def verify_admin(x_admin_token: str = Header(...)):
    if x_admin_token != os.getenv("ADMIN_TOKEN"):
        raise HTTPException(status_code=401, detail="Unauthorized")


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


# ── 承認 ──────────────────────────────────────────────────────────────────────
@router.post("/approve/{report_id}", dependencies=[Depends(verify_admin)])
def approve(report_id: int, db: Session = Depends(get_db)):
    r = db.query(Report).filter(Report.id == report_id).first()
    if not r:
        raise HTTPException(404, "not found")
    r.status      = "human_approved"
    r.approved_at = datetime.now()
    db.add(ModerationLog(report_id=report_id, action="human_approve", actor="admin"))
    db.commit()
    return {"status": "approved"}


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
