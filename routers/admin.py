import os
import re
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from database import get_db
from models import Report, ModerationLog

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
