import hashlib
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Request, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel
from database import get_db
from models import Report, SiteType, ModerationLog
from ai_verify import verify_report

router = APIRouter(prefix="/api/reports", tags=["reports"])


# ── スキーマ ──────────────────────────────────────────────────────────────────
class ReportCreate(BaseModel):
    site_type_id: int
    title:        Optional[str] = None
    description:  Optional[str] = None
    lat:          float
    lng:          float
    address:      Optional[str] = None
    occurred_at:  Optional[date] = None
    data:         dict = {}
    source_url:   Optional[str] = None


# ── 投稿 ──────────────────────────────────────────────────────────────────────
@router.post("")
async def create_report(
    body: ReportCreate,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    # 投稿者IPをハッシュ化
    ip = request.client.host
    ip_hash = hashlib.sha256(ip.encode()).hexdigest()[:16]

    report = Report(
        site_type_id = body.site_type_id,
        title        = body.title,
        description  = body.description,
        location     = f"SRID=4326;POINT({body.lng} {body.lat})",
        address      = body.address,
        occurred_at  = body.occurred_at,
        data         = body.data,
        source_url   = body.source_url,
        status       = "pending",
        submitted_by = ip_hash,
    )
    db.add(report)
    db.commit()
    db.refresh(report)

    # ソースURLがあればバックグラウンドでAI検証
    if body.source_url:
        background_tasks.add_task(run_ai_verify, report.id, db)

    return {"id": report.id, "status": report.status}


async def run_ai_verify(report_id: int, db: Session):
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        return
    score, reason = await verify_report(report)
    report.ai_score  = score
    report.ai_reason = reason
    if score >= 0.8:
        report.status = "ai_approved"
    elif score < 0.5:
        report.status = "rejected"
    else:
        report.status = "pending"  # 管理者確認へ

    log = ModerationLog(report_id=report_id, action="ai_check",
                        actor="ai", reason=f"score={score:.2f}: {reason}")
    db.add(log)
    db.commit()


# ── 地図表示用一覧取得 ────────────────────────────────────────────────────────
@router.get("")
def list_reports(
    site_type_id: Optional[int] = None,
    # 地図の表示範囲（緯度経度のバウンディングボックス）
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lng: Optional[float] = None,
    db: Session = Depends(get_db),
):
    q = db.query(Report).filter(Report.status.in_(["ai_approved", "human_approved"]))

    if site_type_id:
        q = q.filter(Report.site_type_id == site_type_id)

    # バウンディングボックスフィルタ
    if all(v is not None for v in [min_lat, max_lat, min_lng, max_lng]):
        q = q.filter(text(
            f"ST_Within(location, ST_MakeEnvelope({min_lng},{min_lat},{max_lng},{max_lat},4326))"
        ))

    reports = q.order_by(Report.created_at.desc()).limit(500).all()

    return [
        {
            "id":          r.id,
            "title":       r.title,
            "lat":         db.execute(text(f"SELECT ST_Y('{r.location}'::geometry)")).scalar(),
            "lng":         db.execute(text(f"SELECT ST_X('{r.location}'::geometry)")).scalar(),
            "address":     r.address,
            "occurred_at": str(r.occurred_at) if r.occurred_at else None,
            "data":        r.data,
            "site_type_id": r.site_type_id,
        }
        for r in reports
    ]


# ── 詳細 ──────────────────────────────────────────────────────────────────────
@router.get("/{report_id}")
def get_report(report_id: int, db: Session = Depends(get_db)):
    r = db.query(Report).filter(Report.id == report_id).first()
    if not r:
        return {"error": "not found"}, 404
    return r
