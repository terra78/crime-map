from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from pathlib import Path

from routers import reports, admin, admin_ui, site_types, prefecture_stats, webhooks, comments, contact

# ── バッチジョブ ──────────────────────────────────────────────────────────────
async def job_crawler():
    from news_crawler import run_news_crawler
    await run_news_crawler()

def job_monthly():
    from batch_report import generate_pdf_monthly, generate_csv_monthly
    now   = datetime.now()
    month = now.month - 1 if now.month > 1 else 12
    year  = now.year  if now.month > 1 else now.year - 1
    tag   = f"{year}{month:02d}"
    out   = Path("./reports"); out.mkdir(exist_ok=True)
    generate_pdf_monthly(year, month, out / f"report_monthly_{tag}.pdf")
    generate_csv_monthly(year, month, out / f"report_monthly_{tag}.csv")

def job_yearly():
    from batch_report import generate_pdf_yearly, generate_csv_yearly
    year = datetime.now().year - 1
    out  = Path("./reports"); out.mkdir(exist_ok=True)
    generate_pdf_yearly(year, out / f"report_yearly_{year}.pdf")
    generate_csv_yearly(year, out / f"report_yearly_{year}.csv")

scheduler = AsyncIOScheduler(timezone="Asia/Tokyo")
scheduler.add_job(job_monthly, CronTrigger(day=1,   hour=6,  minute=0), id="monthly")
scheduler.add_job(job_yearly,  CronTrigger(month=1, day=5, hour=7, minute=0), id="yearly")
scheduler.add_job(job_crawler, "interval", hours=6, id="crawler",
                  next_run_time=datetime.now())  # 起動直後に1回実行

# ── アプリ起動/終了 ───────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── DB テーブル作成 + 管理者アカウント初期シード ──────────────────────────
    from database import engine, SessionLocal
    from models import Base, Admin
    from sqlalchemy import text as _text
    Base.metadata.create_all(bind=engine)   # 未作成テーブルのみ作成（既存テーブルは変更しない）
    # 既存 admins テーブルに clerk_user_id カラムを追加（IF NOT EXISTS で冪等）
    with engine.connect() as _conn:
        _conn.execute(_text(
            "ALTER TABLE admins ADD COLUMN IF NOT EXISTS clerk_user_id VARCHAR(256)"
        ))
        _conn.commit()
    with SessionLocal() as _db:
        if not _db.query(Admin).filter(Admin.email == "s.tera78@gmail.com").first():
            _db.add(Admin(email="s.tera78@gmail.com"))
            _db.commit()
            print("[DB] 管理者アカウント初期化: s.tera78@gmail.com")

    scheduler.start()
    print("[Scheduler] 起動")
    yield
    scheduler.shutdown()

app = FastAPI(title="Crime Map API", lifespan=lifespan)

# CORS（フロントエンドからのアクセスを許可）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # 本番では Vercel の URL に絞る
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(reports.router)
app.include_router(admin.router)
app.include_router(admin_ui.router)
app.include_router(site_types.router)
app.include_router(prefecture_stats.router)
app.include_router(webhooks.router)
app.include_router(comments.router)
app.include_router(contact.router)

@app.get("/")
def root():
    return {"status": "ok", "message": "Crime Map API"}
