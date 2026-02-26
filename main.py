from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from pathlib import Path

from routers import reports, admin

# ── バッチジョブ ──────────────────────────────────────────────────────────────
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

# ── アプリ起動/終了 ───────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
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

@app.get("/")
def root():
    return {"status": "ok", "message": "Crime Map API"}
