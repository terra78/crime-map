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

    # ── site_types の国籍一覧を文部科学省基準に更新 ─────────────────────────
    _new_nationality_groups = [
        {"label": "アジア", "options": ["日本","中国","韓国","台湾","インド","ネパール","タイ","ベトナム","カンボジア","スリランカ","ミャンマー","パキスタン","バングラデシュ","マレーシア","シンガポール","インドネシア","フィリピン","香港","モンゴル","ブータン","ラオス","ブルネイ","マカオ","モルディブ","東ティモール","その他（アジア地域）"]},
        {"label": "中近東", "options": ["イラン","トルコ","シリア","レバノン","イスラエル","ヨルダン","イラク","クウェート","サウジアラビア","アフガニスタン","パレスチナ","イエメン","アラブ首長国連邦","バーレーン","オマーン","カタール","その他（中近東地域）"]},
        {"label": "アフリカ", "options": ["エジプト","スーダン","リビア","チュニジア","アルジェリア","マダガスカル","ケニア","タンザニア","コンゴ民主共和国","ナイジェリア","ガーナ","リベリア","ガボン","コンゴ共和国","カメルーン","ザンビア","コートジボワール","モロッコ","セネガル","エチオピア","ギニア","ウガンダ","ジンバブエ","南アフリカ","モーリタニア","トーゴ","中央アフリカ","ベナン","マラウイ","ギニアビサウ","スワジランド","エリトリア","コモロ","ナミビア","ボツワナ","マリ","ニジェール","モーリシャス","レソト","セーシェル","ソマリア","モザンビーク","ルワンダ","シエラレオネ","ブルンジ","ジブチ","ガンビア","チャド","その他（アフリカ地域）"]},
        {"label": "大洋州", "options": ["オーストラリア","ニュージーランド","パプアニューギニア","フィジー","パラオ","マーシャル","ミクロネシア","サモア独立国","トンガ","キリバス","ナウル","ソロモン諸島","ツバル","バヌアツ","クック諸島","ニウエ","トケラウ","ニューカレドニア","公海","その他（大洋州地域）"]},
        {"label": "北米", "options": ["カナダ","アメリカ合衆国","その他（北米地域）"]},
        {"label": "中南米", "options": ["メキシコ","グアテマラ","エルサルバドル","ニカラグア","コスタリカ","キューバ","ドミニカ共和国","ブラジル","パラグアイ","ウルグアイ","アルゼンチン","チリ","ボリビア","ペルー","エクアドル","コロンビア","ベネズエラ","ホンジュラス","パナマ","ジャマイカ","トリニダード・トバゴ","バハマ","アンティグア・バーブーダ","バルバドス","ドミニカ国","グレナダ","セントクリストファー・ネーヴィス","セントルシア","セントビンセント","スリナム","ガイアナ","ベリーズ","ハイチ","その他（中南米地域）"]},
        {"label": "ヨーロッパ", "options": ["アイスランド","フィンランド","スウェーデン","ノルウェー","デンマーク","アイルランド","英国","ベルギー","ルクセンブルク","オランダ","ドイツ","フランス","スペイン","ポルトガル","イタリア","マルタ","ギリシャ","オーストリア","スイス","ポーランド","チェコ","ハンガリー","セルビア・モンテネグロ","ルーマニア","ブルガリア","アルバニア","ロシア","エストニア","ラトビア","リトアニア","スロバキア","ウクライナ","ウズベキスタン","カザフスタン","ベラルーシ","クロアチア","スロベニア","マケドニア旧ユーゴスラビア共和国","ボスニア・ヘルツェゴビナ","アンドラ公国","バチカン","キルギス","アゼルバイジャン","グルジア","タジキスタン","トルクメニスタン","アルメニア","モルドバ","キプロス","その他（ヨーロッパ地域）"]},
        {"label": "その他", "options": ["不明","その他"]},
    ]
    try:
        from models import SiteType
        import json as _json
        with SessionLocal() as _db:
            _st = _db.query(SiteType).filter(SiteType.slug == "crime").first()
            if _st and _st.fields:
                _fields = list(_st.fields)
                _updated = False
                for _i, _f in enumerate(_fields):
                    if isinstance(_f, dict) and _f.get("key") == "nationality_type":
                        _f_copy = dict(_f)
                        _f_copy["groups"] = _new_nationality_groups
                        if "options" in _f_copy:
                            del _f_copy["options"]
                        _fields[_i] = _f_copy
                        _updated = True
                        break
                if _updated:
                    _st.fields = _fields
                    _db.commit()
                    print("[DB] site_types 国籍一覧を更新しました")
    except Exception as _e:
        print(f"[DB] 国籍一覧の更新をスキップ: {_e}")

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
