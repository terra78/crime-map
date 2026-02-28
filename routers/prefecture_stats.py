"""
都道府県別犯罪統計 API（警察庁オープンデータ由来）
フロントエンドのヒートマップ・バブルレイヤー用
"""
from typing import Optional
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db
from models import PrefectureStats

router = APIRouter(prefix="/api/prefecture_stats", tags=["prefecture_stats"])


# ── 一覧（地図表示用） ────────────────────────────────────────────────────────
@router.get("")
def list_prefecture_stats(
    year:           Optional[int] = None,
    crime_type:     Optional[str] = None,   # 既存 crime_type でフィルタ
    crime_category: Optional[str] = None,   # 警察庁罪種分類でフィルタ
    db: Session = Depends(get_db),
):
    """
    都道府県別・罪種別の集計データを返す。
    フロントエンドでバブル半径 = count_recognized にして表示を想定。
    """
    q = db.query(PrefectureStats)

    if year:
        q = q.filter(PrefectureStats.year == year)
    if crime_type:
        q = q.filter(PrefectureStats.crime_type == crime_type)
    if crime_category:
        q = q.filter(PrefectureStats.crime_category == crime_category)

    rows = q.order_by(
        PrefectureStats.year.desc(),
        PrefectureStats.prefecture_code,
    ).all()

    return [
        {
            "id":               r.id,
            "year":             r.year,
            "prefecture_code":  r.prefecture_code,
            "prefecture_name":  r.prefecture_name,
            "crime_category":   r.crime_category,
            "crime_type":       r.crime_type,
            "count_recognized": r.count_recognized,
            "count_cleared":    r.count_cleared,
            "count_arrested":   r.count_arrested,
            "lat": db.execute(
                text(f"SELECT ST_Y('{r.location}'::geometry)")
            ).scalar(),
            "lng": db.execute(
                text(f"SELECT ST_X('{r.location}'::geometry)")
            ).scalar(),
            "source": r.source,
        }
        for r in rows
    ]


# ── 年一覧（フロントのセレクタ用） ───────────────────────────────────────────
@router.get("/years")
def list_years(db: Session = Depends(get_db)):
    from sqlalchemy import func
    years = (
        db.query(PrefectureStats.year)
        .distinct()
        .order_by(PrefectureStats.year.desc())
        .all()
    )
    return [y[0] for y in years]


# ── 罪種一覧（フロントのセレクタ用） ─────────────────────────────────────────
@router.get("/categories")
def list_categories(db: Session = Depends(get_db)):
    cats = (
        db.query(PrefectureStats.crime_category)
        .distinct()
        .order_by(PrefectureStats.crime_category)
        .all()
    )
    return [c[0] for c in cats if c[0]]


# ── 都道府県別ランキング（特定年・罪種） ──────────────────────────────────────
@router.get("/ranking")
def ranking(
    year:           int,
    crime_category: Optional[str] = None,
    limit:          int = 10,
    db: Session = Depends(get_db),
):
    """認知件数の多い都道府県トップN"""
    from sqlalchemy import func
    q = (
        db.query(
            PrefectureStats.prefecture_name,
            PrefectureStats.prefecture_code,
            func.sum(PrefectureStats.count_recognized).label("total"),
        )
        .filter(PrefectureStats.year == year)
    )
    if crime_category:
        q = q.filter(PrefectureStats.crime_category == crime_category)

    rows = (
        q.group_by(
            PrefectureStats.prefecture_name,
            PrefectureStats.prefecture_code,
        )
        .order_by(text("total DESC"))
        .limit(limit)
        .all()
    )
    return [
        {"prefecture_name": r[0], "prefecture_code": r[1], "count": r[2]}
        for r in rows
    ]
