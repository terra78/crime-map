from sqlalchemy import Column, Integer, String, Text, Float, Date, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from geoalchemy2 import Geometry
from database import Base

class SiteType(Base):
    __tablename__ = "site_types"

    id          = Column(Integer, primary_key=True)
    slug        = Column(String(64), unique=True, nullable=False)
    name        = Column(String(128), nullable=False)
    description = Column(Text)
    fields      = Column(JSONB, default=[])
    created_at  = Column(DateTime, server_default=func.now())


class Report(Base):
    __tablename__ = "reports"

    id           = Column(Integer, primary_key=True)
    site_type_id = Column(Integer, ForeignKey("site_types.id"))
    title        = Column(String(256))
    description  = Column(Text)
    location     = Column(Geometry("POINT", srid=4326))
    address      = Column(String(512))
    occurred_at  = Column(Date)
    data         = Column(JSONB, default={})
    source_url   = Column(String(1024))
    status       = Column(String(32), default="pending")
    ai_score     = Column(Float)
    ai_reason    = Column(Text)
    submitted_by = Column(String(256))
    archive_url  = Column(String(1024))
    created_at   = Column(DateTime, server_default=func.now())
    approved_at  = Column(DateTime)


class PrefectureStats(Base):
    """警察庁オープンデータ（e-Stat）の都道府県別・年次集計"""
    __tablename__ = "prefecture_stats"

    id                = Column(Integer, primary_key=True)
    year              = Column(Integer, nullable=False)
    prefecture_code   = Column(String(2), nullable=False)   # 01〜47
    prefecture_name   = Column(String(16), nullable=False)
    crime_category    = Column(String(64))   # 警察庁の罪種分類（例: 窃盗犯、凶悪犯）
    crime_type        = Column(String(64))   # 既存 crime_type へのマッピング後
    count_recognized  = Column(Integer)      # 認知件数
    count_cleared     = Column(Integer)      # 検挙件数
    count_arrested    = Column(Integer)      # 検挙人員
    location          = Column(Geometry("POINT", srid=4326))  # 都道府県庁の代表座標
    source            = Column(String(64), default="npa_estat")
    imported_at       = Column(DateTime, server_default=func.now())


class ModerationLog(Base):
    __tablename__ = "moderation_log"

    id         = Column(Integer, primary_key=True)
    report_id  = Column(Integer, ForeignKey("reports.id"))
    action     = Column(String(32))
    actor      = Column(String(128))
    reason     = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
