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


class ModerationLog(Base):
    __tablename__ = "moderation_log"

    id         = Column(Integer, primary_key=True)
    report_id  = Column(Integer, ForeignKey("reports.id"))
    action     = Column(String(32))
    actor      = Column(String(128))
    reason     = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
