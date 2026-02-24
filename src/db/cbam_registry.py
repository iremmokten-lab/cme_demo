from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, Index

from src.db.models import Base


def utcnow():
    return datetime.now(timezone.utc)


class CbamCnMapping(Base):
    """
    CN Code → CBAM Goods Registry (DB)

    match_type:
      - exact: cn_pattern tam eşleşme (örn: 72081000)
      - prefix: cn_pattern prefix (örn: 72, 2716, 2804)

    priority:
      - birden fazla kural eşleşirse:
        önce priority (yüksek), sonra pattern uzunluğu (uzun) kazanır.
    """

    __tablename__ = "cbam_cn_mappings"

    id = Column(Integer, primary_key=True)

    cn_pattern = Column(String(32), nullable=False, index=True)
    match_type = Column(String(10), nullable=False, default="prefix", index=True)  # exact / prefix

    cbam_good_key = Column(String(64), nullable=False, default="other", index=True)
    cbam_good_name = Column(String(200), nullable=False, default="Diğer")

    priority = Column(Integer, nullable=False, default=100)
    active = Column(Boolean, nullable=False, default=True, index=True)

    notes = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        Index("ix_cbam_cn_mappings_active_type", "active", "match_type"),
    )
