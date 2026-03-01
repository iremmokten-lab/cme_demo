from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Index,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from src.db.models import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# CN Codes (global registry)
# ----------------------------
class MasterCNCode(Base):
    __tablename__ = "md_cn_codes"

    code = Column(String(32), primary_key=True)
    description = Column(Text, default="")
    sector = Column(String(100), default="")
    source = Column(String(200), default="")

    valid_from = Column(DateTime(timezone=True), nullable=True)
    valid_to = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)


# ----------------------------
# Product master (versioned)
# ----------------------------
class MasterProduct(Base):
    __tablename__ = "md_products"
    __table_args__ = (
        Index("ix_md_products_company_logical", "company_id", "logical_id"),
        Index("ix_md_products_company_active", "company_id", "is_active"),
        UniqueConstraint("company_id", "logical_id", "version", name="uq_md_products_company_logical_version"),
    )

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)

    # Stable identity across versions
    logical_id = Column(String(36), nullable=False, index=True)  # UUID4

    name = Column(String(200), nullable=False)
    cn_code = Column(String(32), nullable=False, index=True)
    sector = Column(String(100), default="")

    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_to = Column(DateTime(timezone=True), nullable=True)

    version = Column(Integer, nullable=False, default=1)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)

    bom_children = relationship(
        "MasterBOMEdge",
        foreign_keys="MasterBOMEdge.parent_product_id",
        back_populates="parent_product",
        cascade="all, delete-orphan",
    )
    bom_parents = relationship(
        "MasterBOMEdge",
        foreign_keys="MasterBOMEdge.child_product_id",
        back_populates="child_product",
        cascade="all, delete-orphan",
    )


# ----------------------------
# Facility registry (versioned overlay)
# NOTE: Core Facility table exists already. We keep version history here for audit safety.
# ----------------------------
class MasterFacilityVersion(Base):
    __tablename__ = "md_facility_versions"
    __table_args__ = (
        Index("ix_md_fac_versions_company_facility", "company_id", "facility_id"),
        UniqueConstraint("company_id", "facility_id", "version", name="uq_md_fac_versions_company_facility_version"),
    )

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    country = Column(String(100), default="TR")
    sector = Column(String(200), default="")

    version = Column(Integer, nullable=False, default=1)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_to = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)


# ----------------------------
# BOM edges (versioned)
# ----------------------------
class MasterBOMEdge(Base):
    __tablename__ = "md_bom_edges"
    __table_args__ = (
        Index("ix_md_bom_company_parent", "company_id", "parent_product_id"),
        Index("ix_md_bom_company_child", "company_id", "child_product_id"),
    )

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)

    parent_product_id = Column(Integer, ForeignKey("md_products.id"), nullable=False, index=True)
    child_product_id = Column(Integer, ForeignKey("md_products.id"), nullable=False, index=True)

    ratio = Column(Float, nullable=False, default=1.0)
    unit = Column(String(50), default="kg")

    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_to = Column(DateTime(timezone=True), nullable=True)

    version = Column(Integer, nullable=False, default=1)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)

    parent_product = relationship("MasterProduct", foreign_keys=[parent_product_id], back_populates="bom_children")
    child_product = relationship("MasterProduct", foreign_keys=[child_product_id], back_populates="bom_parents")


# ----------------------------
# Change Log (master data specific)
# ----------------------------
class MasterDataChange(Base):
    __tablename__ = "md_change_log"
    __table_args__ = (
        Index("ix_md_change_company_time", "company_id", "created_at"),
        Index("ix_md_change_entity", "entity_type", "entity_logical_id"),
    )

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    entity_type = Column(String(50), nullable=False)  # facility/product/cn_code/bom
    entity_logical_id = Column(String(64), nullable=False)  # logical_id or facility_id or code

    operation = Column(String(20), nullable=False)  # create/update/deactivate

    old_hash = Column(String(64), default="")
    new_hash = Column(String(64), default="")

    note = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)
