from __future__ import annotations
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Text, UniqueConstraint
from src.db.models import Base, utcnow

class MD_Product(Base):
    __tablename__="md_products"
    __table_args__=(UniqueConstraint("project_id","sku", name="uq_md_product_sku"), {"extend_existing": True})
    id=Column(Integer, primary_key=True)
    project_id=Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    sku=Column(String(80), nullable=False, index=True)
    name=Column(String(200), nullable=False)
    cn_code=Column(String(20), default="", index=True)
    uom=Column(String(20), default="t")
    is_active=Column(Boolean, default=True, index=True)
    created_at=Column(DateTime(timezone=True), default=utcnow)

class MD_Material(Base):
    __tablename__="md_materials"
    __table_args__=(UniqueConstraint("project_id","code", name="uq_md_material_code"), {"extend_existing": True})
    id=Column(Integer, primary_key=True)
    project_id=Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    code=Column(String(80), nullable=False, index=True)
    name=Column(String(200), nullable=False)
    uom=Column(String(20), default="t")
    is_active=Column(Boolean, default=True, index=True)
    created_at=Column(DateTime(timezone=True), default=utcnow)

class MD_Process(Base):
    __tablename__="md_processes"
    __table_args__=(UniqueConstraint("project_id","code", name="uq_md_process_code"), {"extend_existing": True})
    id=Column(Integer, primary_key=True)
    project_id=Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    code=Column(String(80), nullable=False, index=True)
    name=Column(String(200), nullable=False)
    sector=Column(String(80), default="generic")
    created_at=Column(DateTime(timezone=True), default=utcnow)

class MD_BOMItem(Base):
    __tablename__="md_bom_items"
    __table_args__=(UniqueConstraint("project_id","product_id","material_id", name="uq_md_bom_line"), {"extend_existing": True})
    id=Column(Integer, primary_key=True)
    project_id=Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    product_id=Column(Integer, ForeignKey("md_products.id"), nullable=False, index=True)
    material_id=Column(Integer, ForeignKey("md_materials.id"), nullable=False, index=True)
    qty_per_unit=Column(String(50), default="0")
    created_at=Column(DateTime(timezone=True), default=utcnow)

class MD_ChangeLog(Base):
    __tablename__="md_change_log"
    __table_args__={"extend_existing": True}
    id=Column(Integer, primary_key=True)
    project_id=Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    entity=Column(String(80), nullable=False, index=True)
    entity_id=Column(Integer, nullable=False, index=True)
    action=Column(String(40), nullable=False)
    diff_json=Column(Text, default="{}")
    actor_user_id=Column(Integer, nullable=True)
    created_at=Column(DateTime(timezone=True), default=utcnow)
