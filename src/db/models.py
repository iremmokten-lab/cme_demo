from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from src.db.session import Base

# -------------------------
# Core Tables
# -------------------------

class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class Project(Base):
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"))

# -------------------------
# Evidence Documents
# -------------------------

class EvidenceDocument(Base):
    __tablename__ = "evidence_documents"
    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"))

# -------------------------
# Calculation Snapshots (FK Hatasını Çözen Model)
# -------------------------

class CalculationSnapshot(Base):
    __tablename__ = "calculation_snapshots"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    description = Column(Text)

# -------------------------
# CBAM Methodology Evidence
# -------------------------

class CBAMMethodologyEvidence(Base):
    __tablename__ = "cbam_methodology_evidence"
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("calculation_snapshots.id"))
    evidence_id = Column(Integer, ForeignKey("evidence_documents.id"))
