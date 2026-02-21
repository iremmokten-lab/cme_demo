from sqlalchemy.orm import Session
from src.db.models import Company, Project

def list_companies(db: Session) -> list[Company]:
    return db.query(Company).order_by(Company.name.asc()).all()

def get_or_create_company(db: Session, name: str) -> Company:
    name = (name or "").strip()
    if not name:
        raise ValueError("Company adı boş olamaz.")
    existing = db.query(Company).filter(Company.name == name).one_or_none()
    if existing:
        return existing
    c = Company(name=name)
    db.add(c)
    db.commit()
    db.refresh(c)
    return c

def list_projects(db: Session, company_id: int) -> list[Project]:
    return (
        db.query(Project)
        .filter(Project.company_id == company_id)
        .order_by(Project.created_at.desc())
        .all()
    )

def create_project(db: Session, company_id: int, name: str) -> Project:
    name = (name or "").strip()
    if not name:
        raise ValueError("Project adı boş olamaz.")
    p = Project(company_id=company_id, name=name)
    db.add(p)
    db.commit()
    db.refresh(p)
    return p
