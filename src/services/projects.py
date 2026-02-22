from sqlalchemy import select
from src.db.session import db
from src.db.models import Company, Facility, Project

def list_companies_for_user(user):
    with db() as s:
        if user.role.startswith("consultant"):
            return s.execute(select(Company).order_by(Company.name)).scalars().all()
        # client -> sadece kendi company
        if not user.company_id:
            return []
        c = s.get(Company, user.company_id)
        return [c] if c else []

def create_company(name: str) -> Company:
    with db() as s:
        c = Company(name=name.strip())
        s.add(c)
        s.commit()
        s.refresh(c)
        return c

def list_facilities(company_id: int):
    with db() as s:
        return s.execute(select(Facility).where(Facility.company_id == company_id).order_by(Facility.name)).scalars().all()

def create_facility(company_id: int, name: str, country: str, sector: str):
    with db() as s:
        f = Facility(company_id=company_id, name=name.strip(), country=country.strip(), sector=sector.strip())
        s.add(f)
        s.commit()
        s.refresh(f)
        return f

def list_projects(company_id: int):
    with db() as s:
        return s.execute(select(Project).where(Project.company_id == company_id).order_by(Project.created_at.desc())).scalars().all()

def create_project(company_id: int, facility_id: int | None, name: str, year: int):
    with db() as s:
        p = Project(company_id=company_id, facility_id=facility_id, name=name.strip(), year=year)
        s.add(p)
        s.commit()
        s.refresh(p)
        return p
