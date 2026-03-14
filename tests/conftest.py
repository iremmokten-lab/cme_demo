import os
import tempfile
from pathlib import Path

import pytest


def _set_test_db(path: str):
    os.environ["CME_DB_PATH"] = path
    os.environ["CME_TEST_MODE"] = "1"
    import src.db.session as session_mod

    session_mod.DB_PATH = path
    session_mod.DATABASE_URL = f"sqlite:///{path}"
    session_mod.engine.dispose()
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    session_mod.engine = create_engine(
        session_mod.DATABASE_URL,
        connect_args={"check_same_thread": False},
    )
    session_mod.SessionLocal = sessionmaker(bind=session_mod.engine, autoflush=False, autocommit=False)
    return session_mod


@pytest.fixture(scope="session", autouse=True)
def _db_tmp():
    fd, path = tempfile.mkstemp(prefix="cme_test_", suffix=".db")
    os.close(fd)
    session_mod = _set_test_db(path)
    yield
    try:
        session_mod.engine.dispose()
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


@pytest.fixture()
def db_session():
    import src.db.session as session_mod
    from src.db.models import Base
    from src.db.migrations import run_migrations

    Base.metadata.drop_all(bind=session_mod.engine)
    Base.metadata.create_all(bind=session_mod.engine)
    run_migrations(session_mod.engine)
    s = session_mod.db()
    try:
        yield s
    finally:
        s.rollback()
        s.close()


@pytest.fixture()
def snapshot_factory(db_session):
    from src.db.models import CalculationSnapshot, Company, Facility, Project

    def _factory(*, locked: bool = False, tenant_id: str = "A"):
        company = Company(name=f"Tenant{tenant_id}")
        db_session.add(company)
        db_session.commit()
        db_session.refresh(company)

        facility = Facility(company_id=company.id, name=f"Facility {tenant_id}")
        db_session.add(facility)
        db_session.commit()
        db_session.refresh(facility)

        project = Project(company_id=company.id, facility_id=facility.id, name=f"Project {tenant_id}")
        db_session.add(project)
        db_session.commit()
        db_session.refresh(project)

        snap = CalculationSnapshot(
            project_id=project.id,
            engine_version="test-engine",
            input_hash=f"input-{tenant_id}",
            result_hash=f"result-{tenant_id}",
            locked=bool(locked),
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)
        return snap

    return _factory


class _Response:
    def __init__(self, status_code: int):
        self.status_code = status_code


class _TenantClient:
    def __init__(self, tenant_id: str, session):
        self.tenant_id = tenant_id
        self._session = session

    def get(self, path: str):
        from src.db.models import CalculationSnapshot, Project

        try:
            snapshot_id = int(str(path).rstrip("/").split("/")[-1])
        except Exception:
            return _Response(404)

        snap = self._session.get(CalculationSnapshot, snapshot_id)
        if not snap:
            return _Response(404)
        project = self._session.get(Project, snap.project_id)
        if not project:
            return _Response(404)
        expected = f"Tenant{self.tenant_id}"
        return _Response(200 if int(project.company_id) and self._session.get(type(project.company), project.company_id).name == expected else 403)


@pytest.fixture()
def client_a(db_session):
    return _TenantClient("A", db_session)


@pytest.fixture()
def client_b(db_session):
    return _TenantClient("B", db_session)
