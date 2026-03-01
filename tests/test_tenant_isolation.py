from src.db.session import db, init_db
from src.db.models import Company, Facility, Project, CalculationSnapshot, User
from src.services.tenant_guard import require_snapshot_access, AccessDenied


def test_tenant_isolation_guard():
    init_db()
    with db() as s:
        a = Company(name="A"); b=Company(name="B")
        s.add_all([a,b]); s.commit(); s.refresh(a); s.refresh(b)
        fa = Facility(company_id=a.id, name="FA"); fb=Facility(company_id=b.id, name="FB")
        s.add_all([fa,fb]); s.commit(); s.refresh(fa); s.refresh(fb)
        pa = Project(company_id=a.id, facility_id=fa.id, name="PA")
        pb = Project(company_id=b.id, facility_id=fb.id, name="PB")
        s.add_all([pa,pb]); s.commit(); s.refresh(pa); s.refresh(pb)
        snap = CalculationSnapshot(project_id=pa.id, input_hash="x", result_hash="y")
        s.add(snap); s.commit(); s.refresh(snap)

        ua = User(company_id=a.id, email="a@x.com", password_hash="x")
        ub = User(company_id=b.id, email="b@x.com", password_hash="x")
        s.add_all([ua,ub]); s.commit(); s.refresh(ua); s.refresh(ub)

    # same tenant ok
    require_snapshot_access(user=ua, project=pa, snapshot=snap)

    # other tenant denied
    denied = False
    try:
        require_snapshot_access(user=ub, project=pa, snapshot=snap)
    except AccessDenied:
        denied = True
    assert denied is True
