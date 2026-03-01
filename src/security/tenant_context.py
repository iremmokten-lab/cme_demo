
from contextlib import contextmanager
from sqlalchemy import text

@contextmanager
def tenant_context(session, tenant_id):
    session.execute(text("SET app.tenant_id = :tenant"), {"tenant": tenant_id})
    try:
        yield
    finally:
        session.execute(text("RESET app.tenant_id"))
