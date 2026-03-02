from __future__ import annotations

from sqlalchemy import text


def run_global_ready_migrations(engine) -> None:
    # Best-effort indexes for scale. SQLite supports CREATE INDEX IF NOT EXISTS.
    stmts = [
        "CREATE INDEX IF NOT EXISTS ix_access_audit_company_time ON access_audit_logs(company_id, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_reg_spec_active ON regulation_spec_versions(spec_name, is_active)",
        "CREATE INDEX IF NOT EXISTS ix_erp_conn_company_active ON erp_connections(company_id, is_active)",
        "CREATE INDEX IF NOT EXISTS ix_mp_versions_project_year_status ON monitoring_plan_versions(project_id, period_year, status)",
        "CREATE INDEX IF NOT EXISTS ix_cbam_sub_project_period ON cbam_quarterly_submissions(project_id, period_year, period_quarter)",
    ]
    with engine.begin() as conn:
        for s in stmts:
            try:
                conn.execute(text(s))
            except Exception:
                continue
