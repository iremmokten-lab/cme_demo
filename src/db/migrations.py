from __future__ import annotations

from sqlalchemy import Engine, text


def _try(conn, sql: str):
    try:
        conn.execute(text(sql))
    except Exception:
        pass


def run_migrations(engine: Engine):
    """SQLite için best-effort şema stabilizasyonu.

    Streamlit Cloud'da Alembic yok. Bu fonksiyon:
      - yeni kolonları ekler (ALTER TABLE)
      - mevcut veriyi bozmaz
    """
    with engine.connect() as conn:
        # --- calculationsnapshots (audit-ready)
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN input_hash VARCHAR(64)")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN factor_set_id INTEGER")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN monitoring_plan_id INTEGER")

        # --- datasetuploads (legacy + cloud)
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN original_filename VARCHAR(255)")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN sha256 VARCHAR(64)")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN content_hash VARCHAR(64)")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN storage_uri VARCHAR(500)")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN content_bytes BLOB")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN content_b64 TEXT")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN meta_json TEXT")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN validated BOOLEAN")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN data_quality_score INTEGER")
        _try(conn, "ALTER TABLE datasetuploads ADD COLUMN data_quality_report_json TEXT")

        # --- evidencedocuments
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN original_filename VARCHAR(255)")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN category VARCHAR(80)")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN notes TEXT")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN sha256 VARCHAR(64)")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN content_hash VARCHAR(64)")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN storage_uri VARCHAR(500)")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN content_bytes BLOB")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN content_b64 TEXT")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN mime_type VARCHAR(120)")
        _try(conn, "ALTER TABLE evidencedocuments ADD COLUMN meta_json TEXT")

        # --- verificationcases
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN period_year INTEGER")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN verifier_org VARCHAR(200)")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN sampling_json TEXT")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN closed_at DATETIME")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN closed_by_user_id INTEGER")

        # --- verificationfindings
        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN evidence_ref VARCHAR(200)")
        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN resolved_at DATETIME")

        # --- alerts
        _try(conn, "ALTER TABLE alerts ADD COLUMN meta_json TEXT")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved BOOLEAN")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved_at DATETIME")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved_by_user_id INTEGER")
