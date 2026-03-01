from __future__ import annotations

from sqlalchemy import Engine, text


def _try(conn, sql: str):
    try:
        conn.execute(text(sql))
    except Exception:
        # SQLite best-effort; ignore if already exists / unsupported
        pass


def run_migrations(engine: Engine):
    """SQLite için best-effort şema stabilizasyonu + audit-grade immutability.

    Streamlit Cloud'da Alembic yok. Bu fonksiyon:
      - yeni kolonları ekler (ALTER TABLE)
      - yeni tabloları oluşturur (CREATE TABLE IF NOT EXISTS)
      - locked snapshot'lar için UPDATE/DELETE engelleyen trigger'ları kurar

    Not:
      - PostgreSQL prod kullanımında RLS ve trigger'lar ayrı DDL ile yönetilebilir.
      - Bu repo Streamlit Cloud uyumu için SQLite üzerinde minimum DB-level koruma sağlar.
    """
    with engine.connect() as conn:
        # ----------------------------
        # calculationsnapshots (audit-grade)
        # ----------------------------
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN dataset_hashes_json TEXT DEFAULT '{}'")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN factor_set_hash VARCHAR(64) DEFAULT ''")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN methodology_hash VARCHAR(64) DEFAULT ''")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN base_snapshot_id INTEGER")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN scenario_meta_json TEXT DEFAULT '{}'")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN price_evidence_json TEXT DEFAULT '[]'")

        # legacy compatibility
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN input_hash VARCHAR(64)")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN factor_set_id INTEGER")
        _try(conn, "ALTER TABLE calculationsnapshots ADD COLUMN monitoring_plan_id INTEGER")

        # ----------------------------
        # datasetuploads (cloud)
        # ----------------------------
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

        # ----------------------------
        # evidencedocuments
        # ----------------------------
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

        # ----------------------------
        # emissionfactors governance
        # ----------------------------
        _try(conn, "ALTER TABLE emissionfactors ADD COLUMN methodology TEXT DEFAULT ''")
        _try(conn, "ALTER TABLE emissionfactors ADD COLUMN valid_from VARCHAR(30) DEFAULT ''")
        _try(conn, "ALTER TABLE emissionfactors ADD COLUMN valid_to VARCHAR(30) DEFAULT ''")
        _try(conn, "ALTER TABLE emissionfactors ADD COLUMN locked BOOLEAN DEFAULT 0")
        _try(conn, "ALTER TABLE emissionfactors ADD COLUMN factor_hash VARCHAR(64) DEFAULT ''")

        # ----------------------------
        # verification workflow extensions (optional future)
        # ----------------------------
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN period_year INTEGER")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN verifier_org VARCHAR(200)")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN sampling_json TEXT")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN closed_at DATETIME")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN closed_by_user_id INTEGER")

        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN evidence_ref VARCHAR(200)")
        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN corrective_action TEXT")
        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN action_due_date VARCHAR(30)")
        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN status VARCHAR(50)")
        _try(conn, "ALTER TABLE verificationfindings ADD COLUMN resolved_at DATETIME")

        # ----------------------------
        # Link tables for immutability & used_in_snapshots
        # ----------------------------
        _try(
            conn,
            """
            CREATE TABLE IF NOT EXISTS snapshot_dataset_links (
                id INTEGER PRIMARY KEY,
                snapshot_id INTEGER NOT NULL,
                datasetupload_id INTEGER NOT NULL,
                dataset_type VARCHAR(50) DEFAULT '',
                sha256 VARCHAR(64) DEFAULT '',
                storage_uri VARCHAR(500) DEFAULT '',
                created_at DATETIME,
                CONSTRAINT uq_snapshot_dataset_link UNIQUE (snapshot_id, datasetupload_id)
            )
            """,
        )

        _try(
            conn,
            """
            CREATE TABLE IF NOT EXISTS snapshot_factor_links (
                id INTEGER PRIMARY KEY,
                snapshot_id INTEGER NOT NULL,
                factor_id INTEGER NOT NULL,
                factor_type VARCHAR(120) DEFAULT '',
                region VARCHAR(20) DEFAULT 'TR',
                year INTEGER,
                version VARCHAR(50) DEFAULT 'v1',
                factor_hash VARCHAR(64) DEFAULT '',
                created_at DATETIME,
                CONSTRAINT uq_snapshot_factor_link UNIQUE (snapshot_id, factor_id)
            )
            """,
        )

        # ----------------------------
        # DB-level immutability triggers (SQLite)
        # ----------------------------
        # Prevent UPDATE/DELETE on locked snapshots
        _try(
            conn,
            """
            CREATE TRIGGER IF NOT EXISTS trg_snapshot_no_update_when_locked
            BEFORE UPDATE ON calculationsnapshots
            FOR EACH ROW
            WHEN OLD.locked = 1
            BEGIN
                SELECT RAISE(ABORT, 'Kilitli snapshot güncellenemez.');
            END;
            """,
        )
        _try(
            conn,
            """
            CREATE TRIGGER IF NOT EXISTS trg_snapshot_no_delete_when_locked
            BEFORE DELETE ON calculationsnapshots
            FOR EACH ROW
            WHEN OLD.locked = 1
            BEGIN
                SELECT RAISE(ABORT, 'Kilitli snapshot silinemez.');
            END;
            """,
        )

        # Prevent update/delete of datasetuploads if linked to a locked snapshot
        _try(
            conn,
            """
            CREATE TRIGGER IF NOT EXISTS trg_datasetupload_no_update_if_locked_snapshot
            BEFORE UPDATE ON datasetuploads
            FOR EACH ROW
            WHEN EXISTS (
                SELECT 1
                FROM snapshot_dataset_links l
                JOIN calculationsnapshots s ON s.id = l.snapshot_id
                WHERE l.datasetupload_id = OLD.id AND s.locked = 1
            )
            BEGIN
                SELECT RAISE(ABORT, 'Bu dataset kilitli bir snapshot tarafından kullanılıyor. Güncellenemez.');
            END;
            """,
        )
        _try(
            conn,
            """
            CREATE TRIGGER IF NOT EXISTS trg_datasetupload_no_delete_if_locked_snapshot
            BEFORE DELETE ON datasetuploads
            FOR EACH ROW
            WHEN EXISTS (
                SELECT 1
                FROM snapshot_dataset_links l
                JOIN calculationsnapshots s ON s.id = l.snapshot_id
                WHERE l.datasetupload_id = OLD.id AND s.locked = 1
            )
            BEGIN
                SELECT RAISE(ABORT, 'Bu dataset kilitli bir snapshot tarafından kullanılıyor. Silinemez.');
            END;
            """,
        )
