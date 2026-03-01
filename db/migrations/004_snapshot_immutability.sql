
-- Prevent updates or deletes on locked snapshots
CREATE OR REPLACE FUNCTION prevent_locked_snapshot_change()
RETURNS trigger AS $$
BEGIN
    IF OLD.locked = true THEN
        RAISE EXCEPTION 'Snapshot is locked and cannot be modified';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_snapshot_update ON snapshots;

CREATE TRIGGER trg_snapshot_update
BEFORE UPDATE OR DELETE ON snapshots
FOR EACH ROW
EXECUTE FUNCTION prevent_locked_snapshot_change();
