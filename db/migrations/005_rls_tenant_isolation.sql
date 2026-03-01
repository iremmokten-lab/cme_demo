
-- Enable RLS
ALTER TABLE datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY;

-- Policy: tenant isolation
CREATE POLICY tenant_isolation_datasets
ON datasets
USING (tenant_id = current_setting('app.tenant_id')::text);

CREATE POLICY tenant_isolation_snapshots
ON snapshots
USING (tenant_id = current_setting('app.tenant_id')::text);
