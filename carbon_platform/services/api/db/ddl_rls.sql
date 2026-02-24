CREATE SCHEMA IF NOT EXISTS app;

CREATE OR REPLACE FUNCTION app.current_tenant_id()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
  SELECT nullif(current_setting('app.tenant_id', true), '')::uuid;
$$;

CREATE OR REPLACE FUNCTION app.current_roles()
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT coalesce(current_setting('app.roles', true), '');
$$;

DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'app_user',
    'user_role',
    'facility',

    'product',
    'material',

    'methodology',
    'monitoring_plan',
    'monitoring_method',
    'metering_asset',
    'qa_qc_control',

    'factor_source',
    'emission_factor',
    'factor_approval',

    'activity_record',
    'fuel_activity',
    'electricity_activity',
    'process_activity',

    'production_record',
    'material_input',
    'export_record',

    'document',
    'calculation_run',
    'cbam_report',

    'evidence_pack',
    'evidence_item',
    'snapshot',
    'audit_log',
    'job'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I;', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING (tenant_id = app.current_tenant_id());',
      t
    );
  END LOOP;
END $$;
