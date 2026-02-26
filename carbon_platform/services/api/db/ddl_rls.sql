CREATE SCHEMA IF NOT EXISTS app;

CREATE OR REPLACE FUNCTION app.current_tenant_id()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
  SELECT nullif(current_setting('app.tenant_id', true), '')::uuid;
$$;

CREATE OR REPLACE FUNCTION app.current_user_id()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
  SELECT nullif(current_setting('app.user_id', true), '')::uuid;
$$;

CREATE OR REPLACE FUNCTION app.current_roles()
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT coalesce(current_setting('app.roles', true), '');
$$;

-- admin bypass helper (roles string contains 'admin')
CREATE OR REPLACE FUNCTION app.is_admin()
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  SELECT position('admin' in app.current_roles()) > 0;
$$;

-- Facility scope check (security definer: policies rely on this)
CREATE OR REPLACE FUNCTION app.user_has_facility(_facility_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT
    app.is_admin()
    OR EXISTS (
      SELECT 1
      FROM user_facility_scope ufs
      WHERE ufs.tenant_id = app.current_tenant_id()
        AND ufs.user_id = app.current_user_id()
        AND ufs.facility_id = _facility_id
        AND ufs.is_active = true
    );
$$;

-- Activity record scope (join-based)
CREATE OR REPLACE FUNCTION app.user_has_activity(_activity_record_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT
    app.is_admin()
    OR EXISTS (
      SELECT 1
      FROM activity_record ar
      JOIN user_facility_scope ufs
        ON ufs.tenant_id = ar.tenant_id
       AND ufs.facility_id = ar.facility_id
       AND ufs.user_id = app.current_user_id()
       AND ufs.is_active = true
      WHERE ar.tenant_id = app.current_tenant_id()
        AND ar.id = _activity_record_id
    );
$$;

-- Verification case scope (facility-based)
CREATE OR REPLACE FUNCTION app.user_has_verification_case(_case_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT
    app.is_admin()
    OR EXISTS (
      SELECT 1
      FROM verification_case vc
      WHERE vc.tenant_id = app.current_tenant_id()
        AND vc.id = _case_id
        AND app.user_has_facility(vc.facility_id)
    );
$$;

-- Scenario scope (facility-based)
CREATE OR REPLACE FUNCTION app.user_has_scenario(_scenario_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT
    app.is_admin()
    OR EXISTS (
      SELECT 1
      FROM scenario s
      WHERE s.tenant_id = app.current_tenant_id()
        AND s.id = _scenario_id
        AND app.user_has_facility(s.facility_id)
    );
$$;

DO $$
DECLARE
  t text;
BEGIN
  -- tenant isolation: all tables
  FOREACH t IN ARRAY ARRAY[
    'app_user','user_role','role','tenant',
    'facility','user_facility_scope',

    'product','material',

    'methodology','monitoring_plan','monitoring_method','metering_asset','qa_qc_control',

    'factor_source','emission_factor','factor_approval',

    'activity_record','fuel_activity','electricity_activity','process_activity',

    'production_record','material_input','export_record',

    'document','calculation_run','cbam_report','cbam_report_line','compliance_check',

    'evidence_pack','evidence_item','snapshot','audit_log','job',

    'verification_case','verification_finding','capa_action',

    'scenario','scenario_assumption','scenario_run'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I;', t);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (tenant_id = app.current_tenant_id());', t);
  END LOOP;
END $$;

-- Facility scope policies (additional USING restrictions)

-- Facility itself
DROP POLICY IF EXISTS facility_scope ON facility;
CREATE POLICY facility_scope ON facility
USING (app.user_has_facility(id));

-- Product: has facility_id
DROP POLICY IF EXISTS product_scope ON product;
CREATE POLICY product_scope ON product
USING (app.user_has_facility(facility_id));

-- ActivityRecord: has facility_id
DROP POLICY IF EXISTS activity_scope ON activity_record;
CREATE POLICY activity_scope ON activity_record
USING (app.user_has_facility(facility_id));

-- Fuel/Electricity/Process: join via activity_record_id
DROP POLICY IF EXISTS fuel_scope ON fuel_activity;
CREATE POLICY fuel_scope ON fuel_activity
USING (app.user_has_activity(activity_record_id));

DROP POLICY IF EXISTS electricity_scope ON electricity_activity;
CREATE POLICY electricity_scope ON electricity_activity
USING (app.user_has_activity(activity_record_id));

DROP POLICY IF EXISTS process_scope ON process_activity;
CREATE POLICY process_scope ON process_activity
USING (app.user_has_activity(activity_record_id));

-- ProductionRecord/MaterialInput: join via activity_record_id
DROP POLICY IF EXISTS prodrec_scope ON production_record;
CREATE POLICY prodrec_scope ON production_record
USING (app.user_has_activity(activity_record_id));

DROP POLICY IF EXISTS matinput_scope ON material_input;
CREATE POLICY matinput_scope ON material_input
USING (app.user_has_activity(activity_record_id));

-- ExportRecord: has facility_id
DROP POLICY IF EXISTS export_scope ON export_record;
CREATE POLICY export_scope ON export_record
USING (app.user_has_facility(facility_id));

-- Monitoring plan + metering asset
DROP POLICY IF EXISTS mp_scope ON monitoring_plan;
CREATE POLICY mp_scope ON monitoring_plan
USING (app.user_has_facility(facility_id));

DROP POLICY IF EXISTS meter_scope ON metering_asset;
CREATE POLICY meter_scope ON metering_asset
USING (app.user_has_facility(facility_id));

-- Verification: case facility-based; finding/capa by parent ids
DROP POLICY IF EXISTS ver_case_scope ON verification_case;
CREATE POLICY ver_case_scope ON verification_case
USING (app.user_has_facility(facility_id));

DROP POLICY IF EXISTS finding_scope ON verification_finding;
CREATE POLICY finding_scope ON verification_finding
USING (app.user_has_verification_case(case_id));

DROP POLICY IF EXISTS capa_scope ON capa_action;
CREATE POLICY capa_scope ON capa_action
USING (
  app.is_admin()
  OR EXISTS (
    SELECT 1
    FROM verification_finding vf
    WHERE vf.tenant_id = app.current_tenant_id()
      AND vf.id = capa_action.finding_id
      AND app.user_has_verification_case(vf.case_id)
  )
);

-- Scenario: facility-based; assumptions/runs by parent
DROP POLICY IF EXISTS scenario_scope ON scenario;
CREATE POLICY scenario_scope ON scenario
USING (app.user_has_facility(facility_id));

DROP POLICY IF EXISTS scen_assump_scope ON scenario_assumption;
CREATE POLICY scen_assump_scope ON scenario_assumption
USING (app.user_has_scenario(scenario_id));

DROP POLICY IF EXISTS scen_run_scope ON scenario_run;
CREATE POLICY scen_run_scope ON scenario_run
USING (app.user_has_scenario(scenario_id));

-- CalculationRun: facility-based
DROP POLICY IF EXISTS calc_scope ON calculation_run;
CREATE POLICY calc_scope ON calculation_run
USING (app.user_has_facility(facility_id));

-- CBAM report lines and checks: conservative (tenant only) + report lines facility_id if present
DROP POLICY IF EXISTS cbam_line_scope ON cbam_report_line;
CREATE POLICY cbam_line_scope ON cbam_report_line
USING (app.is_admin() OR (facility_id IS NULL) OR app.user_has_facility(facility_id));

DROP POLICY IF EXISTS check_scope ON compliance_check;
CREATE POLICY check_scope ON compliance_check
USING (true);
