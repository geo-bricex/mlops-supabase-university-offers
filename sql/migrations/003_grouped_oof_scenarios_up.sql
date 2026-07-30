-- Add grouped validation, OOF provenance, scenario, rule, and LLM traceability.
BEGIN;

ALTER TABLE audit.inconsistencies ADD COLUMN IF NOT EXISTS rule_id TEXT;
ALTER TABLE audit.inconsistencies ADD COLUMN IF NOT EXISTS severity TEXT;
ALTER TABLE audit.inconsistencies
    ADD COLUMN IF NOT EXISTS contributes_to_label BOOLEAN DEFAULT TRUE;
ALTER TABLE audit.inconsistencies ADD COLUMN IF NOT EXISTS rule_version TEXT;

CREATE TABLE IF NOT EXISTS audit.rule_catalog (
    rule_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    dimension TEXT NOT NULL,
    required_columns JSONB NOT NULL,
    condition TEXT NOT NULL,
    issue_type TEXT NOT NULL UNIQUE,
    severity TEXT NOT NULL,
    contributes_to_label BOOLEAN NOT NULL,
    version TEXT NOT NULL,
    description TEXT NOT NULL,
    event_granularity TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit.rule_run_counts (
    rule_run_count_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES audit.data_quality_runs(run_id)
        ON DELETE CASCADE,
    rule_id TEXT NOT NULL REFERENCES audit.rule_catalog(rule_id),
    event_count INT NOT NULL,
    affected_row_count INT NOT NULL,
    affected_group_count INT NOT NULL,
    label_positive_row_count INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (run_id, rule_id)
);

ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS oof_metrics JSONB;
ALTER TABLE mlops.training_runs
    ADD COLUMN IF NOT EXISTS operational_metrics JSONB;
ALTER TABLE mlops.training_runs
    ADD COLUMN IF NOT EXISTS operational_threshold NUMERIC;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS threshold_policy TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS primary_scenario TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS scenario_results JSONB;

ALTER TABLE mlops.predictions
    ADD COLUMN IF NOT EXISTS prediction_origin TEXT;
ALTER TABLE mlops.predictions ADD COLUMN IF NOT EXISTS scenario TEXT;
ALTER TABLE mlops.predictions ADD COLUMN IF NOT EXISTS fold_id INT;

ALTER TABLE mlops.model_candidates ADD COLUMN IF NOT EXISTS oof_metrics JSONB;
ALTER TABLE mlops.model_candidates
    ADD COLUMN IF NOT EXISTS operational_test_metrics JSONB;
ALTER TABLE mlops.model_candidates
    ADD COLUMN IF NOT EXISTS scenario TEXT NOT NULL DEFAULT 'leakage_controlled';

CREATE TABLE IF NOT EXISTS mlops.scenario_evaluations (
    scenario_evaluation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES mlops.training_runs(run_id)
        ON DELETE CASCADE,
    scenario TEXT NOT NULL,
    scenario_role TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    model_status TEXT NOT NULL,
    categorical_encoding TEXT NOT NULL,
    included_features JSONB NOT NULL,
    excluded_features JSONB NOT NULL,
    best_params JSONB NOT NULL,
    cv_mean NUMERIC NOT NULL,
    cv_std NUMERIC NOT NULL,
    cv_f1_mean NUMERIC,
    test_metrics_at_0_5 JSONB,
    operational_test_metrics JSONB,
    duration_seconds NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (run_id, scenario, algorithm)
);

CREATE TABLE IF NOT EXISTS mlops.llm_interpretation_runs (
    interpretation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES mlops.training_runs(run_id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    model_name TEXT NOT NULL,
    model_digest TEXT,
    model_details JSONB,
    configuration JSONB NOT NULL,
    prompt_sha256 TEXT NOT NULL,
    prompt TEXT NOT NULL,
    structured_input JSONB NOT NULL,
    response TEXT,
    latency_seconds NUMERIC,
    status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    error TEXT
);

CREATE OR REPLACE VIEW mlops.v_latest_quality_risk_predictions AS
SELECT
    p.prediction_id,
    p.run_id,
    p.file_id,
    p.row_num,
    p.natural_key,
    p.risk_label,
    p.risk_probability,
    p.actual_label,
    p.threshold,
    p.prediction_origin,
    p.scenario,
    p.fold_id,
    p.predicted_at,
    t.model_name,
    t.model_version,
    s.nombre_ies,
    s.nombre_carrera,
    s.tipo_ies,
    s.modalidad,
    s.provincia,
    s.canton,
    s.estado
FROM mlops.predictions p
JOIN mlops.training_runs t ON p.run_id = t.run_id
LEFT JOIN raw_ingest.stg_oferta s
    ON p.file_id = s.file_id
   AND p.row_num = s.row_num
WHERE t.is_active = TRUE;

CREATE OR REPLACE FUNCTION mlops.rpc_latest_quality_risks(
    limit_count INT DEFAULT 50
)
RETURNS TABLE (
    file_id UUID,
    row_num INT,
    natural_key TEXT,
    risk_probability NUMERIC,
    risk_label BOOLEAN,
    actual_label BOOLEAN,
    model_version TEXT,
    nombre_ies TEXT,
    nombre_carrera TEXT,
    provincia TEXT,
    canton TEXT,
    estado TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = mlops, raw_ingest, public
AS $$
    SELECT
        p.file_id, p.row_num, p.natural_key, p.risk_probability,
        p.risk_label, p.actual_label, t.model_version, s.nombre_ies,
        s.nombre_carrera, s.provincia, s.canton, s.estado
    FROM mlops.predictions p
    JOIN mlops.training_runs t ON p.run_id = t.run_id
    LEFT JOIN raw_ingest.stg_oferta s
        ON p.file_id = s.file_id AND p.row_num = s.row_num
    WHERE t.is_active = TRUE
      AND p.prediction_origin = 'production_inference'
    ORDER BY p.risk_probability DESC, p.predicted_at DESC
    LIMIT limit_count;
$$;

ALTER TABLE audit.rule_catalog ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit.rule_run_counts ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.scenario_evaluations ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.llm_interpretation_runs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Enable all for anon/service_role" ON audit.rule_catalog;
CREATE POLICY "Enable all for anon/service_role"
    ON audit.rule_catalog FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON audit.rule_run_counts;
CREATE POLICY "Enable all for anon/service_role"
    ON audit.rule_run_counts FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role"
    ON mlops.scenario_evaluations;
CREATE POLICY "Enable all for anon/service_role"
    ON mlops.scenario_evaluations FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role"
    ON mlops.llm_interpretation_runs;
CREATE POLICY "Enable all for anon/service_role"
    ON mlops.llm_interpretation_runs FOR ALL USING (true) WITH CHECK (true);

GRANT USAGE ON SCHEMA audit TO anon, authenticated, service_role;
GRANT SELECT ON audit.rule_catalog TO anon, authenticated, service_role;
GRANT SELECT ON audit.rule_run_counts TO anon, authenticated, service_role;
GRANT SELECT ON mlops.scenario_evaluations
    TO anon, authenticated, service_role;
GRANT SELECT ON mlops.llm_interpretation_runs
    TO anon, authenticated, service_role;

COMMIT;
