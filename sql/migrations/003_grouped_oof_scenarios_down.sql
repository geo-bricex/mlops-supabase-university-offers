-- Revert migration 003 while preserving the pre-existing Q1 registry.
BEGIN;

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
    ORDER BY p.risk_probability DESC, p.predicted_at DESC
    LIMIT limit_count;
$$;

DROP TABLE IF EXISTS mlops.llm_interpretation_runs;
DROP TABLE IF EXISTS mlops.scenario_evaluations;
DROP TABLE IF EXISTS audit.rule_run_counts;
DROP TABLE IF EXISTS audit.rule_catalog;

ALTER TABLE mlops.model_candidates DROP COLUMN IF EXISTS scenario;
ALTER TABLE mlops.model_candidates
    DROP COLUMN IF EXISTS operational_test_metrics;
ALTER TABLE mlops.model_candidates DROP COLUMN IF EXISTS oof_metrics;

ALTER TABLE mlops.predictions DROP COLUMN IF EXISTS fold_id;
ALTER TABLE mlops.predictions DROP COLUMN IF EXISTS scenario;
ALTER TABLE mlops.predictions DROP COLUMN IF EXISTS prediction_origin;

ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS scenario_results;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS primary_scenario;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS threshold_policy;
ALTER TABLE mlops.training_runs
    DROP COLUMN IF EXISTS operational_threshold;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS operational_metrics;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS oof_metrics;

ALTER TABLE audit.inconsistencies DROP COLUMN IF EXISTS rule_version;
ALTER TABLE audit.inconsistencies
    DROP COLUMN IF EXISTS contributes_to_label;
ALTER TABLE audit.inconsistencies DROP COLUMN IF EXISTS severity;
ALTER TABLE audit.inconsistencies DROP COLUMN IF EXISTS rule_id;

COMMIT;
