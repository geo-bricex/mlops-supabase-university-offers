-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw_ingest;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS mlops;

-- 1) raw_ingest.files
CREATE TABLE IF NOT EXISTS raw_ingest.files (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name TEXT NOT NULL,
    checksum_sha256 TEXT UNIQUE NOT NULL,
    rows_loaded INT,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status TEXT CHECK (status IN ('success','failed','skipped','running')),
    notes TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC,
    file_size_bytes BIGINT,
    ingest_new INT,
    ingest_updated INT,
    ingest_unchanged INT,
    skipped_missing_dims INT,
    storage_status TEXT,
    storage_paths JSONB,
    process_metrics JSONB
);

-- 2) raw_ingest.stg_oferta
CREATE TABLE IF NOT EXISTS raw_ingest.stg_oferta (
    stg_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID REFERENCES raw_ingest.files(file_id),
    row_num INT,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Original columns
    nombre_ies TEXT,
    tipo_ies TEXT,
    tipo_financiamiento TEXT,
    nombre_carrera TEXT,
    campo_amplio TEXT,
    nivel_formacion TEXT,
    modalidad TEXT,
    provincia TEXT,
    canton TEXT,
    estado TEXT,
    -- Normalized & System
    normalized_fields JSONB,
    natural_key TEXT,
    row_hash TEXT
);

-- 3) Dimensions
CREATE TABLE IF NOT EXISTS core.dim_ies (
    ies_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre_original TEXT,
    nombre_norm TEXT UNIQUE,
    tipo_ies TEXT,
    tipo_financiamiento TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.dim_territory (
    territory_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provincia_original TEXT,
    canton_original TEXT,
    provincia_norm TEXT,
    canton_norm TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(provincia_norm, canton_norm)
);

CREATE TABLE IF NOT EXISTS core.dim_program (
    program_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    carrera_original TEXT,
    carrera_norm TEXT,
    campo_amplio TEXT,
    nivel_formacion TEXT,
    modalidad TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(carrera_norm, campo_amplio, nivel_formacion, modalidad)
);

-- 4) Facts with change tracking (SCD Type 2)
CREATE TABLE IF NOT EXISTS core.fact_offer (
    offer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ies_id UUID REFERENCES core.dim_ies(ies_id),
    territory_id UUID REFERENCES core.dim_territory(territory_id),
    program_id UUID REFERENCES core.dim_program(program_id),
    estado_original TEXT,
    estado_norm TEXT,
    natural_key TEXT,
    row_hash TEXT,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_file_id UUID REFERENCES raw_ingest.files(file_id),
    is_current BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_offer_current_key 
    ON core.fact_offer(natural_key) 
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_fact_offer_norm_fields 
    ON core.fact_offer(estado_norm);

-- Supporting indexes for filtering
CREATE INDEX IF NOT EXISTS idx_dim_territory_provincia 
    ON core.dim_territory(provincia_norm);
CREATE INDEX IF NOT EXISTS idx_dim_territory_canton 
    ON core.dim_territory(canton_norm);
CREATE INDEX IF NOT EXISTS idx_dim_program_campo 
    ON core.dim_program(campo_amplio);
CREATE INDEX IF NOT EXISTS idx_dim_program_nivel 
    ON core.dim_program(nivel_formacion);
CREATE INDEX IF NOT EXISTS idx_dim_program_modalidad 
    ON core.dim_program(modalidad);
CREATE INDEX IF NOT EXISTS idx_dim_ies_tipo 
    ON core.dim_ies(tipo_ies);
CREATE INDEX IF NOT EXISTS idx_dim_ies_financiamiento 
    ON core.dim_ies(tipo_financiamiento);

-- Backfill columns for existing environments
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS started_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS ingest_new INT;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS ingest_updated INT;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS ingest_unchanged INT;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS skipped_missing_dims INT;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS storage_status TEXT;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS storage_paths JSONB;
ALTER TABLE raw_ingest.files ADD COLUMN IF NOT EXISTS process_metrics JSONB;

-- 5) Audit
CREATE TABLE IF NOT EXISTS audit.data_quality_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID REFERENCES raw_ingest.files(file_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metrics JSONB
);

CREATE TABLE IF NOT EXISTS audit.inconsistencies (
    issue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES audit.data_quality_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    issue_type TEXT,
    natural_key TEXT,
    detail JSONB
);

-- 6) Analytics views and RPC endpoints
CREATE OR REPLACE VIEW core.v_top_provinces AS
SELECT
    t.provincia_norm,
    COUNT(*)::BIGINT AS offers
FROM core.fact_offer f
JOIN core.dim_territory t ON f.territory_id = t.territory_id
WHERE f.is_current = TRUE
GROUP BY t.provincia_norm;

CREATE OR REPLACE VIEW core.v_ingestion_series AS
SELECT
    DATE_TRUNC('day', ingested_at) AS period,
    COUNT(*)::BIGINT AS files,
    COALESCE(SUM(rows_loaded), 0)::BIGINT AS rows_loaded,
    COALESCE(SUM(ingest_new), 0)::BIGINT AS ingest_new,
    COALESCE(SUM(ingest_updated), 0)::BIGINT AS ingest_updated,
    COALESCE(SUM(ingest_unchanged), 0)::BIGINT AS ingest_unchanged,
    COALESCE(SUM(skipped_missing_dims), 0)::BIGINT AS skipped_missing_dims
FROM raw_ingest.files
WHERE status = 'success'
GROUP BY DATE_TRUNC('day', ingested_at)
ORDER BY period;

CREATE OR REPLACE FUNCTION core.rpc_top_provinces(limit_count INT DEFAULT 10)
RETURNS TABLE (provincia_norm TEXT, offers BIGINT)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = core, raw_ingest, public
AS $$
    SELECT
        t.provincia_norm,
        COUNT(*)::BIGINT AS offers
    FROM core.fact_offer f
    JOIN core.dim_territory t ON f.territory_id = t.territory_id
    WHERE f.is_current = TRUE
    GROUP BY t.provincia_norm
    ORDER BY offers DESC
    LIMIT limit_count;
$$;

CREATE OR REPLACE FUNCTION core.rpc_ingestion_series(bucket TEXT DEFAULT 'day')
RETURNS TABLE (
    period TIMESTAMP WITH TIME ZONE,
    files BIGINT,
    rows_loaded BIGINT,
    ingest_new BIGINT,
    ingest_updated BIGINT,
    ingest_unchanged BIGINT,
    skipped_missing_dims BIGINT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = core, raw_ingest, public
AS $$
    SELECT
        CASE
            WHEN bucket IN ('hour', 'day', 'week', 'month') THEN DATE_TRUNC(bucket, ingested_at)
            ELSE DATE_TRUNC('day', ingested_at)
        END AS period,
        COUNT(*)::BIGINT AS files,
        COALESCE(SUM(rows_loaded), 0)::BIGINT AS rows_loaded,
        COALESCE(SUM(ingest_new), 0)::BIGINT AS ingest_new,
        COALESCE(SUM(ingest_updated), 0)::BIGINT AS ingest_updated,
        COALESCE(SUM(ingest_unchanged), 0)::BIGINT AS ingest_unchanged,
        COALESCE(SUM(skipped_missing_dims), 0)::BIGINT AS skipped_missing_dims
    FROM raw_ingest.files
    WHERE status = 'success'
    GROUP BY
        CASE
            WHEN bucket IN ('hour', 'day', 'week', 'month') THEN DATE_TRUNC(bucket, ingested_at)
            ELSE DATE_TRUNC('day', ingested_at)
        END
    ORDER BY period;
$$;

-- 6) Operational monitoring
CREATE TABLE IF NOT EXISTS ops.service_health (
    check_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    service_name TEXT NOT NULL,
    endpoint TEXT,
    status TEXT CHECK (status IN ('up','down')),
    status_code INT,
    latency_ms NUMERIC,
    detail JSONB
);

CREATE INDEX IF NOT EXISTS idx_service_health_created_at
    ON ops.service_health(created_at);
CREATE INDEX IF NOT EXISTS idx_service_health_service_name
    ON ops.service_health(service_name);

CREATE TABLE IF NOT EXISTS ops.etl_step_metrics (
    step_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID REFERENCES raw_ingest.files(file_id),
    step_name TEXT NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC,
    row_count INT,
    detail JSONB
);

CREATE INDEX IF NOT EXISTS idx_etl_step_metrics_file_id
    ON ops.etl_step_metrics(file_id);
CREATE INDEX IF NOT EXISTS idx_etl_step_metrics_step_name
    ON ops.etl_step_metrics(step_name);
CREATE INDEX IF NOT EXISTS idx_etl_step_metrics_started_at
    ON ops.etl_step_metrics(started_at);

-- 7) MLOps registry, predictions, and monitoring
CREATE TABLE IF NOT EXISTS mlops.training_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID REFERENCES raw_ingest.files(file_id),
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL UNIQUE,
    task_name TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    target_name TEXT NOT NULL,
    status TEXT CHECK (status IN ('success', 'failed', 'skipped', 'running')),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC,
    train_rows INT,
    test_rows INT,
    positive_rows INT,
    positive_rate NUMERIC,
    selected_metric TEXT,
    search_method TEXT,
    optimization_metric TEXT,
    cv_folds INT,
    search_iterations INT,
    search_spaces JSONB,
    best_params JSONB,
    best_score NUMERIC,
    train_metrics JSONB,
    cv_metrics JSONB,
    candidate_metrics JSONB,
    overfit_gap NUMERIC,
    metrics JSONB,
    artifact_path TEXT,
    artifact_sha256 TEXT,
    storage_status TEXT,
    storage_paths JSONB,
    feature_schema JSONB,
    dataset_sha256 TEXT,
    dataset_rows INT,
    class_distribution JSONB,
    random_state INT,
    python_version TEXT,
    sklearn_version TEXT,
    git_commit TEXT,
    run_metadata JSONB,
    model_status TEXT,
    notes TEXT,
    is_active BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_mlops_training_runs_started_at
    ON mlops.training_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_mlops_training_runs_model_name
    ON mlops.training_runs(model_name);
CREATE INDEX IF NOT EXISTS idx_mlops_training_runs_is_active
    ON mlops.training_runs(is_active);

CREATE TABLE IF NOT EXISTS mlops.feature_importance (
    importance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES mlops.training_runs(run_id),
    feature_name TEXT NOT NULL,
    importance NUMERIC NOT NULL,
    direction TEXT,
    rank INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mlops_feature_importance_run_id
    ON mlops.feature_importance(run_id);

CREATE TABLE IF NOT EXISTS mlops.predictions (
    prediction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES mlops.training_runs(run_id),
    file_id UUID REFERENCES raw_ingest.files(file_id),
    row_num INT,
    natural_key TEXT,
    risk_label BOOLEAN,
    risk_probability NUMERIC,
    actual_label BOOLEAN,
    threshold NUMERIC,
    predicted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    detail JSONB
);

CREATE INDEX IF NOT EXISTS idx_mlops_predictions_run_id
    ON mlops.predictions(run_id);
CREATE INDEX IF NOT EXISTS idx_mlops_predictions_file_id
    ON mlops.predictions(file_id);
CREATE INDEX IF NOT EXISTS idx_mlops_predictions_probability
    ON mlops.predictions(risk_probability DESC);

CREATE TABLE IF NOT EXISTS mlops.monitoring_runs (
    monitor_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES mlops.training_runs(run_id),
    file_id UUID REFERENCES raw_ingest.files(file_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metrics JSONB
);

CREATE INDEX IF NOT EXISTS idx_mlops_monitoring_runs_run_id
    ON mlops.monitoring_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_mlops_monitoring_runs_created_at
    ON mlops.monitoring_runs(created_at DESC);

ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS selected_metric TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS train_metrics JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS cv_metrics JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS candidate_metrics JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS overfit_gap NUMERIC;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS storage_status TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS storage_paths JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS search_method TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS optimization_metric TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS cv_folds INT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS search_iterations INT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS search_spaces JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS best_params JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS best_score NUMERIC;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS dataset_sha256 TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS dataset_rows INT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS class_distribution JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS random_state INT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS python_version TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS sklearn_version TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS git_commit TEXT;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS run_metadata JSONB;
ALTER TABLE mlops.training_runs ADD COLUMN IF NOT EXISTS model_status TEXT;

CREATE TABLE IF NOT EXISTS mlops.model_candidates (
    candidate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES mlops.training_runs(run_id) ON DELETE CASCADE,
    algorithm TEXT NOT NULL,
    model_status TEXT NOT NULL
        CHECK (model_status IN ('candidate', 'selected', 'rejected')),
    search_method TEXT NOT NULL,
    optimization_metric TEXT NOT NULL,
    cv_folds INT NOT NULL,
    search_iterations INT NOT NULL,
    search_space JSONB NOT NULL,
    best_params JSONB NOT NULL,
    best_score NUMERIC NOT NULL,
    cv_mean NUMERIC NOT NULL,
    cv_std NUMERIC NOT NULL,
    cv_f1_mean NUMERIC,
    cv_f1_std NUMERIC,
    test_metrics JSONB,
    confusion_matrix JSONB,
    artifact_path TEXT,
    cv_results_path TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (run_id, algorithm)
);

CREATE INDEX IF NOT EXISTS idx_mlops_model_candidates_run_id
    ON mlops.model_candidates(run_id);
CREATE INDEX IF NOT EXISTS idx_mlops_model_candidates_status
    ON mlops.model_candidates(model_status);

CREATE OR REPLACE VIEW mlops.v_latest_training_run AS
SELECT *
FROM mlops.training_runs
WHERE is_active = TRUE
ORDER BY started_at DESC;

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

CREATE OR REPLACE FUNCTION mlops.rpc_latest_quality_risks(limit_count INT DEFAULT 50)
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
        p.file_id,
        p.row_num,
        p.natural_key,
        p.risk_probability,
        p.risk_label,
        p.actual_label,
        t.model_version,
        s.nombre_ies,
        s.nombre_carrera,
        s.provincia,
        s.canton,
        s.estado
    FROM mlops.predictions p
    JOIN mlops.training_runs t ON p.run_id = t.run_id
    LEFT JOIN raw_ingest.stg_oferta s
        ON p.file_id = s.file_id
       AND p.row_num = s.row_num
    WHERE t.is_active = TRUE
    ORDER BY p.risk_probability DESC, p.predicted_at DESC
    LIMIT limit_count;
$$;

-- Enable RLS (Optional but recommended for Supabase)
ALTER TABLE raw_ingest.files ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_ingest.stg_oferta ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.dim_ies ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.dim_territory ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.dim_program ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.fact_offer ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit.data_quality_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit.inconsistencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.service_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.etl_step_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.training_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.feature_importance ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.monitoring_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlops.model_candidates ENABLE ROW LEVEL SECURITY;

-- Allow public access for local dev (simply for ease of use in this context)
-- In prod, you would configure specific policies.
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON raw_ingest.files;
CREATE POLICY "Enable all for anon/service_role" ON raw_ingest.files FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON raw_ingest.stg_oferta;
CREATE POLICY "Enable all for anon/service_role" ON raw_ingest.stg_oferta FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON core.dim_ies;
CREATE POLICY "Enable all for anon/service_role" ON core.dim_ies FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON core.dim_territory;
CREATE POLICY "Enable all for anon/service_role" ON core.dim_territory FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON core.dim_program;
CREATE POLICY "Enable all for anon/service_role" ON core.dim_program FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON core.fact_offer;
CREATE POLICY "Enable all for anon/service_role" ON core.fact_offer FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON audit.data_quality_runs;
CREATE POLICY "Enable all for anon/service_role" ON audit.data_quality_runs FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON audit.inconsistencies;
CREATE POLICY "Enable all for anon/service_role" ON audit.inconsistencies FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON ops.service_health;
CREATE POLICY "Enable all for anon/service_role" ON ops.service_health FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON ops.etl_step_metrics;
CREATE POLICY "Enable all for anon/service_role" ON ops.etl_step_metrics FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON mlops.training_runs;
CREATE POLICY "Enable all for anon/service_role" ON mlops.training_runs FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON mlops.feature_importance;
CREATE POLICY "Enable all for anon/service_role" ON mlops.feature_importance FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON mlops.predictions;
CREATE POLICY "Enable all for anon/service_role" ON mlops.predictions FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON mlops.monitoring_runs;
CREATE POLICY "Enable all for anon/service_role" ON mlops.monitoring_runs FOR ALL USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON mlops.model_candidates;
CREATE POLICY "Enable all for anon/service_role" ON mlops.model_candidates FOR ALL USING (true) WITH CHECK (true);

-- Grants for PostgREST + RPC
GRANT USAGE ON SCHEMA core TO anon, authenticated, service_role;
GRANT SELECT ON core.v_top_provinces TO anon, authenticated, service_role;
GRANT SELECT ON core.v_ingestion_series TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION core.rpc_top_provinces(INT) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION core.rpc_ingestion_series(TEXT) TO anon, authenticated, service_role;
GRANT USAGE ON SCHEMA ops TO anon, authenticated, service_role;
GRANT SELECT ON ops.service_health TO anon, authenticated, service_role;
GRANT SELECT ON ops.etl_step_metrics TO anon, authenticated, service_role;
GRANT USAGE ON SCHEMA mlops TO anon, authenticated, service_role;
GRANT SELECT ON mlops.training_runs TO anon, authenticated, service_role;
GRANT SELECT ON mlops.feature_importance TO anon, authenticated, service_role;
GRANT SELECT ON mlops.predictions TO anon, authenticated, service_role;
GRANT SELECT ON mlops.monitoring_runs TO anon, authenticated, service_role;
GRANT SELECT ON mlops.model_candidates TO anon, authenticated, service_role;
GRANT SELECT ON mlops.v_latest_training_run TO anon, authenticated, service_role;
GRANT SELECT ON mlops.v_latest_quality_risk_predictions TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION mlops.rpc_latest_quality_risks(INT) TO anon, authenticated, service_role;
