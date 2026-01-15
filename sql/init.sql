-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw_ingest;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS ops;

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

-- Grants for PostgREST + RPC
GRANT USAGE ON SCHEMA core TO anon, authenticated, service_role;
GRANT SELECT ON core.v_top_provinces TO anon, authenticated, service_role;
GRANT SELECT ON core.v_ingestion_series TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION core.rpc_top_provinces(INT) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION core.rpc_ingestion_series(TEXT) TO anon, authenticated, service_role;
GRANT USAGE ON SCHEMA ops TO anon, authenticated, service_role;
GRANT SELECT ON ops.service_health TO anon, authenticated, service_role;
GRANT SELECT ON ops.etl_step_metrics TO anon, authenticated, service_role;
