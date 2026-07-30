-- Additive, backward-compatible traceability for Q1 model-selection experiments.
BEGIN;

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

ALTER TABLE mlops.model_candidates ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Enable all for anon/service_role" ON mlops.model_candidates;
CREATE POLICY "Enable all for anon/service_role"
    ON mlops.model_candidates FOR ALL USING (true) WITH CHECK (true);

GRANT SELECT ON mlops.model_candidates TO anon, authenticated, service_role;

COMMIT;
