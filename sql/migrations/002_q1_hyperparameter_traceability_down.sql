-- Revert migration 002. Existing legacy registry columns remain untouched.
BEGIN;

DROP TABLE IF EXISTS mlops.model_candidates;

ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS model_status;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS run_metadata;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS git_commit;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS sklearn_version;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS python_version;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS random_state;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS class_distribution;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS dataset_rows;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS dataset_sha256;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS best_score;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS best_params;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS search_spaces;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS search_iterations;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS cv_folds;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS optimization_metric;
ALTER TABLE mlops.training_runs DROP COLUMN IF EXISTS search_method;

COMMIT;
