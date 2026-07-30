# Ecuador Academic Offer - MLOps Supabase Project

This project implements a full MLOps pipeline for ingesting, validating, and analyzing Ecuador's Academic Offer data. It uses the full self-hosted Supabase stack via Docker and provides an interactive Streamlit dashboard.

## Tech Stack
- **Database**: Supabase (Postgres, Kong, GoTrue, Realtime, etc.)
- **ETL**: Python, Pandas, SQLAlchemy (Incremental Loading, SCD Type 2)
- **ML**: scikit-learn quality-risk classifier with registry, evaluation, scoring, and monitoring
- **Dashboard**: Streamlit (Geospatial analysis, Time series)
- **Containerization**: Docker Compose

## Prerequisites
- Docker & Docker Compose
- Python 3.14 (the Docker image and pinned scientific stack use the same major/minor version)

## Setup & Running

1. **Start the full stack**
   ```bash
   docker compose up -d --build
   ```

   **Services**:
   - Studio: [http://localhost:54323](http://localhost:54323)
   - Dashboard: [http://localhost:8501](http://localhost:8501)
   - Supabase API: [http://localhost:8000](http://localhost:8000)
   Note: inside Docker, services reach Supabase via `SUPABASE_URL_INTERNAL=http://kong:8000`.

2. **ETL**
   The `etl` container boots with the stack, initializes the schema, and ingests the file defined in `SOURCE_FILE`.
   After ETL completes, the `ml-trainer` container trains and registers the latest quality-risk model automatically.
   If you replace the source file and need to rerun the load, use:
   ```bash
   docker compose run --rm etl
   ```

3. **MLOps Training**
   The `ml-trainer` service turns the audited ingest into a supervised ML task:
   - target: whether a staged row is likely to contain a data-quality issue
   - training: automated after ETL
   - split: stratified 80% training / 20% test with `random_state=42`
   - sealed test: never used for model, feature, transformation, or hyperparameter selection
   - candidate family: exactly Logistic Regression, Gradient Boosting, and Random Forest
   - search: `RandomizedSearchCV`, 40 combinations per algorithm, `refit=True`, and `n_jobs=-1`
   - validation: `StratifiedKFold(n_splits=5, shuffle=True, random_state=42)`
   - primary criterion: mean cross-validated Average Precision (`scoring="average_precision"`)
   - tie-breaker: mean cross-validated F1 only if Average Precision is exactly tied
   - leakage control: imputation, categorical encoding, and applicable scaling remain inside `Pipeline`/`ColumnTransformer`
   - final evaluation: each refitted best configuration is evaluated once on the sealed test
   - registry: versioned candidate metadata in `mlops.model_candidates` and selected run in `mlops.training_runs`
   - scoring: row-level probabilities stored in `mlops.predictions`
   - persistence: artifact is kept on disk and also uploaded to Supabase Storage through the same stack
   - monitoring: snapshots stored in `mlops.monitoring_runs`

   If the latest dataset does not have enough class diversity to train a meaningful model, the trainer records a `skipped` run instead of breaking the stack.

### Hyperparameter spaces

All spaces are discrete, recorded verbatim in every run, and sampled with `random_state=42`.

| Algorithm | Parameters and evaluated values |
|---|---|
| Logistic Regression | `C=[0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1, 3, 10, 30, 100]`; `class_weight=[null, "balanced"]`; `max_iter=[500, 1000, 2000]`; valid separated dictionaries for `l1+saga+l1_ratio=1.0`, `l2+[lbfgs, liblinear, saga]+l1_ratio=0.0`, and `elasticnet+saga` with `l1_ratio=[0.1, 0.25, 0.5, 0.75, 0.9]` |
| Gradient Boosting | `n_estimators=[50, 100, 150, 200, 300]`; `learning_rate=[0.01, 0.03, 0.05, 0.1, 0.2]`; `max_depth=[1, 2, 3, 4]`; `subsample=[0.6, 0.75, 0.9, 1.0]`; `min_samples_split=[2, 5, 10, 20]`; `min_samples_leaf=[1, 2, 5, 10]` |
| Random Forest | `n_estimators=[100, 200, 300, 500]`; `max_depth=[null, 5, 10, 20, 30]`; `max_features=["sqrt", "log2", 0.5, null]`; `min_samples_split=[2, 5, 10, 20]`; `min_samples_leaf=[1, 2, 4, 8]`; `class_weight=[null, "balanced", "balanced_subsample"]` |

Logistic Regression uses one-hot encoding plus standardization. The tree models use ordinal encoding (unknown value `-1`) and no scaling to keep the 200 Gradient Boosting fits computationally feasible. Both encoders and every imputer are fitted only inside each CV fold.

### Reproduce the scientific experiment

Create an isolated environment and install the pinned versions:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\python -m pip install -r requirements.txt
# Linux/macOS
.venv/bin/python -m pip install -r requirements.txt
```

Run from the canonical workbook without a database:

```bash
python -m src.ml.train \
  --source-path data/oferta-academica2025.xlsx \
  --no-persist \
  --n-iter 40 \
  --n-jobs -1
```

The local path reuses the ETL normalization, territory matching, natural-key generation, and quality-rule functions; it does not redefine labels. To train and persist from an existing Supabase ingest:

```bash
python -m src.ml.train --file-id <INGEST_UUID> --n-iter 40 --n-jobs -1
```

Or run the normal Docker workflow:

```bash
docker compose run --rm ml-trainer
```

Run verification:

```bash
python -m pytest -q
python -m ruff check --select E4,E7,E9,F src tests
python -m ruff format --check src/ml src/dq/checks.py src/db/init_db.py src/db/session.py src/storage/supabase_storage.py tests
```

Each run writes lightweight publication evidence to `reports/modeling/<run_id>/`: complete `cv_results_` CSV files, the exact search-space JSON, comparison CSV, result JSON, curve points, ROC/PR figures, and SHA-256 manifest. Fitted pipelines and the selected-model alias are stored under `artifacts/experiments/<run_id>/` and intentionally ignored by Git.

The generated methodological and editorial documents are:

- `docs/hyperparameter_selection_report.md`
- `docs/editorial_response_hyperparameters.md`

The run uses approximately 600 CV fits (120 sampled configurations × 5 folds), plus cross-validated F1 verification. Runtime and memory depend on CPU cores; use `--n-jobs 1` on memory-constrained systems, but keep `--n-iter 40` for the reported experiment.

### Latest verified experiment

Run `0f6f077d-9e12-4129-93bf-7048a7d15bdc` used all 20,045 source rows (15,392 class 0; 4,653 class 1), with 16,036 training and 4,009 sealed test rows. The source SHA-256 is `fe366924ce44b577c74f72282b042ca7908aedf59445db00893b9a3b2d58848f`.

| Algorithm | CV Average Precision (mean ± SD) | CV F1 | Test Average Precision | Test F1 | Status |
|---|---:|---:|---:|---:|---|
| Logistic Regression | 0.846745 ± 0.012465 | 0.743507 | 0.846498 | 0.741497 | rejected |
| Gradient Boosting | **0.916566 ± 0.004211** | **0.823544** | 0.916629 | 0.821705 | **selected** |
| Random Forest | 0.914684 ± 0.005417 | 0.813988 | 0.917905 | 0.815244 | rejected |

Gradient Boosting was selected before test evaluation because it had the highest training-CV Average Precision. Random Forest's slightly higher test Average Precision did not change the winner. The complete parameters, all test metrics, confusion matrices, and curves are under `reports/modeling/0f6f077d-9e12-4129-93bf-7048a7d15bdc/`.

This verified run used the database-independent reproduction path because Docker Desktop/Supabase was unavailable in the execution environment; therefore its JSON correctly records persistence status `not_requested`. The same payload is written to Supabase by the database-backed command, and the migration can be applied with:

```bash
psql "$DB_CONNECTION_STRING" \
  -f sql/migrations/002_q1_hyperparameter_traceability_up.sql
```

Rollback (drops only the additive Q1 candidate table/columns):

```bash
psql "$DB_CONNECTION_STRING" \
  -f sql/migrations/002_q1_hyperparameter_traceability_down.sql
```

Computational limitations observed for the verified run:

- Model search/evaluation took 665 seconds (about 712 seconds including workbook reconstruction) on 20 parallel workers and briefly used roughly 4–5 GB across worker processes.
- Tree-model ordinal encoding is a documented tractability trade-off; it avoids a large dense one-hot matrix but imposes numeric codes that should not be interpreted as substantive category order.
- scikit-learn 1.9.0 still executes the editor-requested `penalty` search but emits a deprecation warning because the library plans to replace that API in 1.10. The pinned 1.9.0 environment preserves this experiment; any API migration requires a new run and new reported metrics.
- Supabase/Storage persistence could not be exercised in the verified local environment because Docker Desktop was unavailable. SQL shape, additive migration, rollback, payload fields, and local artifact persistence are tested; database-backed reproduction remains the required integration check where Supabase is running.

4. **View Dashboard**
   Navigate to [http://localhost:8501](http://localhost:8501).

5. **Local LLM (Ollama)**
   The Ollama container pulls the configured model on first boot and the `ollama-warmup` container loads it once before the dashboard becomes available.
   The dashboard uses `OLLAMA_INTERNAL_URL` for container-to-container traffic.
   The safer default is `qwen2.5:1.5b` because it is more replicable on modest machines. If you need a different model, update `OLLAMA_MODEL` in `.env` before starting.
   To ingest another dataset, change `SOURCE_FILE` in `.env`.

6. **Reports**
   The ETL writes:
   - `reports/data_quality.json`
   - `reports/data_quality.html`
   - `reports/inconsistencies.csv`
   The MLOps layer writes:
   - `artifacts/experiments/<run_id>/*_best_pipeline.joblib`
   - `artifacts/experiments/<run_id>/*_selected_model.joblib`
   - `reports/modeling/<run_id>/*_cv_results.csv`
   - `reports/modeling/<run_id>/*_model_comparison.csv`
   - `reports/modeling/<run_id>/*_results.json`
   - `reports/modeling/<run_id>/*_curve.csv` and `*.png`

7. **Storage & Monitoring**
   The stack now runs a dedicated `storage-migrate` step before ETL so Supabase Storage is provisioned automatically.
   If `SUPABASE_SERVICE_ROLE_KEY` is set, the ETL uploads the source file and reports to Supabase Storage.
   The MLOps trainer uploads the model artifact and metadata JSON to the same bucket.
   Uploads use the internal Storage endpoint defined by `SUPABASE_STORAGE_URL_INTERNAL`, so they do not depend on host routing.
   Configure the bucket with `SUPABASE_STORAGE_BUCKET` and access it in Studio under Storage.
   Pipeline run metrics (duration, file size, change counts, storage status) are tracked in `raw_ingest.files`
   and visualized in the dashboard Timeline and Monitoring tabs. Step-level timings are captured in
   `ops.etl_step_metrics` and summarized in `raw_ingest.files.process_metrics`.
   Record service health checks with:
   ```bash
   docker compose exec dashboard python -m src.ops.monitor
   ```
   To run periodically (example every 60s):
   ```bash
   docker compose exec dashboard python -m src.ops.monitor --interval 60
   ```
   Override endpoints using `SUPABASE_HEALTH_ENDPOINTS` (JSON map) if running outside Docker.

8. **PostgREST + RPC Analytics**
   Apply schema updates if needed:
   ```bash
   docker compose exec dashboard python -m src.db.init_db
   ```
   Example PostgREST view (top provinces):
   ```bash
   curl "http://localhost:8000/rest/v1/v_top_provinces?select=provincia_norm,offers&order=offers.desc&limit=10" \
     -H "apikey: <SUPABASE_ANON_KEY>" \
     -H "Authorization: Bearer <SUPABASE_ANON_KEY>"
   ```
   Example RPC (ingestion time series):
   ```bash
   curl -X POST "http://localhost:8000/rest/v1/rpc/rpc_ingestion_series" \
     -H "Content-Type: application/json" \
     -H "apikey: <SUPABASE_ANON_KEY>" \
     -H "Authorization: Bearer <SUPABASE_ANON_KEY>" \
     -d '{"bucket":"day"}'
   ```

## Troubleshooting
- `Ollama` and `Postgres` are internal-only services in Docker, so they should not fail because of host port collisions.
- If Docker reports a port-bind error now, it is more likely coming from `Dashboard`, `Studio`, or `Kong`, which are the only host-facing services required for the default demo.
- If you want to inspect the ML outputs through APIs, the `mlops` schema is exposed through PostgREST together with `core`, `audit`, and `ops`.
- If Supabase services (auth, rest, realtime, storage) keep restarting due to password errors, recreate the stack from scratch with `docker compose down -v` and start again after confirming `.env`.
- If artifact upload still fails, check `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_STORAGE_BUCKET`, and the `storage-migrate` logs first.
- Supabase persistence requires `DB_CONNECTION_STRING` (or `DB_HOST`, `POSTGRES_PORT`, `DB_USER`, `POSTGRES_PASSWORD`, and `DB_NAME`), plus `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_STORAGE_BUCKET`, and optionally `SUPABASE_STORAGE_URL_INTERNAL`. Local `--no-persist` experiments require none of these secrets.
- `.env.example` contains deliberately non-functional JWT placeholders. Generate `SUPABASE_ANON_KEY` and `SUPABASE_SERVICE_ROLE_KEY` locally with the configured `JWT_SECRET`; never commit the resulting `.env`.
- If local LLM interpretation is still slow, keep `qwen2.5:1.5b` or reduce `OLLAMA_NUM_PREDICT` before moving to a larger model.
- `average_precision`, five folds, 40 iterations, and `random_state=42` are methodology constants rather than environment overrides. `ML_RISK_THRESHOLD` controls only the fixed post-selection classification threshold.
- If you change `JWT_SECRET`, keep the anon/service tokens in `.env` and `docker/volumes/api/kong.yml` aligned.

## Project Structure
- `data/`: Source Excel files.
- `src/`: Python source code for ETL and logic.
- `sql/`: Database logic (tables, functions).
- `sql/migrations/`: Additive and reversible registry migrations.
- `docs/`: Generated Q1 methodology and editorial-response documents.
- `reports/modeling/`: Versioned lightweight experiment evidence.
- `dashboard/`: Streamlit application.
- `assets/geo/`: GeoJSON files for maps.

## Geo Data Sources
The GeoJSON assets in `assets/geo/` are derived from GADM 4.1 (Global Administrative Areas) for Ecuador:
- Level 1 (provinces) -> `assets/geo/ecuador_provinces.geojson`
- Level 2 (cantons) -> `assets/geo/ecuador_cantons.geojson`
- `assets/geo/territory_catalog.csv` built from the level 2 attributes (NAME_1, NAME_2, GID_1, GID_2) plus normalized fields.

Reproduction outline:
1. Download GADM 4.1 Ecuador shapefiles (levels 1 and 2).
2. Convert to GeoJSON (example):
   ```bash
   ogr2ogr -f GeoJSON assets/geo/ecuador_provinces.geojson gadm41_ECU_1.shp
   ogr2ogr -f GeoJSON assets/geo/ecuador_cantons.geojson gadm41_ECU_2.shp
   ```
3. Build `territory_catalog.csv` from the level 2 attribute table.

## Development
- To stop everything: `docker compose down -v`
- Run tests (inside Docker): `docker compose exec dashboard pytest`
