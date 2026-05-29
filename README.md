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
- Python 3.9+

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
   - model selection: evaluates multiple candidate models and activates the best one
   - candidate family: regularized logistic variants, Extra Trees, and Random Forest
   - evaluation: holdout metrics stored in `mlops.training_runs.metrics`
   - anti-overfitting guard: cross-validation, bounded tree depth, regularization, train-vs-test gap tracking, and a conservative decision threshold
   - registry: versioned model artifact saved in `artifacts/models`
   - scoring: row-level probabilities stored in `mlops.predictions`
   - persistence: artifact is kept on disk and also uploaded to Supabase Storage through the same stack
   - monitoring: snapshots stored in `mlops.monitoring_runs`

   If the latest dataset does not have enough class diversity to train a meaningful model, the trainer records a `skipped` run instead of breaking the stack.

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
   - `artifacts/models/<model-version>.joblib`

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
- If local LLM interpretation is still slow, keep `qwen2.5:1.5b` or reduce `OLLAMA_NUM_PREDICT` before moving to a larger model.
- If you want to tune model selection, review `ML_PRIMARY_SELECTION_METRIC`, `ML_RISK_THRESHOLD`, and the MLOps tab in the dashboard.
- If you change `JWT_SECRET`, keep the anon/service tokens in `.env` and `docker/volumes/api/kong.yml` aligned.

## Project Structure
- `data/`: Source Excel files.
- `src/`: Python source code for ETL and logic.
- `sql/`: Database logic (tables, functions).
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
