# Ecuador Academic Offer - MLOps Supabase Project

This project implements a full MLOps pipeline for ingesting, validating, and analyzing Ecuador's Academic Offer data. It uses the full self-hosted Supabase stack via Docker and provides an interactive Streamlit dashboard.

## Tech Stack
- **Database**: Supabase (Postgres, Kong, GoTrue, Realtime, etc.)
- **ETL**: Python, Pandas, SQLAlchemy (Incremental Loading, SCD Type 2)
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
   - Postgres: `localhost:54322`
   - Ollama: `http://localhost:11434`
   Note: inside Docker, services reach Supabase via `SUPABASE_URL_INTERNAL=http://kong:8000`.

2. **ETL**
   The `etl` container boots with the stack, initializes the schema, and ingests the file defined in `SOURCE_FILE`.
   If you replace the source file and need to rerun the load, use:
   ```bash
   docker compose run --rm etl
   ```

3. **View Dashboard**
   Navigate to [http://localhost:8501](http://localhost:8501).

4. **Local LLM (Ollama)**
   The Ollama container pulls the configured model on first boot and the `ollama-warmup` container loads it once before the dashboard becomes available.
   The dashboard uses `OLLAMA_INTERNAL_URL` for container-to-container traffic and shows `OLLAMA_PUBLIC_URL` as the browser-safe address.
   The safer default is `qwen2.5:1.5b` because it is more replicable on modest machines. If you need a different model, update `OLLAMA_MODEL` in `.env` before starting.
   To ingest another dataset, change `SOURCE_FILE` in `.env`.

5. **Reports**
   The ETL writes:
   - `reports/data_quality.json`
   - `reports/data_quality.html`
   - `reports/inconsistencies.csv`

6. **Storage & Monitoring**
   If `SUPABASE_SERVICE_ROLE_KEY` is set, the ETL uploads the source file and reports to Supabase Storage.
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

7. **PostgREST + RPC Analytics**
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
- If Supabase services (auth, rest, realtime, storage) keep restarting due to password errors, recreate the stack from scratch with `docker compose down -v` and start again after confirming `.env`.
- Artifact upload to Storage is optional. If `SUPABASE_SERVICE_ROLE_KEY` is missing or Storage is not fully provisioned yet, the ETL still completes and only skips uploads.
- `http://ollama:11434` is only resolvable inside Docker. From your browser use `http://localhost:11434` or the value of `OLLAMA_PUBLIC_URL`.
- If local LLM interpretation is still slow, keep `qwen2.5:1.5b` or reduce `OLLAMA_NUM_PREDICT` before moving to a larger model.
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
