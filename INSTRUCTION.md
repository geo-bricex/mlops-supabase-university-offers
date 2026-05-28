# Instruction

This repository is a fully containerized research pipeline for Ecuador academic offer data.
The default workflow is:

1. Start the stack with Docker Compose.
2. Let the Ollama container pull the model on first boot.
3. Let the ETL container initialize the schema and load the Excel source.
4. Review data quality, monitoring, and the dashboard.
5. Use the local Ollama model for interpretation.

## What this project does

- `docker compose` starts the complete Supabase stack, the local Ollama service, and the Streamlit dashboard.
- The Ollama container downloads the configured model on first startup.
- A dedicated warmup container loads the model once before the dashboard is exposed, so the first interpretation request does not start from a cold model.
- The ETL container runs the research pipeline inside Docker and exits when the load finishes.
- `src.db.init_db` creates or updates schemas, tables, views, RPC functions, and policies.
- `src.etl.ingest` reads the Excel file, normalizes fields, matches geography, computes keys, loads dimensions and facts, runs data quality checks, writes reports, and uploads artifacts when storage credentials are available.
- `src.ops.monitor` records service health checks for the MLOps layer.
- `dashboard/streamlit_app.py` is the research interface for geospatial analysis, data quality, timelines, monitoring, and local LLM interpretation.

## Before You Start

1. Install Docker Desktop.
2. Open PowerShell in the repository root.
3. Copy `.env.example` to `.env` if the file does not exist yet.
4. Review only the values that are usually machine-specific:
   - `POSTGRES_PASSWORD`
   - `JWT_SECRET`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `OLLAMA_MODEL`
   - `SOURCE_FILE`
5. Keep `SUPABASE_URL_INTERNAL=http://kong:8000` for the container network.
6. Put the source dataset at `data/oferta-academica2025.xlsx`, or change `SOURCE_FILE` in `.env` if you use another file.

## Key Variables

- `POSTGRES_PASSWORD`: protects the local Postgres/Supabase stack. Change it if you want a different local secret.
- `JWT_SECRET`: signs Supabase auth tokens. Keep it long and stable inside the same project.
- `SUPABASE_ANON_KEY` and `SUPABASE_SERVICE_ROLE_KEY`: local API keys used by Studio, ETL, and Storage.
- `OLLAMA_INTERNAL_URL`: internal Docker endpoint used by the dashboard and other containers. Do not open this in the browser.
- `OLLAMA_MODEL`: local model downloaded inside the Ollama container. The default `qwen2.5:1.5b` is chosen for easier replication on CPU-only machines.
- `OLLAMA_TIMEOUT`: time limit for a dashboard interpretation request. Increase this only if you deliberately move to a larger local model.
- `OLLAMA_NUM_PREDICT`: maximum generated tokens for the interpretation response. Keep this moderate to avoid very long local inference times.
- `SOURCE_FILE`: path used by the ETL container. Change this when you want to ingest another Excel file without editing code.
- `SUPABASE_STORAGE_BUCKET` and `SUPABASE_STORAGE_PUBLIC`: optional artifact publishing settings. Leave them as-is if you only want the pipeline to run and do not need Storage uploads yet.

## Docker-First Start

The project is dockerized, so the canonical start is Docker Compose:

```powershell
docker compose up -d --build
```

What this command does:

- Builds and starts the full Docker stack.
- Starts Ollama, Supabase services, and the dashboard container.
- Pulls the local Ollama model inside the Ollama container on first boot.
- Runs the ETL container so the database is initialized and the source file is ingested inside Docker.

If you want a different model, update `OLLAMA_MODEL` in `.env` before starting:

```text
OLLAMA_MODEL=qwen2.5:1.5b
```

If you want to ingest another dataset, change `SOURCE_FILE`:

```text
SOURCE_FILE=data/another_file.xlsx
```

## Research Pipeline

The ETL runs automatically inside Docker during startup and uses `data/oferta-academica2025.xlsx` by default.

If you replace the source file and want to run the load again, use:

```powershell
docker compose run --rm etl
```

Expected outputs after a successful run:

- `reports/data_quality.json`
- `reports/data_quality.html`
- `reports/inconsistencies.csv`
- new rows in `raw_ingest.files`
- new audit rows in `audit.data_quality_runs` and `audit.inconsistencies`
- step timings in `ops.etl_step_metrics`

The ETL is idempotent by checksum. If you run the same file again without changes, it should skip the duplicate load.

## Monitoring

Capture a one-time health snapshot:

```powershell
docker compose exec dashboard python -m src.ops.monitor
```

Capture checks every 60 seconds:

```powershell
docker compose exec dashboard python -m src.ops.monitor --interval 60
```

## Validation

Run the tests inside the dashboard container:

```powershell
docker compose exec dashboard pytest -q
```

If you want a quick sanity check after ingest, run `docker compose run --rm etl` again with the same file.
The second run should be skipped by checksum if the source did not change.

## Open the Apps

- Dashboard: `http://localhost:8501`
- Supabase Studio: `http://localhost:54323`
- Kong API: `http://localhost:8000`

Important:

- `http://ollama:11434` is the internal Docker hostname and only containers can resolve it.
- The local AI service is intentionally internal-only. You use it through the dashboard, not by opening Ollama directly in the browser.

## How To Use The Dashboard

1. Wait until the stack is fully up.
2. Open the dashboard.
3. Use the Overview tab for KPIs and the research summary.
4. Use Geographic Coverage for province and canton analysis.
5. Use Data Quality and Monitoring for the MLOps view.
6. Use the interpretation button only after Ollama has the model ready.

## What Not To Change Unless Needed

- Do not change the schema names unless you also update the SQL and Python code.
- Do not change the internal Supabase URL inside Docker.
- Do not rename the source Excel file unless you also update the ingest command.
- Do not remove the local territory catalog in `assets/geo/` if you want reproducible geography matching.

## Troubleshooting

- If the dashboard says the database is missing, run `src.db.init_db` again.
- If Ollama is too slow, keep `qwen2.5:1.5b` or move to an even lighter model before trying a larger one.
- If the dashboard shows a timeout, check that the warmup container completed successfully with `docker compose ps`.
- `Ollama` and `Postgres` are internal-only in Docker, so they should not fail because of host port collisions in the default setup.
- If Docker shows a port-bind error now, focus on `Dashboard`, `Studio`, or `Kong`, which are the only services intentionally exposed to the host in the default setup.
- If storage uploads fail, check `SUPABASE_SERVICE_ROLE_KEY` and `SUPABASE_STORAGE_BUCKET`.
- If a service keeps restarting, check the container logs:

```powershell
docker compose logs -f dashboard
```

- If you need a clean stop:

```powershell
docker compose down
```

- If you want a fully fresh database, stop the stack first and then remove the named volume `supabase_db_data` only when you really want to recreate the local Postgres state from scratch.

## Research Flow In Short

1. Start the container stack.
2. Let the ETL container bootstrap the schema and ingest the source file.
3. Review quality, monitoring, and timeline metrics.
4. Use the local LLM to interpret the results.

## What To Change Later

If you want another dataset:

1. Put the new Excel file inside `data/`.
2. Change only `SOURCE_FILE` in `.env`.
3. Run `docker compose run --rm etl`.

If you want another local model:

1. Change only `OLLAMA_MODEL` in `.env`.
2. Run `docker compose up -d`.
3. Wait for `ollama` and `ollama-warmup` to finish preparing the new model.
