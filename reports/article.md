# A Reproducible Supabase-Based Pipeline for Ecuador Academic Offer Data

Author: [Your Name]
Affiliation: [Your Institution]

## Abstract
This work presents a reproducible data engineering pipeline for Ecuador academic offer data using the full Supabase stack, an incremental ETL process, and an interactive Streamlit dashboard. The pipeline ingests a public Excel file, normalizes categorical fields, loads data into a dimensional model, and tracks changes using SCD Type 2 semantics. Data quality checks and audit artifacts are produced for transparency, while operational monitoring and Storage-based artifact publishing improve traceability and reproducibility. A geospatial dashboard enables territorial coverage analysis by province and canton, quality monitoring, and ingestion timelines. Results from a 20,045 row dataset show 17,129 current offers with 3,706 within-file duplicates, 1,176 invalid territories, and 1,801 conflicting states, highlighting the value of automated quality controls in open data workflows.

Keywords: data engineering, MLOps, Supabase, SCD Type 2, data quality, geospatial analytics, open data

## 1. Introduction
Open academic offer data is valuable for policy analysis and institutional benchmarking, but it is often published as static spreadsheets with inconsistencies, changing values, and limited traceability. This paper introduces a fully containerized pipeline that transforms a public Excel dataset into a governed analytical store, tracks historical changes, and provides geospatial exploration. The contributions include:
- A reproducible Supabase-based architecture with Docker Compose.
- An incremental ETL pipeline with SCD Type 2 change tracking.
- Data quality audits with structured reports and issue logs.
- Storage-backed publication of source files and reports.
- Operational monitoring of Supabase services and ETL health.
- PostgREST and RPC endpoints for analytics without a separate backend.
- A Streamlit dashboard for territorial coverage, quality, and observability.

## 2. Related Work
Dimensional modeling and slowly changing dimensions are standard in data warehousing for handling evolving attributes. SCD Type 2, in particular, preserves history by versioning records. Recent open data pipelines emphasize reproducibility, quality checks, and transparent audit trails, especially when public datasets are updated without explicit versioning.

## 3. Data
The source file is `data/oferta-academica2025.xlsx`, which contains approximately 20k rows and the following columns:
NOMBRE_IES, TIPO_IES, TIPO_FINANCIAMIENTO, NOMBRE_CARRERA, CAMPO_AMPLIO, NIVEL_FORMACION, MODALIDAD, PROVINCIA, CANTON, ESTADO.
The raw file includes header offsets and inconsistent formatting, requiring automated header detection and normalization.

## 4. System Architecture
The system runs the full Supabase stack with Docker Compose:
- supabase/postgres for storage
- kong for API routing
- postgrest for REST access
- gotrue for auth
- realtime and storage services
- supabase studio for administration
- a Streamlit dashboard for analysis

The pipeline writes to four schemas: `raw_ingest`, `core`, `audit`, and `ops`. Streamlit reads directly from Postgres for analytics. PostgREST exposes views and RPC endpoints for API-based access without a separate backend.

## 5. Methods

### 5.1 Ingestion and Normalization
The ETL process detects the real header row, normalizes column names (accent removal, casing, spacing), and normalizes categorical text fields with controlled whitespace and Unicode normalization. A checksum prevents duplicate ingest for identical files.

### 5.2 Dimensional Model and SCD Type 2
The dimensional model includes:
- `core.dim_ies`, `core.dim_program`, `core.dim_territory`
- `core.fact_offer` for offer records

SCD Type 2 is implemented on `core.fact_offer`:
- If `natural_key` is new, insert a current record.
- If `natural_key` exists and `row_hash` changes, close the previous record and insert a new current row.
- If unchanged, update `last_seen_at` only.

This design preserves history across public data releases.

### 5.3 Data Quality Checks
Checks are recorded in `audit.data_quality_runs` and `audit.inconsistencies`:
- duplicates by `natural_key` within the same file
- conflicting `ESTADO` values for the same `natural_key`
- invalid province or canton normalization
- invalid province-canton pairs using an official catalog

### 5.4 Reporting
The ETL generates local reports:
- `reports/data_quality.json`
- `reports/data_quality.html`
- `reports/inconsistencies.csv`

### 5.5 Artifact Publishing (Storage)
If a service role key is present, the ETL uploads the source Excel file and reports to Supabase Storage. Each run stores artifacts under a deterministic prefix (by file_id), enabling durable access, sharing, and reproducible analysis without copying data manually.

### 5.6 Monitoring and Observability
Operational health is captured in `ops.service_health` using periodic checks against Supabase services (auth, rest, storage, studio, etc.). These checks record status, latency, and errors, allowing the dashboard to surface service reliability alongside data quality trends.

### 5.7 API Access (PostgREST + RPC)
Analytical access is provided via PostgREST views and SQL RPC functions (e.g., top provinces, ingestion time series). This approach exposes a typed HTTP interface without additional backend code, and aligns with the Supabase security model.

## 6. Results
For the 2025 dataset, the pipeline produced the following metrics:
- rows_loaded: 20,045
- ingest_new: 17,129
- ingest_updated: 0
- ingest_unchanged: 0
- skipped_missing_dims: 1,050
- duplicates_in_file: 3,706
- invalid_territory: 1,176
- conflicting_estado: 1,801

These results demonstrate that automated quality checks are essential for public data governance and for reliable downstream analytics.

## 7. Visualization and Analytics
The Streamlit dashboard provides:
- KPIs for offers, institutions, programs, and coverage.
- Province and canton choropleth maps with offer counts and field diversity.
- Diversity metrics such as HHI and entropy at province and canton levels.
- Data quality metrics over time and inconsistencies export.
- Ingestion timelines showing rows and change counts per run.
- Monitoring views for service health, ETL success rate, and Storage artifacts.

## 8. Discussion and Limitations
The pipeline surfaces significant inconsistencies that are otherwise invisible in static spreadsheets. The main limitations are the quality of the source data and the reliance on a reference territory catalog. Future work can refine fuzzy matching thresholds, add official codes, and incorporate additional cross-domain datasets.

## 9. Conclusion
This project demonstrates a complete, reproducible pipeline that transforms open academic offer data into a governed analytical system with traceable history, data quality audits, and geospatial insights. The approach is portable to similar public datasets and supports transparent decision making.

## 10. Reproducibility
Reproduction steps:
1. `docker compose up -d`
2. `python -m src.db.init_db` (one-time schema init)
3. `python -m src.etl.ingest --path data/oferta-academica2025.xlsx`
4. Optional: `python -m src.ops.monitor` (service health snapshot)
5. Open `http://localhost:8501` for the dashboard and `http://localhost:54323` for Studio.

## References
- Kimball, R. and Ross, M. The Data Warehouse Toolkit.
- Supabase documentation.
- PostgREST documentation.
- Streamlit documentation.
