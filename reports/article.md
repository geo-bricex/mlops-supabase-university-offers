# A Reproducible Supabase-Based Pipeline for Ecuador Academic Offer Data

Author: [Your Name]
Affiliation: [Your Institution]

## Abstract
This work presents a reproducible data engineering pipeline for Ecuador academic offer data using the full Supabase stack, an incremental ETL process, and an interactive Streamlit dashboard. The pipeline ingests a public Excel file, normalizes categorical fields, loads data into a dimensional model, and tracks changes using SCD Type 2 semantics. Data quality checks and audit artifacts are produced for transparency, while operational monitoring and Storage-based artifact publishing improve traceability and reproducibility. A geospatial dashboard enables territorial coverage analysis by province and canton, quality monitoring, and ingestion timelines. Results from a 20,045-row dataset show 17,128 newly loaded offers, 3,706 within-file duplicates, 1,176 unresolved territories, and 1,801 conflicting-state groups, highlighting the value of automated quality controls in open data workflows.

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

### 5.8 Quality-risk model selection
The 18,179 distinct `natural_key` groups were split once into approximately 80% training and 20% test partitions using stratified group assignment with `random_state=42`; the resulting 16,036/4,009 row split had zero group overlap. Hyperparameters for Logistic Regression, Gradient Boosting, and Random Forest were selected exclusively from training data using `RandomizedSearchCV` with 40 sampled configurations per algorithm, five-fold `StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=42)`, `groups=natural_key`, `scoring="average_precision"`, `refit=True`, and parallel execution. The primary leakage-controlled scenario retained only pre-audit contextual predictors and excluded direct label-rule inputs, outputs, and deterministic proxies. A full-feature scenario was retained solely as a rule-reproduction sensitivity analysis. Imputation, sparse one-hot encoding, applicable scaling, and classification were fitted inside scikit-learn pipelines in every fold. The model family was selected by mean grouped cross-validated Average Precision; grouped OOF F1 was reserved only for an exact tie. True grouped OOF probabilities also determined the operational F2 threshold. The sealed test metrics were computed only after the feature scenario, candidate configurations, winner, and threshold had been fixed.

## 6. Results
For the 2025 dataset, the pipeline produced the following metrics:
- rows_loaded: 20,045
- ingest_new: 17,128
- ingest_updated: 0
- ingest_unchanged: 0
- skipped_missing_dims: 1,051
- duplicates_in_file: 3,706
- invalid_territory: 1,176
- conflicting_estado: 1,801

These results demonstrate that automated quality checks are essential for public data governance and for reliable downstream analytics.

### 6.1 Quality-risk classification
Experiment `8c464366-c5ab-433a-abb0-380bad37683a` contained 20,045 observations (15,392 negative; 4,653 positive) and 18,179 groups. In the primary leakage-controlled scenario, Random Forest was selected with grouped-CV Average Precision 0.631359 (SD 0.013577), ahead of Gradient Boosting (0.627654, SD 0.015247) and Logistic Regression (0.598698, SD 0.012425). At the reference threshold of 0.5, its sealed-test results were accuracy 0.805687, precision 0.564298, recall 0.716434, F1 0.631330, ROC AUC 0.849264, and Average Precision 0.655390, with confusion matrix `[[2563, 515], [264, 667]]`. The operational threshold 0.36 was chosen only from grouped OOF training predictions by maximizing F2; on the sealed test it produced precision 0.419268, recall 0.836735, and F1 0.558623. The full-feature sensitivity scenario produced substantially higher values (selected-model grouped-CV AP 0.915518), confirming that direct audit-rule proxies make rule reproduction much easier and must not be reported as the primary predictive result.

## 7. Visualization and Analytics
The Streamlit dashboard provides:
- KPIs for offers, institutions, programs, and coverage.
- Province and canton choropleth maps with offer counts and field diversity.
- Diversity metrics such as HHI and entropy at province and canton levels.
- Data quality metrics over time and inconsistencies export.
- Ingestion timelines showing rows and change counts per run.
- Monitoring views for service health, ETL success rate, and Storage artifacts.

## 8. Discussion and Limitations
The pipeline surfaces significant inconsistencies that are otherwise invisible in static spreadsheets. The ML target is a deterministic audit-rule proxy rather than independently adjudicated ground truth; therefore the classifier measures prioritization of rule-defined risk, not an externally validated latent construct. Grouped validation prevents duplicate-key leakage but does not establish temporal or external generalizability. The full-feature analysis is sensitivity evidence only. Other limitations are the source-data quality and reliance on a reference territory catalog. Future work should add independent expert labels, temporal/external validation, refined matching thresholds, and official geographic codes.

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
