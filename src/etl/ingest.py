import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import unicodedata
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.db.init_db import ensure_schema
from src.db.session import engine, get_db_session
from src.dq.checks import DataQualityChecker
from src.geo.matching import GeoMatcher
from src.storage.supabase_storage import upload_artifacts

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("etl_ingest")

REQUIRED_COLUMNS = [
    'NOMBRE_IES', 'TIPO_IES', 'TIPO_FINANCIAMIENTO', 'NOMBRE_CARRERA',
    'CAMPO_AMPLIO', 'NIVEL_FORMACION', 'MODALIDAD', 'PROVINCIA', 'CANTON', 'ESTADO'
]


def compute_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def normalize_column_name(col_name: str) -> str:
    text = unicodedata.normalize('NFKD', str(col_name))
    text = text.encode('ASCII', 'ignore').decode('utf-8')
    text = text.strip().upper()
    text = re.sub(r'[^A-Z0-9]+', '_', text)
    text = re.sub(r'_+', '_', text).strip('_')
    return text


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = {c: normalize_column_name(c) for c in df.columns}
    collisions = {}
    for original, norm in normalized.items():
        collisions.setdefault(norm, []).append(original)
    dupes = {k: v for k, v in collisions.items() if len(v) > 1}
    if dupes:
        raise ValueError(f"Duplicate columns after normalization: {dupes}")
    return df.rename(columns=normalized)


def detect_header_row(df_raw: pd.DataFrame, max_scan: int = 50) -> Optional[int]:
    scan_limit = min(max_scan, len(df_raw))
    for idx in range(scan_limit):
        row_values = df_raw.iloc[idx].tolist()
        normalized = [normalize_column_name(v) for v in row_values]
        if all(col in normalized for col in REQUIRED_COLUMNS):
            return idx
    return None


def load_excel(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path)
    df = normalize_columns(df)
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if not missing_cols:
        return df

    df_raw = pd.read_excel(file_path, header=None)
    header_row = detect_header_row(df_raw)
    if header_row is None:
        raise ValueError(f"Missing columns: {missing_cols}")

    df = pd.read_excel(file_path, header=header_row)
    df = normalize_columns(df)
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    return df


def normalize_value(matcher: GeoMatcher, value):
    if pd.isna(value) or value == '':
        return None
    text_val = str(value).replace('\xa0', ' ').replace('\t', ' ')
    return matcher.normalize_text(text_val)


def safe_key_part(value) -> str:
    if value is None:
        return ''
    if isinstance(value, float) and pd.isna(value):
        return ''
    return str(value).strip()


def generate_natural_key(row):
    # Deterministic concatenation using normalized fields.
    parts = [
        safe_key_part(row.get('nombre_norm')),
        safe_key_part(row.get('carrera_norm')),
        safe_key_part(row.get('campo_amplio_norm')),
        safe_key_part(row.get('nivel_formacion_norm')),
        safe_key_part(row.get('modalidad_norm')),
        safe_key_part(row.get('provincia_norm')),
        safe_key_part(row.get('canton_norm'))
    ]
    return "|".join(parts)


def generate_row_hash(row):
    # Includes ESTADO and other normalized fields
    content = {
        'natural_key': row['natural_key'],
        'estado_norm': row.get('estado_norm') or '',
    }
    dump = json.dumps(content, sort_keys=True)
    return hashlib.sha256(dump.encode('utf-8')).hexdigest()


def start_step(step_name: str) -> dict:
    return {
        "step_name": step_name,
        "started_at": datetime.utcnow(),
        "_perf": time.perf_counter()
    }


def finish_step(step: dict, row_count: Optional[int] = None, detail: Optional[dict] = None) -> dict:
    step["finished_at"] = datetime.utcnow()
    step["duration_seconds"] = round(time.perf_counter() - step.pop("_perf"), 6)
    if row_count is not None:
        step["row_count"] = int(row_count)
    if detail is not None:
        step["detail"] = detail
    return step


def write_step_metrics(file_id: str, steps: list) -> None:
    if not file_id or not steps:
        return
    payload = []
    for step in steps:
        detail = step.get("detail")
        detail_json = json.dumps(detail) if detail is not None else None
        payload.append({
            "file_id": file_id,
            "step_name": step.get("step_name"),
            "started_at": step.get("started_at"),
            "finished_at": step.get("finished_at"),
            "duration_seconds": step.get("duration_seconds"),
            "row_count": step.get("row_count"),
            "detail": detail_json
        })
    with get_db_session() as session:
        session.execute(
            text(
                "INSERT INTO ops.etl_step_metrics "
                "(file_id, step_name, started_at, finished_at, duration_seconds, row_count, detail) "
                "VALUES (:file_id, :step_name, :started_at, :finished_at, :duration_seconds, :row_count, CAST(:detail AS JSONB))"
            ),
            payload
        )


def build_process_metrics(df: Optional[pd.DataFrame], steps: list, extra: Optional[dict] = None) -> dict:
    metrics = {"timings": {}}
    for step in steps:
        name = step.get("step_name")
        duration = step.get("duration_seconds")
        if name and duration is not None:
            metrics["timings"][name] = duration

    if df is not None and not df.empty:
        metrics["rows"] = {
            "total": int(len(df)),
            "natural_keys": int(df["natural_key"].nunique()) if "natural_key" in df.columns else None
        }
        metrics["unique"] = {}
        if "nombre_norm" in df.columns:
            metrics["unique"]["ies"] = int(df["nombre_norm"].nunique())
        if "provincia_norm" in df.columns and "canton_norm" in df.columns:
            metrics["unique"]["territories"] = int(df[["provincia_norm", "canton_norm"]].drop_duplicates().shape[0])
        if {"carrera_norm", "campo_amplio_norm", "nivel_formacion_norm", "modalidad_norm"}.issubset(df.columns):
            metrics["unique"]["programs"] = int(
                df[["carrera_norm", "campo_amplio_norm", "nivel_formacion_norm", "modalidad_norm"]]
                .drop_duplicates()
                .shape[0]
            )
        metrics["memory"] = {
            "df_bytes": int(df.memory_usage(deep=True).sum())
        }

    if extra:
        metrics.update(extra)
    return metrics


def should_skip_by_checksum(checksum: str):
    with get_db_session() as session:
        res = session.execute(
            text("SELECT file_id, status FROM raw_ingest.files WHERE checksum_sha256 = :c"),
            {"c": checksum}
        ).fetchone()
        if not res:
            return False, None
        if res.status == 'success':
            note = f"Duplicate checksum skipped at {datetime.utcnow().isoformat()}Z"
            session.execute(
                text("UPDATE raw_ingest.files SET notes = :note WHERE file_id = :fid"),
                {"note": note, "fid": res.file_id}
            )
            logger.info("File already ingested successfully. Skipping.")
            return True, res.file_id
        logger.info(f"File found with status {res.status}. Retrying/Updating.")
        return False, None


def update_storage_metadata(file_id: str, storage_result: dict) -> None:
    status = storage_result.get("status")
    paths = storage_result.get("paths") or {}
    with get_db_session() as session:
        session.execute(
            text(
                "UPDATE raw_ingest.files SET storage_status = :status, storage_paths = CAST(:paths AS JSONB) "
                "WHERE file_id = :fid"
            ),
            {"status": status, "paths": json.dumps(paths), "fid": file_id},
        )


def write_reports(dq: DataQualityChecker, output_dir: Path, file_id: str, file_path: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "file_id": file_id,
        "file_path": file_path,
        "run_id": dq.run_id,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "metrics": dq.metrics
    }

    json_path = output_dir / "data_quality.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    issues_df = pd.DataFrame(dq.issues)
    if not issues_df.empty and isinstance(issues_df.iloc[0].get("detail"), dict):
        issues_df["detail"] = issues_df["detail"].apply(lambda d: json.dumps(d, ensure_ascii=False))
    issues_csv = output_dir / "inconsistencies.csv"
    issues_df.to_csv(issues_csv, index=False)

    metrics_df = pd.DataFrame([dq.metrics]).T.reset_index()
    metrics_df.columns = ["metric", "value"]
    issues_preview = issues_df.head(200)

    html_path = output_dir / "data_quality.html"
    html_path.write_text(
        "<html><head><meta charset='utf-8'><title>Data Quality Report</title></head><body>"
        "<h1>Data Quality Report</h1>"
        f"<p><strong>File:</strong> {file_path}</p>"
        f"<p><strong>Run ID:</strong> {dq.run_id}</p>"
        "<h2>Metrics</h2>"
        f"{metrics_df.to_html(index=False, escape=False)}"
        "<h2>Inconsistencies (preview)</h2>"
        f"{issues_preview.to_html(index=False, escape=False)}"
        "</body></html>",
        encoding="utf-8"
    )


def run_pipeline(file_path):
    logger.info(f"Starting pipeline for {file_path}")
    start_time = datetime.utcnow()
    file_size_bytes = os.path.getsize(file_path) if os.path.exists(file_path) else None
    step_metrics = []
    file_id = None
    df = None
    rows_after_drop = None

    # 0. Ensure schema exists
    if os.getenv("DB_AUTO_INIT", "true").lower() in ("1", "true", "yes"):
        step = start_step("ensure_schema")
        ensure_schema()
        step_metrics.append(finish_step(step))

    # 1. Checksum
    step = start_step("checksum")
    checksum = compute_checksum(file_path)
    step_metrics.append(finish_step(step))
    step = start_step("checksum_check")
    should_skip, existing_file_id = should_skip_by_checksum(checksum)
    step_metrics.append(finish_step(step))
    if should_skip:
        if existing_file_id:
            storage_result = upload_artifacts(existing_file_id, file_path, Path("reports"))
            update_storage_metadata(existing_file_id, storage_result)
        return

    # 2. Load Excel
    try:
        step = start_step("load_excel")
        df = load_excel(file_path)
        step_metrics.append(finish_step(step, row_count=len(df)))
    except Exception as e:
        logger.error(f"Failed to read Excel: {e}")
        sys.exit(1)

    df = df.dropna(how='all', subset=REQUIRED_COLUMNS)
    rows_after_drop = len(df)
    logger.info(f"Loaded {len(df)} rows.")

    # 4. Normalize values
    matcher = GeoMatcher()  # Loads catalog

    step = start_step("normalize_fields")
    df['nombre_norm'] = df['NOMBRE_IES'].apply(lambda v: normalize_value(matcher, v))
    df['carrera_norm'] = df['NOMBRE_CARRERA'].apply(lambda v: normalize_value(matcher, v))
    df['estado_norm'] = df['ESTADO'].apply(lambda v: normalize_value(matcher, v))
    df['campo_amplio_norm'] = df['CAMPO_AMPLIO'].apply(lambda v: normalize_value(matcher, v))
    df['nivel_formacion_norm'] = df['NIVEL_FORMACION'].apply(lambda v: normalize_value(matcher, v))
    df['modalidad_norm'] = df['MODALIDAD'].apply(lambda v: normalize_value(matcher, v))
    step_metrics.append(finish_step(step, row_count=len(df)))

    # Geo Matching
    def match_row(row):
        p, c, score_p, score_c, method = matcher.match_territory(row['PROVINCIA'], row['CANTON'])
        return pd.Series([p, c, score_p, score_c, method])

    step = start_step("geo_match")
    df[['provincia_norm', 'canton_norm', 'geo_score_prov', 'geo_score_canton', 'geo_method']] = df.apply(match_row, axis=1)
    step_metrics.append(finish_step(step, row_count=len(df)))

    # 5. Keys
    step = start_step("keys_hash")
    df['natural_key'] = df.apply(generate_natural_key, axis=1)
    df['row_hash'] = df.apply(generate_row_hash, axis=1)
    df['row_num'] = df.index + 1
    step_metrics.append(finish_step(step, row_count=len(df)))

    # 6. Insert File Record
    file_id = str(uuid.uuid4())
    step = start_step("insert_file_record")
    with get_db_session() as session:
        session.execute(text("""
            INSERT INTO raw_ingest.files (
                file_id, file_name, checksum_sha256, rows_loaded, status, started_at, file_size_bytes
            )
            VALUES (:fid, :fname, :chk, :rows, 'running', :started_at, :file_size_bytes)
        """), {
            "fid": file_id,
            "fname": file_path,
            "chk": checksum,
            "rows": len(df),
            "started_at": start_time,
            "file_size_bytes": file_size_bytes
        })
    step_metrics.append(finish_step(step, row_count=len(df)))

    try:
        # 7. Loading Staging (Bulk)
        # We need to serialize JSONB fields
        df['file_id'] = file_id
        df['ingested_at'] = datetime.now()

        # Prepare staging dataframe
        stg_df = df.copy()
        stg_df['normalized_fields'] = stg_df.apply(lambda r: json.dumps({
            'nombre_norm': r['nombre_norm'],
            'carrera_norm': r['carrera_norm'],
            'estado_norm': r['estado_norm'],
            'campo_amplio_norm': r['campo_amplio_norm'],
            'nivel_formacion_norm': r['nivel_formacion_norm'],
            'modalidad_norm': r['modalidad_norm'],
            'provincia_norm': r['provincia_norm'],
            'canton_norm': r['canton_norm'],
            'geo_method': r['geo_method'],
            'geo_score_prov': r['geo_score_prov'],
            'geo_score_canton': r['geo_score_canton']
        }), axis=1)

        # Map columns to DB columns
        db_cols = {
            'NOMBRE_IES': 'nombre_ies',
            'TIPO_IES': 'tipo_ies',
            'TIPO_FINANCIAMIENTO': 'tipo_financiamiento',
            'NOMBRE_CARRERA': 'nombre_carrera',
            'CAMPO_AMPLIO': 'campo_amplio',
            'NIVEL_FORMACION': 'nivel_formacion',
            'MODALIDAD': 'modalidad',
            'PROVINCIA': 'provincia',
            'CANTON': 'canton',
            'ESTADO': 'estado',
            'row_num': 'row_num'
        }
        stg_df = stg_df.rename(columns=db_cols)

        # Select valid columns for insert
        insert_cols = list(db_cols.values()) + ['file_id', 'ingested_at', 'normalized_fields', 'natural_key', 'row_hash']
        step = start_step("load_staging")
        stg_df[insert_cols].to_sql('stg_oferta', engine, schema='raw_ingest', if_exists='append', index=False)
        step_metrics.append(finish_step(step, row_count=len(stg_df)))
        logger.info("Staging load complete.")

        with get_db_session() as session:
            # 8. Upsert Dims
            # Dim IES
            step = start_step("upsert_dims")
            ies_unique = df[['NOMBRE_IES', 'nombre_norm', 'TIPO_IES', 'TIPO_FINANCIAMIENTO']].drop_duplicates('nombre_norm')
            for _, row in ies_unique.iterrows():
                session.execute(text("""
                    INSERT INTO core.dim_ies (nombre_original, nombre_norm, tipo_ies, tipo_financiamiento)
                    VALUES (:orig, :norm, :ti, :tf)
                    ON CONFLICT (nombre_norm) DO UPDATE
                    SET updated_at = NOW(), tipo_ies = EXCLUDED.tipo_ies, tipo_financiamiento = EXCLUDED.tipo_financiamiento
                """), {"orig": row['NOMBRE_IES'], "norm": row['nombre_norm'], "ti": row['TIPO_IES'], "tf": row['TIPO_FINANCIAMIENTO']})

            # Dim Territory
            geo_unique = df[['PROVINCIA', 'CANTON', 'provincia_norm', 'canton_norm']].drop_duplicates(['provincia_norm', 'canton_norm'])
            for _, row in geo_unique.iterrows():
                if row['provincia_norm'] and row['canton_norm']:
                    session.execute(text("""
                        INSERT INTO core.dim_territory (provincia_original, canton_original, provincia_norm, canton_norm)
                        VALUES (:po, :co, :pn, :cn)
                        ON CONFLICT (provincia_norm, canton_norm) DO NOTHING
                    """), {"po": row['PROVINCIA'], "co": row['CANTON'], "pn": row['provincia_norm'], "cn": row['canton_norm']})

            # Dim Program
            prog_unique = df[['NOMBRE_CARRERA', 'carrera_norm', 'campo_amplio_norm', 'nivel_formacion_norm', 'modalidad_norm']].drop_duplicates(
                ['carrera_norm', 'campo_amplio_norm', 'nivel_formacion_norm', 'modalidad_norm']
            )
            for _, row in prog_unique.iterrows():
                session.execute(text("""
                    INSERT INTO core.dim_program (carrera_original, carrera_norm, campo_amplio, nivel_formacion, modalidad)
                    VALUES (:co, :cn, :ca, :nf, :mo)
                    ON CONFLICT (carrera_norm, campo_amplio, nivel_formacion, modalidad) DO NOTHING
                """), {
                    "co": row['NOMBRE_CARRERA'],
                    "cn": row['carrera_norm'],
                    "ca": row['campo_amplio_norm'],
                    "nf": row['nivel_formacion_norm'],
                    "mo": row['modalidad_norm']
                })

            step_metrics.append(
                finish_step(
                    step,
                    row_count=int(len(ies_unique) + len(geo_unique) + len(prog_unique)),
                    detail={
                        "ies": int(len(ies_unique)),
                        "territories": int(len(geo_unique)),
                        "programs": int(len(prog_unique))
                    }
                )
            )
            logger.info("Dimension upsert complete.")

            # 9. SCD Type 2 for Fact Offer
            step = start_step("scd_fact")
            existing_rows = session.execute(
                text("SELECT natural_key, row_hash FROM core.fact_offer WHERE is_current = TRUE")
            ).fetchall()
            existing_map = {r.natural_key: r.row_hash for r in existing_rows}

            # Prepare lookups for IDs
            ies_lookup = {r.nombre_norm: r.ies_id for r in session.execute(text("SELECT nombre_norm, ies_id FROM core.dim_ies")).fetchall()}
            terr_lookup_rows = session.execute(text("SELECT provincia_norm, canton_norm, territory_id FROM core.dim_territory")).fetchall()
            terr_lookup = {(r.provincia_norm, r.canton_norm): r.territory_id for r in terr_lookup_rows}
            prog_lookup_rows = session.execute(text("SELECT carrera_norm, campo_amplio, nivel_formacion, modalidad, program_id FROM core.dim_program")).fetchall()
            prog_lookup = {(r.carrera_norm, r.campo_amplio, r.nivel_formacion, r.modalidad): r.program_id for r in prog_lookup_rows}

            updates_count = 0
            inserts_count = 0
            unchanged_count = 0
            skipped_missing_dims = 0

            df_fact = df.drop_duplicates(subset=['natural_key'], keep='last')
            for _, row in df_fact.iterrows():
                nk = row['natural_key']
                rh = row['row_hash']

                # Resolve IDs
                ies_id = ies_lookup.get(row['nombre_norm'])
                terr_id = terr_lookup.get((row['provincia_norm'], row['canton_norm']))
                prog_id = prog_lookup.get((
                    row['carrera_norm'],
                    row['campo_amplio_norm'],
                    row['nivel_formacion_norm'],
                    row['modalidad_norm']
                ))

                if not (ies_id and terr_id and prog_id):
                    skipped_missing_dims += 1
                    continue

                if nk in existing_map:
                    if existing_map[nk] != rh:
                        # CHANGED: Expire old, Insert new
                        session.execute(
                            text(
                                "UPDATE core.fact_offer SET is_current = FALSE, last_seen_at = NOW(), last_file_id = :fid "
                                "WHERE natural_key = :nk AND is_current = TRUE"
                            ),
                            {"fid": file_id, "nk": nk}
                        )

                        session.execute(text("""
                            INSERT INTO core.fact_offer (ies_id, territory_id, program_id, estado_original, estado_norm, natural_key, row_hash, last_file_id, is_current)
                            VALUES (:ies, :terr, :prog, :eo, :en, :nk, :rh, :fid, TRUE)
                        """), {
                            "ies": ies_id, "terr": terr_id, "prog": prog_id,
                            "eo": row['ESTADO'], "en": row['estado_norm'],
                            "nk": nk, "rh": rh, "fid": file_id
                        })
                        updates_count += 1
                    else:
                        # UNCHANGED: Update last_seen
                        session.execute(
                            text(
                                "UPDATE core.fact_offer SET last_seen_at = NOW(), last_file_id = :fid "
                                "WHERE natural_key = :nk AND is_current = TRUE"
                            ),
                            {"fid": file_id, "nk": nk}
                        )
                        unchanged_count += 1
                else:
                    # NEW
                    session.execute(text("""
                        INSERT INTO core.fact_offer (ies_id, territory_id, program_id, estado_original, estado_norm, natural_key, row_hash, last_file_id, is_current)
                        VALUES (:ies, :terr, :prog, :eo, :en, :nk, :rh, :fid, TRUE)
                    """), {
                        "ies": ies_id, "terr": terr_id, "prog": prog_id,
                        "eo": row['ESTADO'], "en": row['estado_norm'],
                        "nk": nk, "rh": rh, "fid": file_id
                    })
                    inserts_count += 1

            logger.info(f"SCD Logic: {inserts_count} new, {updates_count} updates, {unchanged_count} unchanged.")
            step_metrics.append(
                finish_step(
                    step,
                    row_count=int(len(df_fact)),
                    detail={
                        "new": inserts_count,
                        "updated": updates_count,
                        "unchanged": unchanged_count,
                        "skipped_missing_dims": skipped_missing_dims
                    }
                )
            )

        # 10. Data Quality Checks + Reports
        extra_metrics = {
            "rows_loaded": len(df),
            "ingest_new": inserts_count,
            "ingest_updated": updates_count,
            "ingest_unchanged": unchanged_count,
            "skipped_missing_dims": skipped_missing_dims
        }
        step = start_step("data_quality")
        dq = DataQualityChecker(file_id, valid_pairs=matcher.valid_pairs, extra_metrics=extra_metrics)
        dq.run_checks(df)
        step_metrics.append(finish_step(step, row_count=len(df), detail={"issues": len(dq.issues)}))

        reports_dir = Path("reports")
        step = start_step("write_reports")
        write_reports(dq, reports_dir, file_id, file_path)
        step_metrics.append(finish_step(step))

        step = start_step("storage_upload")
        storage_result = upload_artifacts(file_id, file_path, reports_dir)
        step_metrics.append(
            finish_step(
                step,
                detail={
                    "status": storage_result.get("status"),
                    "paths": list((storage_result.get("paths") or {}).keys())
                }
            )
        )

        # 11. Update File Status
        summary = (
            f"new={inserts_count}, updated={updates_count}, unchanged={unchanged_count}, "
            f"skipped_missing_dims={skipped_missing_dims}"
        )
        process_metrics = build_process_metrics(
            df,
            step_metrics,
            extra={"rows_after_drop": rows_after_drop}
        )
        write_step_metrics(file_id, step_metrics)
        finished_at = datetime.utcnow()
        duration_seconds = (finished_at - start_time).total_seconds()
        with get_db_session() as session:
            session.execute(
                text(
                    "UPDATE raw_ingest.files SET status = 'success', notes = :notes, "
                    "finished_at = :finished_at, duration_seconds = :duration_seconds, "
                    "ingest_new = :ingest_new, ingest_updated = :ingest_updated, ingest_unchanged = :ingest_unchanged, "
                    "skipped_missing_dims = :skipped_missing_dims, storage_status = :storage_status, "
                    "storage_paths = CAST(:storage_paths AS JSONB), "
                    "process_metrics = CAST(:process_metrics AS JSONB) "
                    "WHERE file_id = :fid"
                ),
                {
                    "fid": file_id,
                    "notes": summary,
                    "finished_at": finished_at,
                    "duration_seconds": duration_seconds,
                    "ingest_new": inserts_count,
                    "ingest_updated": updates_count,
                    "ingest_unchanged": unchanged_count,
                    "skipped_missing_dims": skipped_missing_dims,
                    "storage_status": storage_result.get("status"),
                    "storage_paths": json.dumps(storage_result.get("paths") or {}),
                    "process_metrics": json.dumps(process_metrics)
                }
            )

        logger.info("Pipeline finished successfully.")
        logger.info(f"Summary: {summary}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if file_id:
            process_metrics = build_process_metrics(
                df,
                step_metrics,
                extra={"rows_after_drop": rows_after_drop, "error": str(e)}
            )
            write_step_metrics(file_id, step_metrics)
            finished_at = datetime.utcnow()
            duration_seconds = (finished_at - start_time).total_seconds()
            with get_db_session() as session:
                session.execute(
                    text(
                        "UPDATE raw_ingest.files SET status = 'failed', notes = :err, "
                        "finished_at = :finished_at, duration_seconds = :duration_seconds, "
                        "storage_status = :storage_status, process_metrics = CAST(:process_metrics AS JSONB) "
                        "WHERE file_id = :fid"
                    ),
                    {
                        "fid": file_id,
                        "err": str(e),
                        "finished_at": finished_at,
                        "duration_seconds": duration_seconds,
                        "storage_status": "failed",
                        "process_metrics": json.dumps(process_metrics)
                    }
                )
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Path to Excel file")
    args = parser.parse_args()

    run_pipeline(args.path)
