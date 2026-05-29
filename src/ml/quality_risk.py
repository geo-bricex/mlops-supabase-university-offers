import json
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.db.session import engine

logger = logging.getLogger("ml_quality_risk")

CATEGORICAL_FEATURES = [
    "tipo_ies",
    "tipo_financiamiento",
    "campo_amplio",
    "nivel_formacion",
    "modalidad",
    "estado",
    "provincia_norm",
    "canton_norm",
    "geo_method",
]

NUMERIC_FEATURES = [
    "geo_score_prov",
    "geo_score_canton",
    "has_nombre_ies",
    "has_nombre_carrera",
    "has_provincia_norm",
    "has_canton_norm",
    "ies_name_len",
    "carrera_name_len",
    "natural_key_token_count",
]

META_COLUMNS = [
    "file_id",
    "row_num",
    "natural_key",
    "actual_label",
    "issue_count",
    "issue_types",
]


def latest_success_file_id(engine_to_use=engine) -> Optional[str]:
    query = text(
        """
        SELECT file_id
        FROM raw_ingest.files
        WHERE status = 'success'
        ORDER BY ingested_at DESC
        LIMIT 1
        """
    )
    with engine_to_use.connect() as conn:
        row = conn.execute(query).first()
    return str(row.file_id) if row else None


def _parse_json(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _safe_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip().lower()


def _safe_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str) and not value.strip():
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def prepare_quality_risk_frame(staging_df: pd.DataFrame) -> pd.DataFrame:
    if staging_df.empty:
        return pd.DataFrame(columns=META_COLUMNS + CATEGORICAL_FEATURES + NUMERIC_FEATURES)

    normalized = staging_df["normalized_fields"].apply(_parse_json)
    frame = pd.DataFrame({
        "file_id": staging_df["file_id"].astype(str),
        "row_num": pd.to_numeric(staging_df["row_num"], errors="coerce").fillna(0).astype(int),
        "natural_key": staging_df["natural_key"].fillna("").astype(str),
        "actual_label": staging_df["actual_label"].fillna(False).astype(bool),
        "issue_count": pd.to_numeric(staging_df["issue_count"], errors="coerce").fillna(0).astype(int),
        "issue_types": staging_df["issue_types"].apply(_parse_json),
        "tipo_ies": staging_df["tipo_ies"].apply(_safe_text),
        "tipo_financiamiento": staging_df["tipo_financiamiento"].apply(_safe_text),
        "campo_amplio": staging_df["campo_amplio"].apply(_safe_text),
        "nivel_formacion": staging_df["nivel_formacion"].apply(_safe_text),
        "modalidad": staging_df["modalidad"].apply(_safe_text),
        "estado": staging_df["estado"].apply(_safe_text),
    })

    frame["provincia_norm"] = normalized.apply(lambda payload: _safe_text(payload.get("provincia_norm")))
    frame["canton_norm"] = normalized.apply(lambda payload: _safe_text(payload.get("canton_norm")))
    frame["geo_method"] = normalized.apply(lambda payload: _safe_text(payload.get("geo_method")))
    frame["geo_score_prov"] = normalized.apply(lambda payload: _safe_float(payload.get("geo_score_prov")))
    frame["geo_score_canton"] = normalized.apply(lambda payload: _safe_float(payload.get("geo_score_canton")))

    frame["has_nombre_ies"] = staging_df["nombre_ies"].apply(lambda value: 1 if _safe_text(value) else 0)
    frame["has_nombre_carrera"] = staging_df["nombre_carrera"].apply(lambda value: 1 if _safe_text(value) else 0)
    frame["has_provincia_norm"] = frame["provincia_norm"].apply(lambda value: 1 if value else 0)
    frame["has_canton_norm"] = frame["canton_norm"].apply(lambda value: 1 if value else 0)
    frame["ies_name_len"] = staging_df["nombre_ies"].apply(lambda value: len(_safe_text(value)))
    frame["carrera_name_len"] = staging_df["nombre_carrera"].apply(lambda value: len(_safe_text(value)))
    frame["natural_key_token_count"] = frame["natural_key"].apply(lambda value: len([part for part in value.split("|") if part]))

    for column in CATEGORICAL_FEATURES:
        frame[column] = frame[column].fillna("unknown")
        frame.loc[frame[column] == "", column] = "unknown"

    return frame


def fetch_quality_risk_dataset(file_id: str, engine_to_use=engine) -> pd.DataFrame:
    query = text(
        """
        WITH latest_run AS (
            SELECT run_id
            FROM audit.data_quality_runs
            WHERE file_id = :file_id
            ORDER BY created_at DESC
            LIMIT 1
        ),
        issue_map AS (
            SELECT
                natural_key,
                COUNT(*)::INT AS issue_count,
                to_jsonb(array_agg(DISTINCT issue_type ORDER BY issue_type)) AS issue_types
            FROM audit.inconsistencies
            WHERE run_id = (SELECT run_id FROM latest_run)
            GROUP BY natural_key
        )
        SELECT
            s.file_id,
            s.row_num,
            s.natural_key,
            s.nombre_ies,
            s.tipo_ies,
            s.tipo_financiamiento,
            s.nombre_carrera,
            s.campo_amplio,
            s.nivel_formacion,
            s.modalidad,
            s.provincia,
            s.canton,
            s.estado,
            s.normalized_fields,
            COALESCE(im.issue_count, 0) AS issue_count,
            COALESCE(im.issue_types, '[]'::jsonb) AS issue_types,
            COALESCE(im.issue_count, 0) > 0 AS actual_label
        FROM raw_ingest.stg_oferta s
        LEFT JOIN issue_map im ON s.natural_key = im.natural_key
        WHERE s.file_id = :file_id
        ORDER BY s.row_num
        """
    )
    with engine_to_use.connect() as conn:
        raw_df = pd.read_sql(query, conn, params={"file_id": file_id})
    prepared = prepare_quality_risk_frame(raw_df)
    logger.info("Prepared quality risk dataset with %s rows for file %s", len(prepared), file_id)
    return prepared


def feature_schema() -> dict:
    return {
        "categorical_features": list(CATEGORICAL_FEATURES),
        "numeric_features": list(NUMERIC_FEATURES),
        "meta_columns": list(META_COLUMNS),
        "target_column": "actual_label",
        "task_name": "data_quality_risk_classification",
    }
