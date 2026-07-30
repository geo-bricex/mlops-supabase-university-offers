import hashlib
import json
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.db.session import engine
from src.dq.checks import collect_data_quality_issues
from src.etl.ingest import (
    REQUIRED_COLUMNS,
    generate_natural_key,
    generate_row_hash,
    load_excel,
    normalize_value,
)
from src.geo.matching import GeoMatcher

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


def latest_success_file_id(engine_to_use=engine) -> str | None:
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
        return pd.DataFrame(
            columns=META_COLUMNS + CATEGORICAL_FEATURES + NUMERIC_FEATURES
        )

    normalized = staging_df["normalized_fields"].apply(_parse_json)
    frame = pd.DataFrame(
        {
            "file_id": staging_df["file_id"].astype(str),
            "row_num": pd.to_numeric(staging_df["row_num"], errors="coerce")
            .fillna(0)
            .astype(int),
            "natural_key": staging_df["natural_key"].fillna("").astype(str),
            "actual_label": staging_df["actual_label"].fillna(False).astype(bool),
            "issue_count": pd.to_numeric(staging_df["issue_count"], errors="coerce")
            .fillna(0)
            .astype(int),
            "issue_types": staging_df["issue_types"].apply(_parse_json),
            "tipo_ies": staging_df["tipo_ies"].apply(_safe_text),
            "tipo_financiamiento": staging_df["tipo_financiamiento"].apply(_safe_text),
            "campo_amplio": staging_df["campo_amplio"].apply(_safe_text),
            "nivel_formacion": staging_df["nivel_formacion"].apply(_safe_text),
            "modalidad": staging_df["modalidad"].apply(_safe_text),
            "estado": staging_df["estado"].apply(_safe_text),
        }
    )

    frame["provincia_norm"] = normalized.apply(
        lambda payload: _safe_text(payload.get("provincia_norm"))
    )
    frame["canton_norm"] = normalized.apply(
        lambda payload: _safe_text(payload.get("canton_norm"))
    )
    frame["geo_method"] = normalized.apply(
        lambda payload: _safe_text(payload.get("geo_method"))
    )
    frame["geo_score_prov"] = normalized.apply(
        lambda payload: _safe_float(payload.get("geo_score_prov"))
    )
    frame["geo_score_canton"] = normalized.apply(
        lambda payload: _safe_float(payload.get("geo_score_canton"))
    )

    frame["has_nombre_ies"] = staging_df["nombre_ies"].apply(
        lambda value: 1 if _safe_text(value) else 0
    )
    frame["has_nombre_carrera"] = staging_df["nombre_carrera"].apply(
        lambda value: 1 if _safe_text(value) else 0
    )
    frame["has_provincia_norm"] = frame["provincia_norm"].apply(
        lambda value: 1 if value else 0
    )
    frame["has_canton_norm"] = frame["canton_norm"].apply(
        lambda value: 1 if value else 0
    )
    frame["ies_name_len"] = staging_df["nombre_ies"].apply(
        lambda value: len(_safe_text(value))
    )
    frame["carrera_name_len"] = staging_df["nombre_carrera"].apply(
        lambda value: len(_safe_text(value))
    )
    frame["natural_key_token_count"] = frame["natural_key"].apply(
        lambda value: len([part for part in value.split("|") if part])
    )

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
    logger.info(
        "Prepared quality risk dataset with %s rows for file %s", len(prepared), file_id
    )
    return prepared


def build_quality_risk_dataset_from_source(
    source_path: Path,
    catalog_path: Path = Path("assets/geo/territory_catalog.csv"),
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Rebuild the audited ML dataset from the source workbook.

    This path is intended for reproducible research runs when Supabase is not
    available. It uses the same normalization, geospatial matching, natural-key,
    and quality-rule functions as the ETL path.
    """
    source_path = source_path.resolve()
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    frame = load_excel(str(source_path))
    frame = frame.dropna(how="all", subset=REQUIRED_COLUMNS).copy()
    matcher = GeoMatcher(str(catalog_path))

    frame["nombre_norm"] = frame["NOMBRE_IES"].apply(
        lambda value: normalize_value(matcher, value)
    )
    frame["carrera_norm"] = frame["NOMBRE_CARRERA"].apply(
        lambda value: normalize_value(matcher, value)
    )
    frame["estado_norm"] = frame["ESTADO"].apply(
        lambda value: normalize_value(matcher, value)
    )
    frame["campo_amplio_norm"] = frame["CAMPO_AMPLIO"].apply(
        lambda value: normalize_value(matcher, value)
    )
    frame["nivel_formacion_norm"] = frame["NIVEL_FORMACION"].apply(
        lambda value: normalize_value(matcher, value)
    )
    frame["modalidad_norm"] = frame["MODALIDAD"].apply(
        lambda value: normalize_value(matcher, value)
    )

    def match_row(row: pd.Series) -> pd.Series:
        province, canton, province_score, canton_score, method = (
            matcher.match_territory(row["PROVINCIA"], row["CANTON"])
        )
        return pd.Series([province, canton, province_score, canton_score, method])

    geo_columns = [
        "provincia_norm",
        "canton_norm",
        "geo_score_prov",
        "geo_score_canton",
        "geo_method",
    ]
    frame[geo_columns] = frame.apply(match_row, axis=1)
    frame["natural_key"] = frame.apply(generate_natural_key, axis=1)
    frame["row_hash"] = frame.apply(generate_row_hash, axis=1)
    frame["row_num"] = frame.index + 1

    quality_metrics, issues = collect_data_quality_issues(
        frame,
        matcher.valid_pairs,
    )
    issues_frame = pd.DataFrame(issues)
    if issues_frame.empty:
        issue_count: dict[str, int] = {}
        issue_types: dict[str, list] = {}
    else:
        issue_count = issues_frame.groupby("natural_key").size().astype(int).to_dict()
        issue_types = (
            issues_frame.groupby("natural_key")["issue_type"]
            .apply(lambda values: sorted(set(values)))
            .to_dict()
        )

    local_file_id = source_sha256[:32]
    staging = pd.DataFrame(
        {
            "file_id": local_file_id,
            "row_num": frame["row_num"],
            "natural_key": frame["natural_key"],
            "nombre_ies": frame["NOMBRE_IES"],
            "tipo_ies": frame["TIPO_IES"],
            "tipo_financiamiento": frame["TIPO_FINANCIAMIENTO"],
            "nombre_carrera": frame["NOMBRE_CARRERA"],
            "campo_amplio": frame["CAMPO_AMPLIO"],
            "nivel_formacion": frame["NIVEL_FORMACION"],
            "modalidad": frame["MODALIDAD"],
            "provincia": frame["PROVINCIA"],
            "canton": frame["CANTON"],
            "estado": frame["ESTADO"],
            "normalized_fields": frame.apply(
                lambda row: {
                    "provincia_norm": row["provincia_norm"],
                    "canton_norm": row["canton_norm"],
                    "geo_method": row["geo_method"],
                    "geo_score_prov": row["geo_score_prov"],
                    "geo_score_canton": row["geo_score_canton"],
                },
                axis=1,
            ),
            "issue_count": frame["natural_key"].map(issue_count).fillna(0).astype(int),
            "issue_types": frame["natural_key"]
            .map(issue_types)
            .apply(lambda value: value if isinstance(value, list) else []),
            "actual_label": frame["natural_key"].isin(issue_count),
        }
    )
    prepared = prepare_quality_risk_frame(staging)
    try:
        portable_source_path = source_path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        portable_source_path = source_path.as_posix()
    metadata: dict[str, object] = {
        "source_path": portable_source_path,
        "source_sha256": source_sha256,
        "dataset_file_id": local_file_id,
        "quality_metrics": quality_metrics,
        "label_rules": [
            "duplicate_natural_key",
            "missing_territory_norm",
            "invalid_territory_pair",
            "conflicting_estado",
            "missing_nombre_ies",
            "missing_nombre_carrera",
        ],
    }
    logger.info(
        "Rebuilt quality-risk dataset with %s rows from %s",
        len(prepared),
        source_path,
    )
    return prepared, metadata


def feature_schema() -> dict:
    return {
        "categorical_features": list(CATEGORICAL_FEATURES),
        "numeric_features": list(NUMERIC_FEATURES),
        "meta_columns": list(META_COLUMNS),
        "target_column": "actual_label",
        "task_name": "data_quality_risk_classification",
    }
