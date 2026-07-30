import json
import logging
import uuid
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.session import get_db_session

logger = logging.getLogger("dq_checks")


def collect_data_quality_issues(
    df: pd.DataFrame,
    valid_pairs: set[tuple[str, str]] | None = None,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    """Evaluate the canonical quality rules without writing to the database.

    Keeping detection separate from persistence makes the supervised target
    reproducible from the source workbook while preserving the exact rules used
    by the ETL audit.
    """
    metrics: dict[str, int] = {}
    issues: list[dict[str, Any]] = []
    pairs_catalog = valid_pairs or set()

    def add_issue(issue_type: str, natural_key: str, detail: dict[str, Any]) -> None:
        issues.append(
            {
                "issue_type": issue_type,
                "natural_key": natural_key,
                "detail": detail,
            }
        )

    duplicates = df[df.duplicated(subset=["natural_key"], keep=False)]
    metrics["duplicates_in_file"] = len(duplicates)
    for index, row in duplicates.iterrows():
        add_issue(
            "duplicate_natural_key",
            row.get("natural_key"),
            {"row_index": index, "row_hash": row.get("row_hash")},
        )

    invalid_geo_mask = (
        df["provincia_norm"].isna()
        | (df["provincia_norm"] == "")
        | df["canton_norm"].isna()
        | (df["canton_norm"] == "")
    )
    invalid_geo = df[invalid_geo_mask]
    metrics["invalid_territory"] = len(invalid_geo)
    for _, row in invalid_geo.iterrows():
        add_issue(
            "missing_territory_norm",
            row.get("natural_key"),
            {
                "provincia_original": row.get("PROVINCIA"),
                "canton_original": row.get("CANTON"),
            },
        )

    if pairs_catalog:
        row_pairs = list(zip(df["provincia_norm"], df["canton_norm"]))
        invalid_pair_mask = pd.Series(
            [pair not in pairs_catalog for pair in row_pairs],
            index=df.index,
        )
        invalid_pairs = df[invalid_pair_mask & ~invalid_geo_mask]
        metrics["invalid_territory_pair"] = len(invalid_pairs)
        for _, row in invalid_pairs.iterrows():
            add_issue(
                "invalid_territory_pair",
                row.get("natural_key"),
                {
                    "provincia_norm": row.get("provincia_norm"),
                    "canton_norm": row.get("canton_norm"),
                },
            )
    else:
        metrics["invalid_territory_pair"] = 0

    state_counts = df.groupby("natural_key")["estado_norm"].nunique(dropna=True)
    conflicting = state_counts[state_counts > 1]
    metrics["conflicting_estado"] = int(conflicting.shape[0])
    for natural_key in conflicting.index.tolist():
        states = (
            df.loc[df["natural_key"] == natural_key, "estado_norm"]
            .dropna()
            .unique()
            .tolist()
        )
        add_issue(
            "conflicting_estado",
            natural_key,
            {"states": states},
        )

    for column in ["NOMBRE_IES", "NOMBRE_CARRERA"]:
        missing = df[df[column].isna()]
        metrics[f"missing_{column}"] = len(missing)
        for index, row in missing.iterrows():
            add_issue(
                f"missing_{column.lower()}",
                row.get("natural_key", f"row_{index}"),
                {"column": column},
            )

    return metrics, issues


class DataQualityChecker:
    def __init__(self, file_id: str, valid_pairs=None, extra_metrics=None):
        self.file_id = file_id
        self.run_id = str(uuid.uuid4())
        self.issues = []
        self.metrics = extra_metrics or {}
        self.valid_pairs = valid_pairs or set()

    def run_checks(self, df: pd.DataFrame):
        """
        Run all checks on the dataframe being ingested.
        df should have 'natural_key', 'provincia_norm', 'canton_norm', 'estado_norm', etc.
        """
        logger.info(f"Starting DQ checks for file {self.file_id}")
        detected_metrics, detected_issues = collect_data_quality_issues(
            df,
            self.valid_pairs,
        )
        self.metrics.update(detected_metrics)
        for issue in detected_issues:
            self.add_issue(
                issue_type=issue["issue_type"],
                natural_key=issue["natural_key"],
                detail=issue["detail"],
            )

        # Save results
        self.save_results()

    def add_issue(self, issue_type, natural_key, detail):
        self.issues.append(
            {
                "issue_id": str(uuid.uuid4()),
                "run_id": self.run_id,
                "issue_type": issue_type,
                "natural_key": natural_key,
                "detail": detail,
            }
        )

    def save_results(self):
        try:
            with get_db_session() as session:
                # 1. Create Run Record
                sql_run = text("""
                    INSERT INTO audit.data_quality_runs (run_id, file_id, metrics)
                    VALUES (:run_id, :file_id, :metrics)
                """)
                session.execute(
                    sql_run,
                    {
                        "run_id": self.run_id,
                        "file_id": self.file_id,
                        "metrics": json.dumps(self.metrics),
                    },
                )

                # 2. Insert Inconsistencies
                if self.issues:
                    sql_issue = text("""
                        INSERT INTO audit.inconsistencies (issue_id, run_id, issue_type, natural_key, detail)
                        VALUES (:issue_id, :run_id, :issue_type, :natural_key, :detail)
                    """)
                    # Bulk insert or loop
                    for issue in self.issues:
                        session.execute(
                            sql_issue,
                            {
                                "issue_id": issue["issue_id"],
                                "run_id": issue["run_id"],
                                "issue_type": issue["issue_type"],
                                "natural_key": issue["natural_key"],
                                "detail": json.dumps(issue["detail"]),
                            },
                        )

            logger.info(f"DQ Run {self.run_id} completed. Metrics: {self.metrics}")
        except Exception as e:
            logger.error(f"Failed to save DQ results: {e}")
            raise
