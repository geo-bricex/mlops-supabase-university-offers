import json
import logging
import uuid
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.session import get_db_session
from src.dq.rules import (
    QUALITY_RULES,
    RULES_BY_ISSUE_TYPE,
    label_contributing_issue_types,
    rule_catalog_payload,
)

logger = logging.getLogger("dq_checks")


def collect_data_quality_issues(
    df: pd.DataFrame,
    valid_pairs: set[tuple[str, str]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Evaluate the canonical quality rules without writing to the database.

    Keeping detection separate from persistence makes the supervised target
    reproducible from the source workbook while preserving the exact rules used
    by the ETL audit.
    """
    metrics: dict[str, Any] = {}
    issues: list[dict[str, Any]] = []
    pairs_catalog = valid_pairs or set()

    def add_issue(issue_type: str, natural_key: str, detail: dict[str, Any]) -> None:
        rule = RULES_BY_ISSUE_TYPE[issue_type]
        issues.append(
            {
                "rule_id": rule.rule_id,
                "issue_type": issue_type,
                "natural_key": natural_key,
                "severity": rule.severity,
                "contributes_to_label": rule.contributes_to_label,
                "rule_version": rule.version,
                "detail": detail,
            }
        )

    duplicates = df[df.duplicated(subset=["natural_key"], keep=False)]
    metrics["duplicates_in_file"] = len(duplicates)
    for index, row in duplicates.iterrows():
        add_issue(
            "duplicate_natural_key",
            row.get("natural_key"),
            {"row_index": int(index), "row_hash": row.get("row_hash")},
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
                "row_index": int(row.name),
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
                    "row_index": int(row.name),
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
                {"column": column, "row_index": int(index)},
            )

    rule_counts = summarize_rule_results(df, issues)
    contributing = set(label_contributing_issue_types())
    positive_groups = {
        str(issue["natural_key"])
        for issue in issues
        if issue["issue_type"] in contributing
    }
    metrics.update(
        {
            "rows_evaluated": len(df),
            "audit_events_total": len(issues),
            "label_positive_groups": len(positive_groups),
            "label_positive_rows": int(
                df["natural_key"].astype(str).isin(positive_groups).sum()
            ),
            "rule_counts": rule_counts,
        }
    )
    return metrics, issues


def summarize_rule_results(
    df: pd.DataFrame,
    issues: list[dict[str, Any]],
) -> dict[str, dict[str, int]]:
    """Separate audit events, directly affected rows, and labeled rows."""
    summary: dict[str, dict[str, int]] = {}
    natural_keys = df["natural_key"].astype(str)
    for rule in QUALITY_RULES:
        rule_issues = [
            issue for issue in issues if issue["issue_type"] == rule.issue_type
        ]
        affected_groups = {str(issue["natural_key"]) for issue in rule_issues}
        if rule.event_granularity == "row":
            affected_rows = {
                int(issue["detail"]["row_index"])
                for issue in rule_issues
                if "row_index" in issue["detail"]
            }
            affected_row_count = len(affected_rows)
        else:
            affected_row_count = int(natural_keys.isin(affected_groups).sum())
        summary[rule.rule_id] = {
            "event_count": len(rule_issues),
            "affected_row_count": int(affected_row_count),
            "affected_group_count": len(affected_groups),
            "label_positive_row_count": (
                int(natural_keys.isin(affected_groups).sum())
                if rule.contributes_to_label
                else 0
            ),
        }
    return summary


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
                rule_id=issue["rule_id"],
                issue_type=issue["issue_type"],
                natural_key=issue["natural_key"],
                severity=issue["severity"],
                contributes_to_label=issue["contributes_to_label"],
                rule_version=issue["rule_version"],
                detail=issue["detail"],
            )

        # Save results
        self.save_results()

    def add_issue(
        self,
        *,
        rule_id,
        issue_type,
        natural_key,
        severity,
        contributes_to_label,
        rule_version,
        detail,
    ):
        self.issues.append(
            {
                "issue_id": str(uuid.uuid4()),
                "run_id": self.run_id,
                "rule_id": rule_id,
                "issue_type": issue_type,
                "natural_key": natural_key,
                "severity": severity,
                "contributes_to_label": contributes_to_label,
                "rule_version": rule_version,
                "detail": detail,
            }
        )

    def save_results(self):
        try:
            with get_db_session() as session:
                for rule in rule_catalog_payload():
                    session.execute(
                        text(
                            """
                            INSERT INTO audit.rule_catalog (
                                rule_id, name, dimension, required_columns,
                                condition, issue_type, severity,
                                contributes_to_label, version, description,
                                event_granularity, updated_at
                            )
                            VALUES (
                                :rule_id, :name, :dimension,
                                CAST(:required_columns AS JSONB), :condition,
                                :issue_type, :severity, :contributes_to_label,
                                :version, :description, :event_granularity,
                                NOW()
                            )
                            ON CONFLICT (rule_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                dimension = EXCLUDED.dimension,
                                required_columns = EXCLUDED.required_columns,
                                condition = EXCLUDED.condition,
                                issue_type = EXCLUDED.issue_type,
                                severity = EXCLUDED.severity,
                                contributes_to_label =
                                    EXCLUDED.contributes_to_label,
                                version = EXCLUDED.version,
                                description = EXCLUDED.description,
                                event_granularity =
                                    EXCLUDED.event_granularity,
                                updated_at = NOW()
                            """
                        ),
                        {
                            **rule,
                            "required_columns": json.dumps(rule["required_columns"]),
                        },
                    )

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

                if self.issues:
                    sql_issue = text("""
                        INSERT INTO audit.inconsistencies (
                            issue_id, run_id, rule_id, issue_type, natural_key,
                            severity, contributes_to_label, rule_version, detail
                        )
                        VALUES (
                            :issue_id, :run_id, :rule_id, :issue_type,
                            :natural_key, :severity, :contributes_to_label,
                            :rule_version, :detail
                        )
                    """)
                    # Bulk insert or loop
                    for issue in self.issues:
                        session.execute(
                            sql_issue,
                            {
                                "issue_id": issue["issue_id"],
                                "run_id": issue["run_id"],
                                "rule_id": issue["rule_id"],
                                "issue_type": issue["issue_type"],
                                "natural_key": issue["natural_key"],
                                "severity": issue["severity"],
                                "contributes_to_label": issue["contributes_to_label"],
                                "rule_version": issue["rule_version"],
                                "detail": json.dumps(issue["detail"]),
                            },
                        )

                for rule_id, counts in self.metrics.get("rule_counts", {}).items():
                    session.execute(
                        text(
                            """
                            INSERT INTO audit.rule_run_counts (
                                rule_run_count_id, run_id, rule_id,
                                event_count, affected_row_count,
                                affected_group_count,
                                label_positive_row_count
                            )
                            VALUES (
                                :rule_run_count_id, :run_id, :rule_id,
                                :event_count, :affected_row_count,
                                :affected_group_count,
                                :label_positive_row_count
                            )
                            ON CONFLICT (run_id, rule_id) DO UPDATE SET
                                event_count = EXCLUDED.event_count,
                                affected_row_count =
                                    EXCLUDED.affected_row_count,
                                affected_group_count =
                                    EXCLUDED.affected_group_count,
                                label_positive_row_count =
                                    EXCLUDED.label_positive_row_count
                            """
                        ),
                        {
                            "rule_run_count_id": str(uuid.uuid4()),
                            "run_id": self.run_id,
                            "rule_id": rule_id,
                            **counts,
                        },
                    )

            logger.info(f"DQ Run {self.run_id} completed. Metrics: {self.metrics}")
        except Exception as e:
            logger.error(f"Failed to save DQ results: {e}")
            raise
