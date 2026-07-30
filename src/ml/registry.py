"""Backward-compatible Supabase/Postgres experiment registry."""

import json
import logging
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from src.db.session import get_db_session
from src.ml.artifacts import json_default

logger = logging.getLogger("ml_registry")


def _json(value: Any) -> str:
    return json.dumps(
        value,
        default=json_default,
        allow_nan=False,
        sort_keys=True,
    )


class ExperimentRegistry:
    """Persist experiment lifecycle data using additive registry fields."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled

    def start_run(
        self,
        *,
        run_id: str,
        file_id: str,
        model_name: str,
        model_version: str,
        task_name: str,
        target_name: str,
        metadata: dict[str, Any],
    ) -> None:
        """Create the parent training record before any fitting begins."""
        if not self.enabled:
            return
        with get_db_session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO mlops.training_runs (
                        run_id, file_id, model_name, model_version, task_name,
                        algorithm, target_name, status, started_at, selected_metric,
                        search_method, optimization_metric, cv_folds,
                        search_iterations, search_spaces, dataset_sha256,
                        dataset_rows, class_distribution, random_state,
                        python_version, sklearn_version, git_commit, run_metadata,
                        model_status
                    )
                    VALUES (
                        :run_id, :file_id, :model_name, :model_version, :task_name,
                        'model_selection', :target_name, 'running', :started_at,
                        :optimization_metric, :search_method, :optimization_metric,
                        :cv_folds, :search_iterations, CAST(:search_spaces AS JSONB),
                        :dataset_sha256, :dataset_rows,
                        CAST(:class_distribution AS JSONB), :random_state,
                        :python_version, :sklearn_version, :git_commit,
                        CAST(:run_metadata AS JSONB), 'candidate'
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "file_id": file_id,
                    "model_name": model_name,
                    "model_version": model_version,
                    "task_name": task_name,
                    "target_name": target_name,
                    "started_at": datetime.now(timezone.utc),
                    "optimization_metric": metadata["optimization_metric"],
                    "search_method": metadata["search_method"],
                    "cv_folds": metadata["cv_folds"],
                    "search_iterations": metadata["search_iterations"],
                    "search_spaces": _json(metadata["search_spaces"]),
                    "dataset_sha256": metadata["dataset_sha256"],
                    "dataset_rows": metadata["dataset_rows"],
                    "class_distribution": _json(metadata["class_distribution"]),
                    "random_state": metadata["random_state"],
                    "python_version": metadata["python_version"],
                    "sklearn_version": metadata["sklearn_version"],
                    "git_commit": metadata["git_commit"],
                    "run_metadata": _json(metadata),
                },
            )

    def record_skipped(
        self,
        *,
        file_id: str,
        model_name: str,
        task_name: str,
        target_name: str,
        reason: str,
    ) -> str:
        """Preserve the legacy behavior for datasets that cannot support CV."""
        if not self.enabled:
            raise RuntimeError("A skipped run requires an enabled registry.")
        run_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        model_version = (
            f"{model_name}-skipped-{now.strftime('%Y%m%dT%H%M%SZ')}-{run_id[:8]}"
        )
        with get_db_session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO mlops.training_runs (
                        run_id, file_id, model_name, model_version, task_name,
                        algorithm, target_name, status, started_at, finished_at,
                        duration_seconds, notes, is_active
                    )
                    VALUES (
                        :run_id, :file_id, :model_name, :model_version,
                        :task_name, 'model_selection', :target_name, 'skipped',
                        :started_at, :finished_at, 0, :notes, FALSE
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "file_id": file_id,
                    "model_name": model_name,
                    "model_version": model_version,
                    "task_name": task_name,
                    "target_name": target_name,
                    "started_at": now,
                    "finished_at": now,
                    "notes": reason,
                },
            )
        logger.warning("Recorded skipped training run %s: %s", run_id, reason)
        return run_id

    def finish_success(
        self,
        *,
        run_id: str,
        model_name: str,
        selected_algorithm: str,
        selected_summary: dict[str, Any],
        candidate_records: Iterable[dict[str, Any]],
        scenario_records: Iterable[dict[str, Any]],
        test_metrics: dict[str, Any],
        operational_test_metrics: dict[str, Any],
        oof_metrics: dict[str, Any],
        artifact_path: str,
        artifact_sha256: str,
        storage_result: dict[str, Any],
        feature_schema: dict[str, Any],
        train_rows: int,
        test_rows: int,
        positive_rows: int,
        positive_rate: float,
        duration_seconds: float,
        operational_threshold: float,
        threshold_policy: str,
        metadata: dict[str, Any],
        predictions: Iterable[dict[str, Any]] | None = None,
        importances: Iterable[dict[str, Any]] | None = None,
        monitoring_metrics: dict[str, Any] | None = None,
    ) -> None:
        """Finalize the selected model and all per-algorithm candidate rows."""
        if not self.enabled:
            return
        candidates = list(candidate_records)
        scenarios = list(scenario_records)
        dashboard_candidates = {
            "candidates": [
                {
                    "name": row["algorithm"],
                    "mean_average_precision": row["cv_mean"],
                    "std_average_precision": row["cv_std"],
                    "mean_f1": row["cv_f1_mean"],
                    "std_f1": row["cv_f1_std"],
                    "model_status": row["model_status"],
                    "best_params": row["best_params"],
                }
                for row in candidates
            ]
        }
        cv_metrics = {
            "name": selected_algorithm,
            "mean_average_precision": selected_summary["cv_mean"],
            "std_average_precision": selected_summary["cv_std"],
            "mean_f1": selected_summary["cv_f1_mean"],
            "std_f1": selected_summary["cv_f1_std"],
        }

        with get_db_session() as session:
            session.execute(
                text(
                    """
                    UPDATE mlops.training_runs
                    SET is_active = FALSE
                    WHERE model_name = :model_name AND run_id <> :run_id
                    """
                ),
                {"model_name": model_name, "run_id": run_id},
            )
            session.execute(
                text(
                    """
                    UPDATE mlops.training_runs
                    SET
                        status = 'success',
                        finished_at = :finished_at,
                        duration_seconds = :duration_seconds,
                        train_rows = :train_rows,
                        test_rows = :test_rows,
                        positive_rows = :positive_rows,
                        positive_rate = :positive_rate,
                        algorithm = :algorithm,
                        best_params = CAST(:best_params AS JSONB),
                        best_score = :best_score,
                        train_metrics = CAST(:train_metrics AS JSONB),
                        oof_metrics = CAST(:oof_metrics AS JSONB),
                        cv_metrics = CAST(:cv_metrics AS JSONB),
                        candidate_metrics = CAST(:candidate_metrics AS JSONB),
                        metrics = CAST(:metrics AS JSONB),
                        operational_metrics = CAST(
                            :operational_metrics AS JSONB
                        ),
                        operational_threshold = :operational_threshold,
                        threshold_policy = :threshold_policy,
                        primary_scenario = :primary_scenario,
                        scenario_results = CAST(:scenario_results AS JSONB),
                        artifact_path = :artifact_path,
                        artifact_sha256 = :artifact_sha256,
                        storage_status = :storage_status,
                        storage_paths = CAST(:storage_paths AS JSONB),
                        feature_schema = CAST(:feature_schema AS JSONB),
                        run_metadata = CAST(:run_metadata AS JSONB),
                        notes = :notes,
                        model_status = 'selected',
                        is_active = TRUE
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "finished_at": datetime.now(timezone.utc),
                    "duration_seconds": duration_seconds,
                    "train_rows": train_rows,
                    "test_rows": test_rows,
                    "positive_rows": positive_rows,
                    "positive_rate": positive_rate,
                    "algorithm": selected_algorithm,
                    "best_params": _json(selected_summary["best_params"]),
                    "best_score": selected_summary["best_score"],
                    "train_metrics": _json(oof_metrics),
                    "oof_metrics": _json(oof_metrics),
                    "cv_metrics": _json(cv_metrics),
                    "candidate_metrics": _json(dashboard_candidates),
                    "metrics": _json(test_metrics),
                    "operational_metrics": _json(operational_test_metrics),
                    "operational_threshold": operational_threshold,
                    "threshold_policy": threshold_policy,
                    "primary_scenario": metadata["primary_scenario"],
                    "scenario_results": _json(scenarios),
                    "artifact_path": artifact_path,
                    "artifact_sha256": artifact_sha256,
                    "storage_status": storage_result.get("status"),
                    "storage_paths": _json(storage_result.get("paths") or {}),
                    "feature_schema": _json(feature_schema),
                    "run_metadata": _json(metadata),
                    "notes": (
                        f"Selected {selected_algorithm} in the "
                        f"{metadata['primary_scenario']} scenario exclusively "
                        "from grouped training-data cross-validation."
                    ),
                },
            )

            for candidate in candidates:
                session.execute(
                    text(
                        """
                        INSERT INTO mlops.model_candidates (
                            candidate_id, run_id, algorithm, model_status,
                            search_method, optimization_metric, cv_folds,
                            search_iterations, search_space, best_params,
                            best_score, cv_mean, cv_std, cv_f1_mean, cv_f1_std,
                            oof_metrics, test_metrics,
                            operational_test_metrics, confusion_matrix,
                            artifact_path, cv_results_path, scenario
                        )
                        VALUES (
                            :candidate_id, :run_id, :algorithm, :model_status,
                            :search_method, :optimization_metric, :cv_folds,
                            :search_iterations, CAST(:search_space AS JSONB),
                            CAST(:best_params AS JSONB), :best_score, :cv_mean,
                            :cv_std, :cv_f1_mean, :cv_f1_std,
                            CAST(:oof_metrics AS JSONB),
                            CAST(:test_metrics AS JSONB),
                            CAST(:operational_test_metrics AS JSONB),
                            CAST(:confusion_matrix AS JSONB), :artifact_path,
                            :cv_results_path, :scenario
                        )
                        ON CONFLICT (run_id, algorithm) DO UPDATE SET
                            model_status = EXCLUDED.model_status,
                            best_params = EXCLUDED.best_params,
                            best_score = EXCLUDED.best_score,
                            cv_mean = EXCLUDED.cv_mean,
                            cv_std = EXCLUDED.cv_std,
                            cv_f1_mean = EXCLUDED.cv_f1_mean,
                            cv_f1_std = EXCLUDED.cv_f1_std,
                            oof_metrics = EXCLUDED.oof_metrics,
                            test_metrics = EXCLUDED.test_metrics,
                            operational_test_metrics =
                                EXCLUDED.operational_test_metrics,
                            confusion_matrix = EXCLUDED.confusion_matrix,
                            artifact_path = EXCLUDED.artifact_path,
                            cv_results_path = EXCLUDED.cv_results_path
                        """
                    ),
                    {
                        **candidate,
                        "candidate_id": str(uuid.uuid4()),
                        "run_id": run_id,
                        "search_space": _json(candidate["search_space"]),
                        "best_params": _json(candidate["best_params"]),
                        "test_metrics": _json(candidate["test_metrics"]),
                        "oof_metrics": _json(candidate["oof_metrics_at_0_5"]),
                        "operational_test_metrics": _json(
                            candidate.get("test_metrics_operational")
                        ),
                        "confusion_matrix": _json(
                            candidate["test_metrics"]["confusion_matrix"]
                        ),
                    },
                )

            for scenario_record in scenarios:
                for model in scenario_record["models"]:
                    session.execute(
                        text(
                            """
                            INSERT INTO mlops.scenario_evaluations (
                                scenario_evaluation_id, run_id, scenario,
                                scenario_role, algorithm, model_status,
                                categorical_encoding, included_features,
                                excluded_features, best_params, cv_mean, cv_std,
                                cv_f1_mean, test_metrics_at_0_5,
                                operational_test_metrics, duration_seconds
                            )
                            VALUES (
                                :scenario_evaluation_id, :run_id, :scenario,
                                :scenario_role, :algorithm, :model_status,
                                :categorical_encoding,
                                CAST(:included_features AS JSONB),
                                CAST(:excluded_features AS JSONB),
                                CAST(:best_params AS JSONB), :cv_mean, :cv_std,
                                :cv_f1_mean,
                                CAST(:test_metrics_at_0_5 AS JSONB),
                                CAST(:operational_test_metrics AS JSONB),
                                :duration_seconds
                            )
                            ON CONFLICT (run_id, scenario, algorithm)
                            DO UPDATE SET
                                model_status = EXCLUDED.model_status,
                                best_params = EXCLUDED.best_params,
                                cv_mean = EXCLUDED.cv_mean,
                                cv_std = EXCLUDED.cv_std,
                                cv_f1_mean = EXCLUDED.cv_f1_mean,
                                test_metrics_at_0_5 =
                                    EXCLUDED.test_metrics_at_0_5,
                                operational_test_metrics =
                                    EXCLUDED.operational_test_metrics,
                                duration_seconds = EXCLUDED.duration_seconds
                            """
                        ),
                        {
                            "scenario_evaluation_id": str(uuid.uuid4()),
                            "run_id": run_id,
                            "scenario": scenario_record["name"],
                            "scenario_role": scenario_record["role"],
                            "algorithm": model["algorithm"],
                            "model_status": model["model_status"],
                            "categorical_encoding": model["categorical_encoding"],
                            "included_features": _json(
                                scenario_record["feature_definition"][
                                    "included_features"
                                ]
                            ),
                            "excluded_features": _json(
                                scenario_record["feature_definition"][
                                    "excluded_features"
                                ]
                            ),
                            "best_params": _json(model["best_params"]),
                            "cv_mean": model["cv_mean"],
                            "cv_std": model["cv_std"],
                            "cv_f1_mean": model["cv_f1_mean"],
                            "test_metrics_at_0_5": _json(model["test_metrics_at_0_5"]),
                            "operational_test_metrics": _json(
                                model.get("test_metrics_operational")
                            ),
                            "duration_seconds": scenario_record["duration_seconds"],
                        },
                    )

            importance_rows = list(importances or [])
            if importance_rows:
                session.execute(
                    text(
                        """
                        INSERT INTO mlops.feature_importance (
                            importance_id, run_id, feature_name, importance,
                            direction, rank
                        )
                        VALUES (
                            :importance_id, :run_id, :feature_name, :importance,
                            :direction, :rank
                        )
                        """
                    ),
                    importance_rows,
                )

            prediction_rows = list(predictions or [])
            if prediction_rows:
                session.execute(
                    text(
                        """
                        INSERT INTO mlops.predictions (
                            prediction_id, run_id, file_id, row_num, natural_key,
                            risk_label, risk_probability, actual_label, threshold,
                            prediction_origin, scenario, fold_id, detail
                        )
                        VALUES (
                            :prediction_id, :run_id, :file_id, :row_num,
                            :natural_key, :risk_label, :risk_probability,
                            :actual_label, :threshold, :prediction_origin,
                            :scenario, :fold_id, CAST(:detail AS JSONB)
                        )
                        """
                    ),
                    prediction_rows,
                )

            if monitoring_metrics:
                session.execute(
                    text(
                        """
                        INSERT INTO mlops.monitoring_runs (
                            monitor_id, run_id, file_id, metrics
                        )
                        VALUES (
                            :monitor_id, :run_id, :file_id,
                            CAST(:metrics AS JSONB)
                        )
                        """
                    ),
                    {
                        "monitor_id": str(uuid.uuid4()),
                        "run_id": run_id,
                        "file_id": metadata["dataset_file_id"],
                        "metrics": _json(monitoring_metrics),
                    },
                )

    def finish_failure(
        self,
        run_id: str,
        message: str,
        duration_seconds: float,
    ) -> None:
        """Mark a started registry row as failed."""
        if not self.enabled:
            return
        with get_db_session() as session:
            session.execute(
                text(
                    """
                    UPDATE mlops.training_runs
                    SET status = 'failed', finished_at = :finished_at,
                        duration_seconds = :duration_seconds, notes = :notes
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "finished_at": datetime.now(timezone.utc),
                    "duration_seconds": duration_seconds,
                    "notes": message,
                },
            )
