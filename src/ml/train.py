"""Reproducible hyperparameter search, evaluation, and model registration."""

import argparse
import hashlib
import json
import logging
import os
import platform
import subprocess
import time
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from sklearn.model_selection import (
    RandomizedSearchCV,
    cross_val_score,
    train_test_split,
)

from src.db.init_db import ensure_schema
from src.ml.artifacts import (
    ExperimentPaths,
    build_manifest,
    file_sha256,
    json_default,
    portable_path,
    save_search_results,
    save_selected_model,
    write_json,
)
from src.ml.config import (
    CV_FOLDS,
    OPTIMIZATION_METRIC,
    RANDOM_STATE,
    SEARCH_ITERATIONS,
    SEARCH_METHOD,
    TEST_SIZE,
    ModelSearchSpec,
    build_cv,
    build_model_specs,
    search_spaces_for_json,
)
from src.ml.evaluation import (
    classification_metrics_from_scores,
    evaluate_fitted_pipeline,
    save_evaluation_outputs,
)
from src.ml.quality_risk import (
    CATEGORICAL_FEATURES,
    META_COLUMNS,
    NUMERIC_FEATURES,
    build_quality_risk_dataset_from_source,
    feature_schema,
    fetch_quality_risk_dataset,
    latest_success_file_id,
)
from src.ml.registry import ExperimentRegistry
from src.ml.reporting import (
    write_editorial_response,
    write_hyperparameter_report,
)
from src.storage.supabase_storage import upload_model_artifacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("ml_train")

MODEL_NAME = os.getenv("ML_MODEL_NAME", "quality_risk_classifier")
TASK_NAME = "data_quality_risk_classification"
TARGET_NAME = "actual_label"
DEFAULT_THRESHOLD = float(os.getenv("ML_RISK_THRESHOLD", "0.5"))
DEFAULT_ARTIFACT_ROOT = Path(os.getenv("ML_ARTIFACT_DIR", "artifacts/experiments"))
DEFAULT_REPORT_ROOT = Path(os.getenv("ML_REPORT_DIR", "reports/modeling"))


def _dataset_sha256(dataset: pd.DataFrame) -> str:
    """Hash the canonical modeling rows, features, target, and row identity."""
    columns = META_COLUMNS + CATEGORICAL_FEATURES + NUMERIC_FEATURES
    canonical = dataset[columns].copy()
    for column in canonical.columns:
        canonical[column] = canonical[column].apply(
            lambda value: (
                json.dumps(
                    value,
                    sort_keys=True,
                    ensure_ascii=False,
                    default=json_default,
                )
                if isinstance(value, (dict, list))
                else value
            )
        )
    payload = canonical.to_json(
        orient="split",
        index=False,
        force_ascii=False,
        double_precision=15,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _git_environment() -> dict[str, Any]:
    """Capture source provenance without assuming that Git is available."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        commit = "unavailable"
        dirty = True
    return {
        "python_version": platform.python_version(),
        "sklearn_version": sklearn.__version__,
        "git_commit": commit,
        "git_dirty": dirty,
    }


def stratified_holdout_split(
    features: pd.DataFrame,
    target: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """Create the one canonical stratified 80/20 train/test boundary."""
    return train_test_split(
        features,
        target,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=target,
    )


def create_search(
    spec: ModelSearchSpec,
    *,
    n_iter: int = SEARCH_ITERATIONS,
    n_jobs: int = -1,
) -> RandomizedSearchCV:
    """Build an unfitted, single-metric randomized search."""
    return RandomizedSearchCV(
        estimator=spec.pipeline,
        param_distributions=spec.parameter_space,
        n_iter=n_iter,
        scoring=OPTIMIZATION_METRIC,
        n_jobs=n_jobs,
        cv=build_cv(),
        refit=True,
        random_state=RANDOM_STATE,
        return_train_score=True,
        error_score="raise",
    )


def select_model_from_cv(summaries: Iterable[dict[str, Any]]) -> str:
    """Select by CV Average Precision, using CV F1 only for an exact tie."""
    candidates = list(summaries)
    if not candidates:
        raise ValueError("No fitted model summaries were provided.")
    winner = max(
        candidates,
        key=lambda item: (item["cv_mean"], item["cv_f1_mean"]),
    )
    return str(winner["algorithm"])


def _class_distribution(target: pd.Series) -> dict[str, int]:
    counts = target.value_counts().sort_index()
    return {str(int(label)): int(count) for label, count in counts.items()}


def _extract_importances(
    pipeline: Any, run_id: str, top_n: int = 20
) -> list[dict[str, Any]]:
    preprocess = pipeline.named_steps["preprocess"]
    classifier = pipeline.named_steps["model"]
    feature_names = preprocess.get_feature_names_out()
    if hasattr(classifier, "coef_"):
        values = np.asarray(classifier.coef_)[0]
    elif hasattr(classifier, "feature_importances_"):
        values = np.asarray(classifier.feature_importances_)
    else:
        return []
    ranked = sorted(
        zip(feature_names, values),
        key=lambda item: abs(float(item[1])),
        reverse=True,
    )[:top_n]
    return [
        {
            "importance_id": str(uuid.uuid4()),
            "run_id": run_id,
            "feature_name": str(feature_name),
            "importance": float(value),
            "direction": ("positive_risk" if float(value) >= 0 else "negative_risk"),
            "rank": rank,
        }
        for rank, (feature_name, value) in enumerate(ranked, start=1)
    ]


def _prediction_payload(
    *,
    run_id: str,
    file_id: str,
    metadata_frame: pd.DataFrame,
    probabilities: np.ndarray,
    threshold: float,
) -> list[dict[str, Any]]:
    labels = probabilities >= threshold
    rows = []
    for (_, row), probability, label in zip(
        metadata_frame.iterrows(),
        probabilities,
        labels,
    ):
        rows.append(
            {
                "prediction_id": str(uuid.uuid4()),
                "run_id": run_id,
                "file_id": file_id,
                "row_num": int(row["row_num"]),
                "natural_key": str(row["natural_key"]),
                "risk_label": bool(label),
                "risk_probability": float(probability),
                "actual_label": bool(row["actual_label"]),
                "threshold": float(threshold),
                "detail": json.dumps(
                    {
                        "issue_count": int(row["issue_count"]),
                        "issue_types": row["issue_types"],
                    },
                    default=json_default,
                ),
            }
        )
    return rows


def _comparison_frame(models: Iterable[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for model in models:
        metrics = model["test_metrics"]
        rows.append(
            {
                "algorithm": model["algorithm"],
                "model_status": model["model_status"],
                "cv_average_precision_mean": model["cv_mean"],
                "cv_average_precision_std": model["cv_std"],
                "cv_f1_mean": model["cv_f1_mean"],
                "cv_f1_std": model["cv_f1_std"],
                "best_params": json.dumps(
                    model["best_params"],
                    sort_keys=True,
                    default=json_default,
                ),
                "test_accuracy": metrics["accuracy"],
                "test_precision": metrics["precision"],
                "test_recall": metrics["recall"],
                "test_f1": metrics["f1"],
                "test_roc_auc": metrics["roc_auc"],
                "test_average_precision": metrics["average_precision"],
                "confusion_matrix": json.dumps(metrics["confusion_matrix"]),
            }
        )
    return pd.DataFrame(rows)


def _dataset_validation_error(dataset: pd.DataFrame) -> str | None:
    """Return a precise reason when five-fold stratified training is unsafe."""
    if dataset.empty:
        return "The modeling dataset is empty."
    if len(dataset) < 50:
        return "At least 50 rows are required for stable training."
    if dataset[TARGET_NAME].nunique() != 2:
        return "The target must contain exactly two classes."
    minimum_class = int(dataset[TARGET_NAME].value_counts().min())
    if minimum_class < CV_FOLDS:
        return f"Each class needs at least {CV_FOLDS} rows for stratified CV."
    return None


def run_experiment(
    dataset: pd.DataFrame,
    dataset_metadata: dict[str, Any],
    *,
    persist: bool,
    n_iter: int = SEARCH_ITERATIONS,
    n_jobs: int = -1,
    threshold: float = DEFAULT_THRESHOLD,
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    report_root: Path = DEFAULT_REPORT_ROOT,
    docs_root: Path = Path("docs"),
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run the complete train-only search followed by one holdout evaluation."""
    validation_error = _dataset_validation_error(dataset)
    if validation_error:
        raise ValueError(validation_error)

    run_id = run_id or str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)
    model_version = f"{MODEL_NAME}-{created_at.strftime('%Y%m%dT%H%M%SZ')}-{run_id[:8]}"
    paths = ExperimentPaths.create(
        run_id,
        artifact_root=artifact_root,
        report_root=report_root,
    )
    started = time.perf_counter()

    feature_columns = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    features = dataset[feature_columns].copy()
    target = dataset[TARGET_NAME].astype(int)
    X_train, X_test, y_train, y_test = stratified_holdout_split(
        features,
        target,
    )

    environment = _git_environment()
    metadata = {
        **dataset_metadata,
        **environment,
        "run_id": run_id,
        "model_version": model_version,
        "dataset_sha256": _dataset_sha256(dataset),
        "dataset_rows": len(dataset),
        "class_distribution": _class_distribution(target),
        "train_class_distribution": _class_distribution(y_train),
        "test_class_distribution": _class_distribution(y_test),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "test_size": TEST_SIZE,
        "random_state": RANDOM_STATE,
        "search_method": SEARCH_METHOD,
        "optimization_metric": OPTIMIZATION_METRIC,
        "cv_folds": CV_FOLDS,
        "search_iterations": n_iter,
        "n_jobs": n_jobs,
        "search_spaces": search_spaces_for_json(),
    }
    file_id = str(dataset_metadata["dataset_file_id"])
    registry = ExperimentRegistry(enabled=persist)
    registry.start_run(
        run_id=run_id,
        file_id=file_id,
        model_name=MODEL_NAME,
        model_version=model_version,
        task_name=TASK_NAME,
        target_name=TARGET_NAME,
        metadata=metadata,
    )

    try:
        spaces_path = paths.report_dir / f"{run_id}_search_spaces.json"
        write_json(spaces_path, search_spaces_for_json())

        searches: dict[str, RandomizedSearchCV] = {}
        summaries: list[dict[str, Any]] = []
        output_files: list[Path] = [spaces_path]
        specs = build_model_specs(estimator_n_jobs=n_jobs)

        for spec in specs:
            logger.info(
                "Searching %s: n_iter=%s, cv=%s, scoring=%s",
                spec.name,
                n_iter,
                CV_FOLDS,
                OPTIMIZATION_METRIC,
            )
            search = create_search(spec, n_iter=n_iter, n_jobs=n_jobs)
            search.fit(X_train, y_train)
            searches[spec.name] = search

            f1_scores = cross_val_score(
                search.best_estimator_,
                X_train,
                y_train,
                scoring="f1",
                cv=build_cv(),
                n_jobs=n_jobs,
                error_score="raise",
            )
            stored = save_search_results(spec.name, search, paths)
            output_files.extend(Path(path) for path in stored.values())
            summaries.append(
                {
                    "algorithm": spec.name,
                    "best_params": search.best_params_,
                    "best_score": float(search.best_score_),
                    "cv_mean": float(search.best_score_),
                    "cv_std": float(
                        search.cv_results_["std_test_score"][search.best_index_]
                    ),
                    "cv_f1_mean": float(np.mean(f1_scores)),
                    "cv_f1_std": float(np.std(f1_scores, ddof=0)),
                    "search_space": spec.parameter_space,
                    "search_method": SEARCH_METHOD,
                    "optimization_metric": OPTIMIZATION_METRIC,
                    "cv_folds": CV_FOLDS,
                    "search_iterations": n_iter,
                    "artifact_path": stored["best_pipeline"],
                    "cv_results_path": stored["cv_results"],
                }
            )

        # This decision occurs before any access to X_test/y_test below.
        selected_algorithm = select_model_from_cv(summaries)
        logger.info("Selected from training CV only: %s", selected_algorithm)

        model_records: list[dict[str, Any]] = []
        test_evaluations: dict[str, dict[str, Any]] = {}
        for summary in summaries:
            algorithm = summary["algorithm"]
            evaluation = evaluate_fitted_pipeline(
                searches[algorithm].best_estimator_,
                X_test,
                y_test,
                threshold=threshold,
            )
            test_evaluations[algorithm] = evaluation
            curve_paths = save_evaluation_outputs(
                algorithm,
                evaluation,
                paths.report_dir,
            )
            output_files.extend(Path(path) for path in curve_paths.values())
            model_records.append(
                {
                    **summary,
                    "model_status": (
                        "selected" if algorithm == selected_algorithm else "rejected"
                    ),
                    "test_metrics": evaluation["metrics"],
                    "curve_paths": curve_paths,
                }
            )

        selected_record = next(
            row for row in model_records if row["algorithm"] == selected_algorithm
        )
        selected_search = searches[selected_algorithm]
        selected_pipeline_path = Path(selected_record["artifact_path"])
        final_model_path = save_selected_model(
            selected_pipeline_path,
            paths,
        )
        output_files.append(final_model_path)

        comparison_path = paths.report_dir / f"{run_id}_model_comparison.csv"
        _comparison_frame(model_records).to_csv(
            comparison_path,
            index=False,
        )
        output_files.append(comparison_path)

        train_evaluation = evaluate_fitted_pipeline(
            selected_search.best_estimator_,
            X_train,
            y_train,
            threshold=threshold,
        )
        full_scores = pd.Series(index=features.index, dtype=float)
        full_scores.loc[X_train.index] = train_evaluation["scores"]
        full_scores.loc[X_test.index] = test_evaluations[selected_algorithm]["scores"]
        if full_scores.isna().any():
            raise RuntimeError("Not all rows received an out-of-split score.")
        full_score_values = full_scores.to_numpy()
        full_metrics, full_predictions = classification_metrics_from_scores(
            target,
            full_score_values,
            threshold=threshold,
        )
        importances = _extract_importances(
            selected_search.best_estimator_,
            run_id,
        )
        predictions = _prediction_payload(
            run_id=run_id,
            file_id=file_id,
            metadata_frame=dataset[META_COLUMNS],
            probabilities=full_score_values,
            threshold=threshold,
        )
        monitoring_metrics = {
            **full_metrics,
            "prediction_positive_rate": float(np.mean(full_predictions)),
            "actual_positive_rate": float(target.mean()),
            "mean_risk_probability": float(np.mean(full_score_values)),
            "top_10_mean_probability": float(np.mean(np.sort(full_score_values)[-10:])),
        }

        draft_results_path = paths.report_dir / f"{run_id}_results.json"
        persistence_status = "not_requested"
        storage_result: dict[str, Any] = {
            "status": "not_requested",
            "paths": {},
        }
        if persist:
            upload_paths = {path.stem: path for path in output_files if path.exists()}
            storage_result = upload_model_artifacts(
                run_id,
                model_version,
                upload_paths,
            )
            persistence_status = "pending"

        duration_seconds = round(time.perf_counter() - started, 6)
        results: dict[str, Any] = {
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "model_version": model_version,
            "selected_model": selected_algorithm,
            "selection_rule": (
                "Maximum mean cross-validated Average Precision; mean "
                "cross-validated F1 only for an exact tie."
            ),
            "method": {
                "split": "stratified 80/20 train/test",
                "test_size": TEST_SIZE,
                "test_used_for_selection": False,
                "search_method": SEARCH_METHOD,
                "optimization_metric": OPTIMIZATION_METRIC,
                "cv_strategy": "StratifiedKFold",
                "cv_folds": CV_FOLDS,
                "cv_shuffle": True,
                "random_state": RANDOM_STATE,
                "search_iterations": n_iter,
                "refit": True,
                "n_jobs": n_jobs,
                "decision_threshold": threshold,
            },
            "dataset": {
                key: metadata[key]
                for key in [
                    "dataset_file_id",
                    "dataset_sha256",
                    "dataset_rows",
                    "class_distribution",
                    "train_class_distribution",
                    "test_class_distribution",
                    "train_rows",
                    "test_rows",
                ]
            },
            "environment": environment,
            "feature_schema": feature_schema(),
            "models": model_records,
            "selected_train_metrics": train_evaluation["metrics"],
            "selected_monitoring_metrics": monitoring_metrics,
            "persistence": {
                "status": persistence_status,
                "storage": storage_result,
            },
            "paths": {
                "model_dir": portable_path(paths.model_dir),
                "report_dir": portable_path(paths.report_dir),
                "selected_model": portable_path(final_model_path),
                "comparison_csv": portable_path(comparison_path),
                "results_json": portable_path(draft_results_path),
            },
            "duration_seconds": duration_seconds,
        }
        for key in [
            "source_path",
            "source_sha256",
            "quality_metrics",
            "label_rules",
        ]:
            if key in metadata:
                results["dataset"][key] = metadata[key]

        manifest = build_manifest(paths, output_files)
        manifest_path = paths.report_dir / f"{run_id}_artifact_manifest.json"
        write_json(manifest_path, manifest)
        results["paths"]["artifact_manifest"] = portable_path(manifest_path)
        write_json(draft_results_path, results)

        selected_artifact_sha = file_sha256(final_model_path)
        registry.finish_success(
            run_id=run_id,
            model_name=MODEL_NAME,
            selected_algorithm=selected_algorithm,
            selected_summary=selected_record,
            candidate_records=model_records,
            test_metrics=selected_record["test_metrics"],
            train_metrics=train_evaluation["metrics"],
            artifact_path=portable_path(final_model_path),
            artifact_sha256=selected_artifact_sha,
            storage_result=storage_result,
            feature_schema=feature_schema(),
            train_rows=len(X_train),
            test_rows=len(X_test),
            positive_rows=int(target.sum()),
            positive_rate=float(target.mean()),
            duration_seconds=duration_seconds,
            metadata=metadata,
            predictions=predictions,
            importances=importances,
            monitoring_metrics=monitoring_metrics,
        )
        if persist:
            results["persistence"]["status"] = "success"
            write_json(draft_results_path, results)

        write_hyperparameter_report(
            results,
            docs_root / "hyperparameter_selection_report.md",
        )
        write_editorial_response(
            results,
            docs_root / "editorial_response_hyperparameters.md",
        )
        write_json(
            report_root / "latest_run.json",
            {
                "run_id": run_id,
                "selected_model": selected_algorithm,
                "results_json": portable_path(draft_results_path),
                "comparison_csv": portable_path(comparison_path),
            },
        )
        logger.info(
            "Experiment %s completed in %.2f seconds; selected %s",
            run_id,
            duration_seconds,
            selected_algorithm,
        )
        return results
    except Exception as exc:
        duration_seconds = round(time.perf_counter() - started, 6)
        registry.finish_failure(run_id, str(exc), duration_seconds)
        logger.exception("Experiment %s failed", run_id)
        raise


def run_training(
    file_id: str,
    threshold: float = DEFAULT_THRESHOLD,
    *,
    n_iter: int = SEARCH_ITERATIONS,
    n_jobs: int = -1,
) -> str:
    """Compatibility entry point for the Docker/Supabase training service."""
    ensure_schema()
    dataset = fetch_quality_risk_dataset(file_id)
    validation_error = _dataset_validation_error(dataset)
    if validation_error:
        return ExperimentRegistry(enabled=True).record_skipped(
            file_id=file_id,
            model_name=MODEL_NAME,
            task_name=TASK_NAME,
            target_name=TARGET_NAME,
            reason=validation_error,
        )
    results = run_experiment(
        dataset,
        {"dataset_file_id": file_id},
        persist=True,
        n_iter=n_iter,
        n_jobs=n_jobs,
        threshold=threshold,
    )
    return str(results["run_id"])


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run leakage-safe Q1 hyperparameter selection."
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--file-id",
        help="Successful Supabase ingest UUID to train from.",
    )
    source_group.add_argument(
        "--source-path",
        type=Path,
        help=(
            "Rebuild the audited dataset locally from the canonical workbook; "
            "use --no-persist when Supabase is unavailable."
        ),
    )
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path("assets/geo/territory_catalog.csv"),
    )
    parser.add_argument(
        "--n-iter",
        type=int,
        default=SEARCH_ITERATIONS,
    )
    parser.add_argument("--n-jobs", type=int, default=-1)
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Do not write to Supabase or Supabase Storage.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.n_iter < 1:
        raise SystemExit("--n-iter must be at least 1.")
    if not 0.0 < args.threshold < 1.0:
        raise SystemExit("--threshold must be between 0 and 1.")

    if args.source_path:
        dataset, dataset_metadata = build_quality_risk_dataset_from_source(
            args.source_path,
            args.catalog_path,
        )
        persist = not args.no_persist
        if persist:
            raise SystemExit(
                "Local source runs do not have a Supabase file UUID. "
                "Pass --no-persist, or ingest first and use --file-id."
            )
    else:
        ensure_schema()
        file_id = args.file_id or latest_success_file_id()
        if not file_id:
            raise SystemExit(
                "No successful ingest found. Pass --source-path for a local run."
            )
        dataset = fetch_quality_risk_dataset(file_id)
        dataset_metadata = {"dataset_file_id": file_id}
        persist = not args.no_persist

    validation_error = _dataset_validation_error(dataset)
    if validation_error:
        if persist and not args.source_path:
            run_id = ExperimentRegistry(enabled=True).record_skipped(
                file_id=str(dataset_metadata["dataset_file_id"]),
                model_name=MODEL_NAME,
                task_name=TASK_NAME,
                target_name=TARGET_NAME,
                reason=validation_error,
            )
            logger.warning("Training skipped: %s (%s)", validation_error, run_id)
            return
        raise SystemExit(validation_error)

    results = run_experiment(
        dataset,
        dataset_metadata,
        persist=persist,
        n_iter=args.n_iter,
        n_jobs=args.n_jobs,
        threshold=args.threshold,
        artifact_root=args.artifact_root,
        report_root=args.report_root,
    )
    logger.info("Selected model: %s", results["selected_model"])
    logger.info("Results: %s", results["paths"]["results_json"])


if __name__ == "__main__":
    main()
