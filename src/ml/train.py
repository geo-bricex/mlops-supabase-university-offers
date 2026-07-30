"""Grouped, leakage-controlled model selection and experiment traceability."""

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
from sklearn.base import clone
from sklearn.model_selection import RandomizedSearchCV

from src.db.init_db import ensure_schema
from src.ml.ablation import compare_categorical_encodings
from src.ml.artifacts import (
    ExperimentPaths,
    build_manifest,
    file_sha256,
    json_default,
    portable_path,
    save_fitted_pipeline,
    save_search_results,
    save_selected_model,
    write_json,
)
from src.ml.config import (
    CV_FOLDS,
    OPTIMIZATION_METRIC,
    PRIMARY_ENCODING,
    RANDOM_STATE,
    REFERENCE_THRESHOLD,
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
from src.ml.group_validation import (
    GroupedHoldout,
    generate_grouped_oof_probabilities,
    grouped_fold_assignments,
    grouped_holdout_indices,
    select_f2_threshold,
)
from src.ml.quality_risk import (
    FULL_CATEGORICAL_FEATURES,
    FULL_NUMERIC_FEATURES,
    META_COLUMNS,
    PRIMARY_SCENARIO,
    SENSITIVITY_SCENARIO,
    build_quality_risk_dataset_from_source,
    feature_schema,
    fetch_quality_risk_dataset,
    fetch_quality_risk_metadata,
    latest_success_file_id,
    scenario_definition,
)
from src.ml.registry import ExperimentRegistry
from src.ml.reporting import (
    write_editorial_response,
    write_feature_ablation_report,
    write_hyperparameter_report,
    write_rule_catalog,
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
DEFAULT_ARTIFACT_ROOT = Path(os.getenv("ML_ARTIFACT_DIR", "artifacts/experiments"))
DEFAULT_REPORT_ROOT = Path(os.getenv("ML_REPORT_DIR", "reports/modeling"))
SCENARIO_NAMES = (PRIMARY_SCENARIO, SENSITIVITY_SCENARIO)


def _dataset_sha256(dataset: pd.DataFrame) -> str:
    """Hash all modeled features, target values, and row identities."""
    columns = META_COLUMNS + FULL_CATEGORICAL_FEATURES + FULL_NUMERIC_FEATURES
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
    """Capture source and runtime provenance without assuming Git is present."""
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
        commit = os.getenv("GIT_COMMIT", "unavailable")
        dirty = os.getenv("GIT_DIRTY", "unknown") != "false"
    return {
        "python_version": platform.python_version(),
        "sklearn_version": sklearn.__version__,
        "git_commit": commit,
        "git_dirty": dirty,
        "operating_system": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unavailable",
        "logical_cpu_count": os.cpu_count(),
    }


def grouped_holdout_split(
    features: pd.DataFrame,
    target: pd.Series,
    groups: pd.Series,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.Series,
    pd.Series,
    pd.Series,
    pd.Series,
    GroupedHoldout,
]:
    """Create the canonical approximately 80/20 group-disjoint boundary."""
    boundary = grouped_holdout_indices(features, target, groups)
    return (
        features.loc[boundary.train_index],
        features.loc[boundary.test_index],
        target.loc[boundary.train_index],
        target.loc[boundary.test_index],
        groups.loc[boundary.train_index],
        groups.loc[boundary.test_index],
        boundary,
    )


def create_search(
    spec: ModelSearchSpec,
    *,
    n_iter: int = SEARCH_ITERATIONS,
    n_jobs: int = -1,
) -> RandomizedSearchCV:
    """Build an unfitted grouped randomized search."""
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
    """Select by CV AP, using grouped OOF F1 only for an exact tie."""
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


def _sha256_text_series(values: pd.Series) -> str:
    payload = "\n".join(values.astype(str).tolist())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _score_sha256(frame: pd.DataFrame) -> str:
    canonical = frame.sort_values(
        ["prediction_origin", "row_num"],
    ).to_csv(index=False, float_format="%.17g")
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _extract_importances(
    pipeline: Any,
    run_id: str,
    top_n: int = 20,
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
    prediction_origin: str,
    scenario: str,
    fold_ids: pd.Series | None = None,
) -> list[dict[str, Any]]:
    labels = probabilities >= threshold
    rows = []
    for position, ((_, row), probability, label) in enumerate(
        zip(metadata_frame.iterrows(), probabilities, labels)
    ):
        fold_id = int(fold_ids.iloc[position]) if fold_ids is not None else None
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
                "prediction_origin": prediction_origin,
                "scenario": scenario,
                "fold_id": fold_id,
                "detail": json.dumps(
                    {
                        "issue_count": int(row["issue_count"]),
                        "issue_types": row["issue_types"],
                        "evaluation_eligible": prediction_origin
                        in {"oof_train", "sealed_test"},
                    },
                    default=json_default,
                ),
            }
        )
    return rows


def _comparison_frame(
    scenario_records: Iterable[dict[str, Any]],
) -> pd.DataFrame:
    rows = []
    for scenario in scenario_records:
        for model in scenario["models"]:
            reference = model["test_metrics_at_0_5"]
            operational = model.get("test_metrics_operational")
            rows.append(
                {
                    "scenario": scenario["name"],
                    "scenario_role": scenario["role"],
                    "categorical_encoding": model["categorical_encoding"],
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
                    "test_accuracy_at_0_5": reference["accuracy"],
                    "test_precision_at_0_5": reference["precision"],
                    "test_recall_at_0_5": reference["recall"],
                    "test_f1_at_0_5": reference["f1"],
                    "test_roc_auc": reference["roc_auc"],
                    "test_average_precision": reference["average_precision"],
                    "operational_threshold": (
                        operational["threshold"] if operational else None
                    ),
                    "test_precision_operational": (
                        operational["precision"] if operational else None
                    ),
                    "test_recall_operational": (
                        operational["recall"] if operational else None
                    ),
                    "test_f1_operational": (operational["f1"] if operational else None),
                    "confusion_matrix_at_0_5": json.dumps(
                        reference["confusion_matrix"]
                    ),
                }
            )
    return pd.DataFrame(rows)


def _scenario_comparison_frame(
    scenarios: Iterable[dict[str, Any]],
) -> pd.DataFrame:
    rows = []
    for scenario in scenarios:
        winner = next(
            model
            for model in scenario["models"]
            if model["algorithm"] == scenario["selected_model"]
        )
        reference = winner["test_metrics_at_0_5"]
        operational = winner["test_metrics_operational"]
        rows.append(
            {
                "scenario": scenario["name"],
                "role": scenario["role"],
                "included_features": json.dumps(
                    scenario["feature_definition"]["included_features"]
                ),
                "excluded_features": json.dumps(
                    scenario["feature_definition"]["excluded_features"],
                    sort_keys=True,
                ),
                "selected_model": scenario["selected_model"],
                "best_params": json.dumps(
                    winner["best_params"],
                    sort_keys=True,
                    default=json_default,
                ),
                "cv_average_precision_mean": winner["cv_mean"],
                "cv_average_precision_std": winner["cv_std"],
                "cv_f1_mean": winner["cv_f1_mean"],
                "test_average_precision": reference["average_precision"],
                "test_roc_auc": reference["roc_auc"],
                "test_precision_at_0_5": reference["precision"],
                "test_recall_at_0_5": reference["recall"],
                "test_f1_at_0_5": reference["f1"],
                "operational_threshold": operational["threshold"],
                "test_precision_operational": operational["precision"],
                "test_recall_operational": operational["recall"],
                "test_f1_operational": operational["f1"],
                "duration_seconds": scenario["duration_seconds"],
            }
        )
    return pd.DataFrame(rows)


def _dataset_validation_error(dataset: pd.DataFrame) -> str | None:
    """Return why grouped five-fold training would be unsafe."""
    if dataset.empty:
        return "The modeling dataset is empty."
    if len(dataset) < 50:
        return "At least 50 rows are required for stable training."
    if dataset[TARGET_NAME].nunique() != 2:
        return "The target must contain exactly two classes."
    group_class_counts = (
        dataset[["natural_key", TARGET_NAME]]
        .drop_duplicates()
        .groupby("natural_key")[TARGET_NAME]
        .nunique()
    )
    if int(group_class_counts.max()) != 1:
        return "Each natural_key must map to exactly one operational label."
    unique_groups_by_class = (
        dataset[["natural_key", TARGET_NAME]]
        .drop_duplicates()["actual_label"]
        .value_counts()
    )
    if int(unique_groups_by_class.min()) < CV_FOLDS + 1:
        return (
            f"Each class needs at least {CV_FOLDS + 1} groups for grouped "
            "holdout plus five-fold CV."
        )
    return None


def _public_model_record(record: dict[str, Any]) -> dict[str, Any]:
    excluded = {"_search", "_oof", "_test_evaluation"}
    return {key: value for key, value in record.items() if key not in excluded}


def _run_scenario(
    *,
    scenario: str,
    dataset: pd.DataFrame,
    boundary: GroupedHoldout,
    paths: ExperimentPaths,
    n_iter: int,
    n_jobs: int,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[Path]]:
    definition = scenario_definition(scenario)
    feature_columns = definition["included_features"]
    features = dataset[feature_columns].copy()
    target = dataset[TARGET_NAME].astype(int)
    groups = dataset["natural_key"].astype(str)
    X_train = features.loc[boundary.train_index]
    X_test = features.loc[boundary.test_index]
    y_train = target.loc[boundary.train_index]
    y_test = target.loc[boundary.test_index]
    groups_train = groups.loc[boundary.train_index]

    started = time.perf_counter()
    records: list[dict[str, Any]] = []
    internals: dict[str, dict[str, Any]] = {}
    output_files: list[Path] = []
    specs = build_model_specs(
        estimator_n_jobs=n_jobs,
        scenario=scenario,
        encoding_strategy=PRIMARY_ENCODING,
    )
    for spec in specs:
        logger.info(
            "Searching scenario=%s model=%s n_iter=%s cv=%s scoring=%s",
            scenario,
            spec.name,
            n_iter,
            CV_FOLDS,
            OPTIMIZATION_METRIC,
        )
        search_started = time.perf_counter()
        search = create_search(spec, n_iter=n_iter, n_jobs=n_jobs)
        search.fit(X_train, y_train, groups=groups_train)
        search_duration = time.perf_counter() - search_started
        oof = generate_grouped_oof_probabilities(
            search.best_estimator_,
            X_train,
            y_train,
            groups_train,
            n_jobs=n_jobs,
            threshold=REFERENCE_THRESHOLD,
        )
        stored = save_search_results(scenario, spec.name, search, paths)
        output_files.extend(Path(path) for path in stored.values())
        fold_metrics_path = (
            paths.report_dir
            / f"{paths.run_id}_{scenario}_{spec.name.lower()}_oof_folds.csv"
        )
        pd.DataFrame(oof["fold_metrics"]).to_csv(
            fold_metrics_path,
            index=False,
        )
        output_files.append(fold_metrics_path)
        record = {
            "scenario": scenario,
            "algorithm": spec.name,
            "categorical_encoding": PRIMARY_ENCODING,
            "best_params": search.best_params_,
            "best_score": float(search.best_score_),
            "cv_mean": float(search.best_score_),
            "cv_std": float(search.cv_results_["std_test_score"][search.best_index_]),
            "cv_f1_mean": oof["f1_mean"],
            "cv_f1_std": oof["f1_std"],
            "oof_metrics_at_0_5": oof["metrics"],
            "oof_fold_metrics": oof["fold_metrics"],
            "oof_probability_sha256": hashlib.sha256(
                np.asarray(oof["scores"], dtype="<f8").tobytes()
            ).hexdigest(),
            "search_space": spec.parameter_space,
            "search_method": SEARCH_METHOD,
            "optimization_metric": OPTIMIZATION_METRIC,
            "cv_folds": CV_FOLDS,
            "search_iterations": n_iter,
            "search_duration_seconds": float(search_duration),
            "oof_duration_seconds": oof["duration_seconds"],
            "best_mean_fit_time": float(
                search.cv_results_["mean_fit_time"][search.best_index_]
            ),
            "best_std_fit_time": float(
                search.cv_results_["std_fit_time"][search.best_index_]
            ),
            "best_rank": int(search.cv_results_["rank_test_score"][search.best_index_]),
            "artifact_path": stored["best_pipeline"],
            "cv_results_path": stored["cv_results"],
            "oof_fold_metrics_path": portable_path(fold_metrics_path),
            "_search": search,
            "_oof": oof,
        }
        records.append(record)
        internals[spec.name] = record

    selected_algorithm = select_model_from_cv(records)
    selected_oof = internals[selected_algorithm]["_oof"]
    threshold_selection = select_f2_threshold(
        y_train,
        selected_oof["scores"],
    )
    operational_threshold = threshold_selection["selected_threshold"]

    # The winner and its threshold are fixed before this test loop starts.
    for record in records:
        algorithm = record["algorithm"]
        evaluation = evaluate_fitted_pipeline(
            internals[algorithm]["_search"].best_estimator_,
            X_test,
            y_test,
            threshold=REFERENCE_THRESHOLD,
        )
        curve_paths = save_evaluation_outputs(
            scenario,
            algorithm,
            evaluation,
            paths.report_dir,
        )
        output_files.extend(Path(path) for path in curve_paths.values())
        record["model_status"] = (
            "selected" if algorithm == selected_algorithm else "rejected"
        )
        record["test_metrics_at_0_5"] = evaluation["metrics"]
        record["test_metrics"] = evaluation["metrics"]
        record["curve_paths"] = curve_paths
        record["sealed_test_probability_sha256"] = hashlib.sha256(
            np.asarray(evaluation["scores"], dtype="<f8").tobytes()
        ).hexdigest()
        if algorithm == selected_algorithm:
            operational_metrics, _ = classification_metrics_from_scores(
                y_test,
                evaluation["scores"],
                threshold=operational_threshold,
            )
            record["test_metrics_operational"] = operational_metrics
        else:
            record["test_metrics_operational"] = None
        record["_test_evaluation"] = evaluation

    return (
        {
            "name": scenario,
            "role": definition["role"],
            "interpretation": definition["interpretation"],
            "feature_definition": definition,
            "categorical_encoding": PRIMARY_ENCODING,
            "selected_model": selected_algorithm,
            "selection_rule": (
                "Maximum mean grouped-CV Average Precision; mean grouped-OOF "
                "F1 at 0.5 only for an exact tie."
            ),
            "threshold_selection": threshold_selection,
            "models": [_public_model_record(record) for record in records],
            "duration_seconds": float(time.perf_counter() - started),
        },
        internals,
        output_files,
    )


def run_experiment(
    dataset: pd.DataFrame,
    dataset_metadata: dict[str, Any],
    *,
    persist: bool,
    n_iter: int = SEARCH_ITERATIONS,
    n_jobs: int = -1,
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    report_root: Path = DEFAULT_REPORT_ROOT,
    docs_root: Path = Path("docs"),
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run grouped primary and sensitivity analyses plus final persistence."""
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
    target = dataset[TARGET_NAME].astype(int)
    groups = dataset["natural_key"].astype(str)
    primary_definition = scenario_definition(PRIMARY_SCENARIO)
    split_features = dataset[primary_definition["included_features"]]
    boundary = grouped_holdout_indices(split_features, target, groups)
    train_groups = groups.loc[boundary.train_index]
    test_groups = groups.loc[boundary.test_index]

    environment = _git_environment()
    metadata = {
        **dataset_metadata,
        **environment,
        "run_id": run_id,
        "model_version": model_version,
        "dataset_sha256": _dataset_sha256(dataset),
        "dataset_rows": len(dataset),
        "dataset_groups": int(groups.nunique()),
        "class_distribution": _class_distribution(target),
        "train_class_distribution": _class_distribution(
            target.loc[boundary.train_index]
        ),
        "test_class_distribution": _class_distribution(target.loc[boundary.test_index]),
        "train_rows": len(boundary.train_index),
        "test_rows": len(boundary.test_index),
        "train_groups": int(train_groups.nunique()),
        "test_groups": int(test_groups.nunique()),
        "group_overlap_count": len(set(train_groups) & set(test_groups)),
        "row_test_fraction": boundary.row_test_fraction,
        "group_test_fraction": boundary.group_test_fraction,
        "test_size": TEST_SIZE,
        "random_state": RANDOM_STATE,
        "search_method": SEARCH_METHOD,
        "optimization_metric": OPTIMIZATION_METRIC,
        "cv_folds": CV_FOLDS,
        "cv_strategy": "StratifiedGroupKFold",
        "search_iterations": n_iter,
        "n_jobs": n_jobs,
        "search_spaces": search_spaces_for_json(),
        "primary_scenario": PRIMARY_SCENARIO,
        "scenario_definitions": {
            name: scenario_definition(name) for name in SCENARIO_NAMES
        },
        "partition_group_sha256": {
            "train": _sha256_text_series(pd.Series(sorted(set(train_groups)))),
            "test": _sha256_text_series(pd.Series(sorted(set(test_groups)))),
        },
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
        output_files: list[Path] = [spaces_path]
        scenarios = []
        scenario_internals = {}
        for scenario_name in SCENARIO_NAMES:
            scenario_record, internals, scenario_files = _run_scenario(
                scenario=scenario_name,
                dataset=dataset,
                boundary=boundary,
                paths=paths,
                n_iter=n_iter,
                n_jobs=n_jobs,
            )
            scenarios.append(scenario_record)
            scenario_internals[scenario_name] = internals
            output_files.extend(scenario_files)

        primary = next(
            scenario for scenario in scenarios if scenario["name"] == PRIMARY_SCENARIO
        )
        selected_algorithm = primary["selected_model"]
        primary_internal = scenario_internals[PRIMARY_SCENARIO]
        selected_internal = primary_internal[selected_algorithm]
        selected_search = selected_internal["_search"]
        selected_oof = selected_internal["_oof"]
        selected_test = selected_internal["_test_evaluation"]
        operational_threshold = primary["threshold_selection"]["selected_threshold"]

        encoding_rows = []
        primary_features = dataset[primary["feature_definition"]["included_features"]]
        X_train = primary_features.loc[boundary.train_index]
        y_train = target.loc[boundary.train_index]
        for algorithm, internal in primary_internal.items():
            encoding_rows.extend(
                compare_categorical_encodings(
                    algorithm=algorithm,
                    best_params=internal["best_params"],
                    fitted_onehot_pipeline=internal["_search"].best_estimator_,
                    onehot_oof=internal["_oof"],
                    scenario=PRIMARY_SCENARIO,
                    features=X_train,
                    target=y_train,
                    groups=train_groups,
                    n_jobs=n_jobs,
                )
            )
        encoding_path = paths.report_dir / f"{run_id}_encoding_comparison.csv"
        pd.DataFrame(encoding_rows).to_csv(encoding_path, index=False)
        output_files.append(encoding_path)

        comparison_path = paths.report_dir / f"{run_id}_model_comparison.csv"
        _comparison_frame(scenarios).to_csv(comparison_path, index=False)
        scenario_comparison_path = (
            paths.report_dir / f"{run_id}_scenario_comparison.csv"
        )
        _scenario_comparison_frame(scenarios).to_csv(
            scenario_comparison_path,
            index=False,
        )
        output_files.extend([comparison_path, scenario_comparison_path])

        selected_evaluation_path = save_selected_model(
            Path(selected_internal["artifact_path"]),
            paths,
        )
        production_pipeline = clone(selected_search.best_estimator_).fit(
            primary_features, target
        )
        production_model_path = save_fitted_pipeline(
            production_pipeline,
            paths,
            artifact_name="production_model",
        )
        output_files.extend([selected_evaluation_path, production_model_path])

        fold_ids = grouped_fold_assignments(
            X_train,
            y_train,
            train_groups,
        )
        partition_frame = pd.DataFrame(
            {
                "row_num": dataset["row_num"],
                "group_sha256": groups.apply(
                    lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()
                ),
                "actual_label": target,
                "split": np.where(
                    dataset.index.isin(boundary.train_index),
                    "train",
                    "test",
                ),
                "oof_fold": -1,
            }
        )
        partition_frame.loc[boundary.train_index, "oof_fold"] = fold_ids.astype(int)
        partition_path = paths.model_dir / f"{run_id}_partition_assignments.csv"
        partition_frame.to_csv(partition_path, index=False)
        output_files.append(partition_path)

        production_scores = production_pipeline.predict_proba(primary_features)[:, 1]
        prediction_evidence = pd.concat(
            [
                pd.DataFrame(
                    {
                        "row_num": dataset.loc[boundary.train_index, "row_num"],
                        "prediction_origin": "oof_train",
                        "fold_id": fold_ids.to_numpy(),
                        "risk_probability": selected_oof["scores"],
                    }
                ),
                pd.DataFrame(
                    {
                        "row_num": dataset.loc[boundary.test_index, "row_num"],
                        "prediction_origin": "sealed_test",
                        "fold_id": -1,
                        "risk_probability": selected_test["scores"],
                    }
                ),
                pd.DataFrame(
                    {
                        "row_num": dataset["row_num"],
                        "prediction_origin": "production_inference",
                        "fold_id": -1,
                        "risk_probability": production_scores,
                    }
                ),
            ],
            ignore_index=True,
        )
        prediction_evidence["operational_threshold"] = operational_threshold
        prediction_evidence["risk_label"] = (
            prediction_evidence["risk_probability"] >= operational_threshold
        )
        prediction_path = paths.model_dir / f"{run_id}_prediction_provenance.csv"
        prediction_evidence.to_csv(
            prediction_path,
            index=False,
            float_format="%.17g",
        )
        output_files.append(prediction_path)

        predictions = []
        predictions.extend(
            _prediction_payload(
                run_id=run_id,
                file_id=file_id,
                metadata_frame=dataset.loc[boundary.train_index, META_COLUMNS],
                probabilities=selected_oof["scores"],
                threshold=operational_threshold,
                prediction_origin="oof_train",
                scenario=PRIMARY_SCENARIO,
                fold_ids=fold_ids,
            )
        )
        predictions.extend(
            _prediction_payload(
                run_id=run_id,
                file_id=file_id,
                metadata_frame=dataset.loc[boundary.test_index, META_COLUMNS],
                probabilities=selected_test["scores"],
                threshold=operational_threshold,
                prediction_origin="sealed_test",
                scenario=PRIMARY_SCENARIO,
            )
        )
        predictions.extend(
            _prediction_payload(
                run_id=run_id,
                file_id=file_id,
                metadata_frame=dataset[META_COLUMNS],
                probabilities=production_scores,
                threshold=operational_threshold,
                prediction_origin="production_inference",
                scenario=PRIMARY_SCENARIO,
            )
        )
        importances = _extract_importances(
            production_pipeline,
            run_id,
        )
        production_labels = production_scores >= operational_threshold
        monitoring_metrics = {
            "workflow_description": (
                "feedback-capable, human-supervised monitoring workflow"
            ),
            "prediction_origin": "production_inference",
            "prediction_positive_rate": float(np.mean(production_labels)),
            "mean_risk_probability": float(np.mean(production_scores)),
            "top_10_mean_probability": float(np.mean(np.sort(production_scores)[-10:])),
            "drift_detection_executed": False,
            "automatic_retraining": False,
            "note": (
                "Production scores are not used for performance metrics; "
                "OOF and sealed-test scores remain separate."
            ),
        }

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

        duration_seconds = round(
            time.perf_counter() - started,
            6,
        )
        results_path = paths.report_dir / f"{run_id}_results.json"
        results: dict[str, Any] = {
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "model_version": model_version,
            "primary_scenario": PRIMARY_SCENARIO,
            "selected_model": selected_algorithm,
            "selection_rule": primary["selection_rule"],
            "method": {
                "split": (
                    "approximately 80/20 StratifiedGroupKFold holdout by natural_key"
                ),
                "test_size_target": TEST_SIZE,
                "test_used_for_selection": False,
                "groups": "natural_key",
                "group_overlap_count": 0,
                "search_method": SEARCH_METHOD,
                "optimization_metric": OPTIMIZATION_METRIC,
                "cv_strategy": "StratifiedGroupKFold",
                "cv_folds": CV_FOLDS,
                "cv_shuffle": True,
                "groups_passed_to_fit": True,
                "random_state": RANDOM_STATE,
                "search_iterations": n_iter,
                "refit": True,
                "n_jobs": n_jobs,
                "reference_threshold": REFERENCE_THRESHOLD,
                "operational_threshold": operational_threshold,
                "threshold_policy": primary["threshold_selection"]["policy"],
            },
            "dataset": {
                key: metadata[key]
                for key in [
                    "dataset_file_id",
                    "dataset_sha256",
                    "dataset_rows",
                    "dataset_groups",
                    "class_distribution",
                    "train_class_distribution",
                    "test_class_distribution",
                    "train_rows",
                    "test_rows",
                    "train_groups",
                    "test_groups",
                    "group_overlap_count",
                    "row_test_fraction",
                    "group_test_fraction",
                    "partition_group_sha256",
                ]
            },
            "environment": environment,
            "feature_schema": feature_schema(PRIMARY_SCENARIO),
            "scenario_definitions": metadata["scenario_definitions"],
            "scenarios": scenarios,
            "models": primary["models"],
            "encoding_comparison": encoding_rows,
            "selected_oof_metrics_at_0_5": selected_oof["metrics"],
            "selected_test_metrics_at_0_5": selected_internal["test_metrics_at_0_5"],
            "selected_test_metrics_operational": selected_internal[
                "test_metrics_operational"
            ],
            "threshold_selection": primary["threshold_selection"],
            "selected_monitoring_metrics": monitoring_metrics,
            "prediction_provenance": {
                "counts": {
                    key: int(value)
                    for key, value in prediction_evidence["prediction_origin"]
                    .value_counts()
                    .items()
                },
                "sha256": _score_sha256(prediction_evidence),
                "artifact_path": portable_path(prediction_path),
                "evaluation_origins": [
                    "oof_train",
                    "sealed_test",
                ],
                "non_evaluation_origin": "production_inference",
            },
            "persistence": {
                "status": ("pending" if persist else "not_requested"),
                "storage": storage_result,
            },
            "paths": {
                "model_dir": portable_path(paths.model_dir),
                "report_dir": portable_path(paths.report_dir),
                "selected_evaluation_model": portable_path(selected_evaluation_path),
                "production_model": portable_path(production_model_path),
                "comparison_csv": portable_path(comparison_path),
                "scenario_comparison_csv": portable_path(scenario_comparison_path),
                "encoding_comparison_csv": portable_path(encoding_path),
                "partition_assignments": portable_path(partition_path),
                "results_json": portable_path(results_path),
            },
            "duration_seconds": duration_seconds,
        }
        for key in [
            "source_path",
            "source_sha256",
            "quality_metrics",
            "label_rules",
            "rule_catalog",
            "rule_counts",
        ]:
            if key in metadata:
                results["dataset"][key] = metadata[key]

        selected_artifact_sha = file_sha256(production_model_path)
        registry.finish_success(
            run_id=run_id,
            model_name=MODEL_NAME,
            selected_algorithm=selected_algorithm,
            selected_summary=selected_internal,
            candidate_records=primary["models"],
            scenario_records=scenarios,
            test_metrics=selected_internal["test_metrics_at_0_5"],
            operational_test_metrics=selected_internal["test_metrics_operational"],
            oof_metrics=selected_oof["metrics"],
            artifact_path=portable_path(production_model_path),
            artifact_sha256=selected_artifact_sha,
            storage_result=storage_result,
            feature_schema=feature_schema(PRIMARY_SCENARIO),
            train_rows=len(boundary.train_index),
            test_rows=len(boundary.test_index),
            positive_rows=int(target.sum()),
            positive_rate=float(target.mean()),
            duration_seconds=duration_seconds,
            operational_threshold=operational_threshold,
            threshold_policy=primary["threshold_selection"]["policy"],
            metadata=metadata,
            predictions=predictions,
            importances=importances,
            monitoring_metrics=monitoring_metrics,
        )
        if persist:
            results["persistence"]["status"] = "success"

        manifest = build_manifest(paths, output_files)
        manifest_path = paths.report_dir / f"{run_id}_artifact_manifest.json"
        write_json(manifest_path, manifest)
        results["paths"]["artifact_manifest"] = portable_path(manifest_path)
        write_json(results_path, results)
        write_hyperparameter_report(
            results,
            docs_root / "hyperparameter_selection_report.md",
        )
        write_editorial_response(
            results,
            docs_root / "editorial_response_hyperparameters.md",
        )
        write_feature_ablation_report(
            results,
            docs_root / "feature_ablation_report.md",
        )
        write_rule_catalog(
            results,
            docs_root / "rule_catalog.md",
        )
        write_json(
            report_root / "latest_run.json",
            {
                "run_id": run_id,
                "selected_model": selected_algorithm,
                "primary_scenario": PRIMARY_SCENARIO,
                "results_json": portable_path(results_path),
                "comparison_csv": portable_path(comparison_path),
                "scenario_comparison_csv": portable_path(scenario_comparison_path),
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
        duration_seconds = round(
            time.perf_counter() - started,
            6,
        )
        registry.finish_failure(run_id, str(exc), duration_seconds)
        logger.exception("Experiment %s failed", run_id)
        raise


def run_training(
    file_id: str,
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
        fetch_quality_risk_metadata(file_id),
        persist=True,
        n_iter=n_iter,
        n_jobs=n_jobs,
    )
    return str(results["run_id"])


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run grouped, leakage-controlled Q1 model selection."
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
        dataset_metadata = fetch_quality_risk_metadata(file_id)
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
            logger.warning(
                "Training skipped: %s (%s)",
                validation_error,
                run_id,
            )
            return
        raise SystemExit(validation_error)

    results = run_experiment(
        dataset,
        dataset_metadata,
        persist=persist,
        n_iter=args.n_iter,
        n_jobs=args.n_jobs,
        artifact_root=args.artifact_root,
        report_root=args.report_root,
    )
    logger.info("Selected model: %s", results["selected_model"])
    logger.info("Results: %s", results["paths"]["results_json"])


if __name__ == "__main__":
    main()
