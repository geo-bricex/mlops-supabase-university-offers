import argparse
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler
from sqlalchemy import text

from src.db.init_db import ensure_schema
from src.db.session import get_db_session
from src.ml.quality_risk import (
    CATEGORICAL_FEATURES,
    META_COLUMNS,
    NUMERIC_FEATURES,
    feature_schema,
    fetch_quality_risk_dataset,
    latest_success_file_id,
)
from src.storage.supabase_storage import upload_model_artifacts

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ml_train")

MODEL_NAME = os.getenv("ML_MODEL_NAME", "quality_risk_classifier")
TASK_NAME = "data_quality_risk_classification"
TARGET_NAME = "actual_label"
DEFAULT_THRESHOLD = float(os.getenv("ML_RISK_THRESHOLD", "0.5"))
PRIMARY_SELECTION_METRIC = os.getenv("ML_PRIMARY_SELECTION_METRIC", "average_precision")
ARTIFACT_DIR = Path(os.getenv("ML_ARTIFACT_DIR", "artifacts/models"))


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _start_run(file_id: str, model_version: str, algorithm: str) -> str:
    run_id = str(uuid.uuid4())
    with get_db_session() as session:
        session.execute(
            text(
                """
                INSERT INTO mlops.training_runs
                    (run_id, file_id, model_name, model_version, task_name, algorithm, target_name, status, started_at)
                VALUES
                    (:run_id, :file_id, :model_name, :model_version, :task_name, :algorithm, :target_name, 'running', :started_at)
                """
            ),
            {
                "run_id": run_id,
                "file_id": file_id,
                "model_name": MODEL_NAME,
                "model_version": model_version,
                "task_name": TASK_NAME,
                "algorithm": algorithm,
                "target_name": TARGET_NAME,
                "started_at": datetime.now(timezone.utc),
            },
        )
    return run_id


def _finish_run_success(
    run_id: str,
    file_id: str,
    model_version: str,
    metrics: dict,
    train_metrics: dict,
    cv_metrics: dict,
    candidate_metrics: dict,
    artifact_path: Path,
    artifact_sha256: str,
    storage_result: dict,
    train_rows: int,
    test_rows: int,
    positive_rows: int,
    positive_rate: float,
    selected_metric: str,
    overfit_gap: float,
    duration_seconds: float,
    importances: list,
    predictions: list,
    monitoring_metrics: dict,
) -> None:
    with get_db_session() as session:
        session.execute(
            text("UPDATE mlops.training_runs SET is_active = FALSE WHERE model_name = :model_name"),
            {"model_name": MODEL_NAME},
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
                    selected_metric = :selected_metric,
                    train_metrics = CAST(:train_metrics AS JSONB),
                    cv_metrics = CAST(:cv_metrics AS JSONB),
                    candidate_metrics = CAST(:candidate_metrics AS JSONB),
                    overfit_gap = :overfit_gap,
                    metrics = CAST(:metrics AS JSONB),
                    artifact_path = :artifact_path,
                    artifact_sha256 = :artifact_sha256,
                    storage_status = :storage_status,
                    storage_paths = CAST(:storage_paths AS JSONB),
                    feature_schema = CAST(:feature_schema AS JSONB),
                    notes = :notes,
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
                "selected_metric": selected_metric,
                "train_metrics": json.dumps(train_metrics),
                "cv_metrics": json.dumps(cv_metrics),
                "candidate_metrics": json.dumps(candidate_metrics),
                "overfit_gap": overfit_gap,
                "metrics": json.dumps(metrics),
                "artifact_path": str(artifact_path),
                "artifact_sha256": artifact_sha256,
                "storage_status": storage_result.get("status"),
                "storage_paths": json.dumps(storage_result.get("paths") or {}),
                "feature_schema": json.dumps(feature_schema()),
                "notes": f"Active model version {model_version} trained from file {file_id}",
            },
        )

        if importances:
            session.execute(
                text(
                    """
                    INSERT INTO mlops.feature_importance (importance_id, run_id, feature_name, importance, direction, rank)
                    VALUES (:importance_id, :run_id, :feature_name, :importance, :direction, :rank)
                    """
                ),
                importances,
            )

        if predictions:
            session.execute(
                text(
                    """
                    INSERT INTO mlops.predictions
                        (prediction_id, run_id, file_id, row_num, natural_key, risk_label, risk_probability, actual_label, threshold, detail)
                    VALUES
                        (:prediction_id, :run_id, :file_id, :row_num, :natural_key, :risk_label, :risk_probability, :actual_label, :threshold, CAST(:detail AS JSONB))
                    """
                ),
                predictions,
            )

        session.execute(
            text(
                """
                INSERT INTO mlops.monitoring_runs (monitor_id, run_id, file_id, metrics)
                VALUES (:monitor_id, :run_id, :file_id, CAST(:metrics AS JSONB))
                """
            ),
            {
                "monitor_id": str(uuid.uuid4()),
                "run_id": run_id,
                "file_id": file_id,
                "metrics": json.dumps(monitoring_metrics),
            },
        )


def _finish_run_failure(run_id: str, message: str, duration_seconds: float, status: str = "failed") -> None:
    with get_db_session() as session:
        session.execute(
            text(
                """
                UPDATE mlops.training_runs
                SET
                    status = :status,
                    finished_at = :finished_at,
                    duration_seconds = :duration_seconds,
                    notes = :notes
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "finished_at": datetime.now(timezone.utc),
                "duration_seconds": duration_seconds,
                "notes": message,
            },
        )


def _record_skipped_run(file_id: str, reason: str) -> str:
    model_version = f"{MODEL_NAME}-{file_id[:8]}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    run_id = _start_run(file_id, model_version, "logistic_regression_onehot")
    _finish_run_failure(run_id, reason, duration_seconds=0.0, status="skipped")
    logger.warning("Skipping ML training for file %s: %s", file_id, reason)
    return run_id


def _build_regularized_logistic_pipeline(
    c_value: float,
    *,
    penalty: str = "l2",
    l1_ratio: Optional[float] = None,
) -> Pipeline:
    preprocess = ColumnTransformer(
        transformers=[
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                        ("scaler", StandardScaler(with_mean=False)),
                    ]
                ),
                NUMERIC_FEATURES,
            ),
        ]
    )
    model_kwargs = {
        "max_iter": 2000,
        "class_weight": "balanced",
        "C": c_value,
        "random_state": 42,
        "solver": "saga",
        "tol": 1e-3,
        "penalty": penalty,
    }
    if l1_ratio is not None:
        model_kwargs["l1_ratio"] = l1_ratio
    return Pipeline(
        steps=[
            ("preprocess", preprocess),
            ("model", LogisticRegression(**model_kwargs)),
        ]
    )


def _build_random_forest_pipeline() -> Pipeline:
    preprocess = ColumnTransformer(
        transformers=[
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("ordinal", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                    ]
                ),
                NUMERIC_FEATURES,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocess", preprocess),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=300,
                    max_depth=10,
                    min_samples_leaf=8,
                    min_samples_split=16,
                    class_weight="balanced_subsample",
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def _build_extra_trees_pipeline() -> Pipeline:
    preprocess = ColumnTransformer(
        transformers=[
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("ordinal", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                    ]
                ),
                NUMERIC_FEATURES,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocess", preprocess),
            (
                "model",
                ExtraTreesClassifier(
                    n_estimators=350,
                    max_depth=12,
                    min_samples_leaf=6,
                    min_samples_split=12,
                    class_weight="balanced",
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def _collect_metrics(y_true, y_pred, y_score) -> dict:
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
    }
    try:
        metrics["average_precision"] = float(average_precision_score(y_true, y_score))
    except ValueError:
        metrics["average_precision"] = None
    try:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_score))
    except ValueError:
        metrics["roc_auc"] = None
    return metrics


def _score_candidate(name: str, pipeline: Pipeline, X_train: pd.DataFrame, y_train: pd.Series) -> dict:
    splitter = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    cv = cross_validate(
        pipeline,
        X_train,
        y_train,
        cv=splitter,
        scoring={
            "f1": "f1",
            "average_precision": "average_precision",
            "precision": "precision",
            "recall": "recall",
            "roc_auc": "roc_auc",
        },
        n_jobs=1,
        error_score="raise",
    )
    return {
        "name": name,
        "mean_f1": float(pd.Series(cv["test_f1"]).mean()),
        "mean_average_precision": float(pd.Series(cv["test_average_precision"]).mean()),
        "mean_precision": float(pd.Series(cv["test_precision"]).mean()),
        "mean_recall": float(pd.Series(cv["test_recall"]).mean()),
        "mean_roc_auc": float(pd.Series(cv["test_roc_auc"]).mean()),
        "std_f1": float(pd.Series(cv["test_f1"]).std(ddof=0)),
        "std_average_precision": float(pd.Series(cv["test_average_precision"]).std(ddof=0)),
    }


def _extract_importances(model_pipeline: Pipeline, run_id: str, top_n: int = 20) -> list:
    preprocess = model_pipeline.named_steps["preprocess"]
    classifier = model_pipeline.named_steps["model"]
    feature_names = preprocess.get_feature_names_out()
    if hasattr(classifier, "coef_"):
        coefficients = classifier.coef_[0]
    elif hasattr(classifier, "feature_importances_"):
        coefficients = classifier.feature_importances_
    else:
        return []

    rows = []
    ranked = sorted(
        zip(feature_names, coefficients),
        key=lambda item: abs(item[1]),
        reverse=True,
    )[:top_n]

    for rank, (feature_name, importance) in enumerate(ranked, start=1):
        rows.append(
            {
                "importance_id": str(uuid.uuid4()),
                "run_id": run_id,
                "feature_name": feature_name,
                "importance": float(importance),
                "direction": "positive_risk" if importance >= 0 else "negative_risk",
                "rank": rank,
            }
        )
    return rows


def _build_prediction_payload(run_id: str, file_id: str, frame: pd.DataFrame, probabilities, threshold: float) -> list:
    payload = []
    labels = probabilities >= threshold
    for (_, row), probability, risk_label in zip(frame.iterrows(), probabilities, labels):
        payload.append(
            {
                "prediction_id": str(uuid.uuid4()),
                "run_id": run_id,
                "file_id": file_id,
                "row_num": int(row["row_num"]),
                "natural_key": row["natural_key"],
                "risk_label": bool(risk_label),
                "risk_probability": float(probability),
                "actual_label": bool(row["actual_label"]),
                "threshold": float(threshold),
                "detail": json.dumps(
                    {
                        "issue_count": int(row["issue_count"]),
                        "issue_types": row["issue_types"],
                    }
                ),
            }
        )
    return payload


def run_training(file_id: str, threshold: float = DEFAULT_THRESHOLD) -> str:
    ensure_schema()

    dataset = fetch_quality_risk_dataset(file_id)
    if dataset.empty:
        return _record_skipped_run(file_id, f"No staging data available for file {file_id}")

    if dataset[TARGET_NAME].nunique() < 2:
        return _record_skipped_run(
            file_id,
            "The latest dataset does not contain both positive and negative quality-risk labels.",
        )

    if len(dataset) < 50:
        return _record_skipped_run(file_id, "Not enough rows to train a stable quality-risk model.")

    feature_columns = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    X = dataset[feature_columns].copy()
    y = dataset[TARGET_NAME].astype(int)

    algorithm = "model_selection"
    model_version = f"{MODEL_NAME}-{file_id[:8]}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    run_id = _start_run(file_id, model_version, algorithm)
    started = time.perf_counter()

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=y,
        )

        candidates = [
            ("logreg_l2_balanced_c1", _build_regularized_logistic_pipeline(1.0)),
            ("logreg_l2_balanced_c03", _build_regularized_logistic_pipeline(0.3)),
            (
                "logreg_elasticnet_balanced",
                _build_regularized_logistic_pipeline(0.5, penalty="elasticnet", l1_ratio=0.35),
            ),
            ("random_forest_guarded", _build_random_forest_pipeline()),
            ("extra_trees_guarded", _build_extra_trees_pipeline()),
        ]

        candidate_results = []
        for candidate_name, candidate_pipeline in candidates:
            logger.info("Evaluating candidate model: %s", candidate_name)
            candidate_results.append(_score_candidate(candidate_name, candidate_pipeline, X_train, y_train))

        candidate_results = sorted(
            candidate_results,
            key=lambda item: (
                item.get(f"mean_{PRIMARY_SELECTION_METRIC}", float("-inf")),
                item.get("mean_f1", float("-inf")),
            ),
            reverse=True,
        )
        best_name = candidate_results[0]["name"]
        model_pipeline = next(pipeline for name, pipeline in candidates if name == best_name)
        model_pipeline.fit(X_train, y_train)

        train_score = model_pipeline.predict_proba(X_train)[:, 1]
        train_pred = (train_score >= threshold).astype(int)
        y_score = model_pipeline.predict_proba(X_test)[:, 1]
        y_pred = (y_score >= threshold).astype(int)

        metrics = _collect_metrics(y_test, y_pred, y_score)
        train_metrics = _collect_metrics(y_train, train_pred, train_score)
        metrics["threshold"] = threshold
        metrics["selected_candidate"] = best_name
        metrics["selection_metric"] = PRIMARY_SELECTION_METRIC
        metrics["class_balance"] = {
            "train_positive_rate": float(y_train.mean()),
            "test_positive_rate": float(y_test.mean()),
        }
        cv_metrics = candidate_results[0]
        candidate_metrics = {"candidates": candidate_results}
        overfit_gap = round(float(train_metrics["f1"] - metrics["f1"]), 6)
        metrics["overfit_gap_f1"] = overfit_gap
        metrics["regularization_guard"] = {
            "selected_candidate": best_name,
            "cross_validated": True,
            "bounded_tree_depth": best_name in {"random_forest_guarded", "extra_trees_guarded"},
            "candidate_count": len(candidate_results),
        }

        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        artifact_path = ARTIFACT_DIR / f"{model_version}.joblib"
        metadata_path = ARTIFACT_DIR / f"{model_version}.metadata.json"
        joblib.dump(
            {
                "pipeline": model_pipeline,
                "model_name": MODEL_NAME,
                "model_version": model_version,
                "task_name": TASK_NAME,
                "threshold": threshold,
                "trained_at": datetime.now(timezone.utc).isoformat(),
                "dataset_file_id": file_id,
                "feature_schema": feature_schema(),
                "metrics": metrics,
                "train_metrics": train_metrics,
                "cv_metrics": candidate_results,
                "selected_candidate": best_name,
            },
            artifact_path,
        )
        metadata_path.write_text(
            json.dumps(
                {
                    "model_name": MODEL_NAME,
                    "model_version": model_version,
                    "task_name": TASK_NAME,
                    "dataset_file_id": file_id,
                    "selected_candidate": best_name,
                    "selection_metric": PRIMARY_SELECTION_METRIC,
                    "threshold": threshold,
                    "metrics": metrics,
                    "train_metrics": train_metrics,
                    "cv_metrics": candidate_results,
                    "feature_schema": feature_schema(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        artifact_sha256 = _compute_sha256(artifact_path)
        storage_result = upload_model_artifacts(
            run_id,
            model_version,
            {"model_joblib": artifact_path, "model_metadata_json": metadata_path},
        )

        full_probabilities = model_pipeline.predict_proba(X)[:, 1]
        predictions = _build_prediction_payload(run_id, file_id, dataset[META_COLUMNS], full_probabilities, threshold)

        monitoring_pred = (full_probabilities >= threshold).astype(int)
        monitoring_metrics = _collect_metrics(y, monitoring_pred, full_probabilities)
        monitoring_metrics.update(
            {
                "prediction_positive_rate": float(monitoring_pred.mean()),
                "actual_positive_rate": float(y.mean()),
                "mean_risk_probability": float(pd.Series(full_probabilities).mean()),
                "top_10_mean_probability": float(pd.Series(full_probabilities).sort_values(ascending=False).head(10).mean()),
            }
        )

        importances = _extract_importances(model_pipeline, run_id)
        duration_seconds = round(time.perf_counter() - started, 6)
        _finish_run_success(
            run_id=run_id,
            file_id=file_id,
            model_version=model_version,
            metrics=metrics,
            train_metrics=train_metrics,
            cv_metrics=cv_metrics,
            candidate_metrics=candidate_metrics,
            artifact_path=artifact_path,
            artifact_sha256=artifact_sha256,
            storage_result=storage_result,
            train_rows=int(len(X_train)),
            test_rows=int(len(X_test)),
            positive_rows=int(y.sum()),
            positive_rate=float(y.mean()),
            selected_metric=PRIMARY_SELECTION_METRIC,
            overfit_gap=overfit_gap,
            duration_seconds=duration_seconds,
            importances=importances,
            predictions=predictions,
            monitoring_metrics=monitoring_metrics,
        )
        logger.info("Training run %s completed successfully for file %s", run_id, file_id)
        return run_id
    except Exception as exc:
        duration_seconds = round(time.perf_counter() - started, 6)
        _finish_run_failure(run_id, str(exc), duration_seconds)
        logger.error("Training run %s failed: %s", run_id, exc)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", help="Specific successful ingest file_id to use for training")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="Decision threshold for risk label")
    args = parser.parse_args()

    file_id = args.file_id or latest_success_file_id()
    if not file_id:
        raise SystemExit("No successful ingest file found for ML training.")

    run_id = run_training(file_id=file_id, threshold=args.threshold)
    logger.info("Active MLOps training run ready: %s", run_id)


if __name__ == "__main__":
    main()
