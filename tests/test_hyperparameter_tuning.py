import json
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

from src.ml import train as train_module
from src.ml.config import (
    CV_FOLDS,
    LOGISTIC_REGRESSION_SPACE,
    MODEL_NAMES,
    OPTIMIZATION_METRIC,
    RANDOM_STATE,
    TEST_SIZE,
    build_cv,
    build_model_specs,
)
from src.ml.quality_risk import (
    CATEGORICAL_FEATURES,
    META_COLUMNS,
    NUMERIC_FEATURES,
)
from src.ml.registry import ExperimentRegistry
from src.ml.train import (
    create_search,
    run_experiment,
    select_model_from_cv,
    stratified_holdout_split,
)


def _small_dataset(rows: int = 100) -> pd.DataFrame:
    records = []
    for index in range(rows):
        target = int(index % 4 == 0)
        record = {
            "file_id": "synthetic-file",
            "row_num": index + 1,
            "natural_key": f"key-{index}",
            "actual_label": target,
            "issue_count": target,
            "issue_types": ["synthetic_issue"] if target else [],
        }
        for offset, column in enumerate(CATEGORICAL_FEATURES):
            record[column] = f"{column}-{(index + offset) % 4}"
        for offset, column in enumerate(NUMERIC_FEATURES):
            record[column] = float((index + offset) % 11)
        records.append(record)
    return pd.DataFrame(records)[META_COLUMNS + CATEGORICAL_FEATURES + NUMERIC_FEATURES]


def test_stratified_holdout_is_80_20_and_isolated():
    dataset = _small_dataset()
    features = dataset[CATEGORICAL_FEATURES + NUMERIC_FEATURES]
    target = dataset["actual_label"]

    X_train, X_test, y_train, y_test = stratified_holdout_split(
        features,
        target,
    )

    assert TEST_SIZE == 0.20
    assert len(X_train) == 80
    assert len(X_test) == 20
    assert set(X_train.index).isdisjoint(X_test.index)
    assert y_train.mean() == y_test.mean() == target.mean()


def test_cross_validation_and_search_configuration_are_exact():
    cv = build_cv()
    spec = build_model_specs(estimator_n_jobs=1)[0]
    search = create_search(spec, n_iter=2, n_jobs=1)

    assert CV_FOLDS == 5
    assert cv.n_splits == 5
    assert cv.shuffle is True
    assert cv.random_state == RANDOM_STATE == 42
    assert search.scoring == OPTIMIZATION_METRIC == "average_precision"
    assert search.refit is True
    assert search.n_jobs == 1
    assert search.cv.n_splits == 5


def test_required_models_and_unfitted_preprocessing_pipelines():
    specs = build_model_specs(estimator_n_jobs=1)

    assert tuple(spec.name for spec in specs) == MODEL_NAMES
    assert "ExtraTreesClassifier" not in MODEL_NAMES
    for spec in specs:
        assert isinstance(spec.pipeline, Pipeline)
        assert list(spec.pipeline.named_steps) == ["preprocess", "model"]
        preprocessor = spec.pipeline.named_steps["preprocess"]
        assert isinstance(preprocessor, ColumnTransformer)
        assert not hasattr(preprocessor, "transformers_")
        for _, transformer, _ in preprocessor.transformers:
            assert isinstance(transformer, Pipeline)
            assert "imputer" in transformer.named_steps


def test_logistic_search_dictionaries_are_solver_and_ratio_compatible():
    by_penalty = {
        space["model__penalty"][0]: space for space in LOGISTIC_REGRESSION_SPACE
    }

    assert by_penalty["l1"]["model__solver"] == ["saga"]
    assert by_penalty["l1"]["model__l1_ratio"] == [1.0]
    assert by_penalty["l2"]["model__solver"] == ["lbfgs", "liblinear", "saga"]
    assert by_penalty["l2"]["model__l1_ratio"] == [0.0]
    assert by_penalty["elasticnet"]["model__solver"] == ["saga"]
    assert by_penalty["elasticnet"]["model__l1_ratio"] == [
        0.1,
        0.25,
        0.5,
        0.75,
        0.9,
    ]


def test_model_selection_uses_only_cross_validation_values():
    summaries = [
        {
            "algorithm": "A",
            "cv_mean": 0.81,
            "cv_f1_mean": 0.40,
            "test_metrics": {"average_precision": 0.99, "f1": 0.99},
        },
        {
            "algorithm": "B",
            "cv_mean": 0.82,
            "cv_f1_mean": 0.10,
            "test_metrics": {"average_precision": 0.01, "f1": 0.01},
        },
    ]
    assert select_model_from_cv(summaries) == "B"

    summaries[0]["test_metrics"] = {"average_precision": 1.0, "f1": 1.0}
    summaries[1]["test_metrics"] = {"average_precision": 0.0, "f1": 0.0}
    assert select_model_from_cv(summaries) == "B"


def test_small_end_to_end_run_persists_real_search_outputs(
    tmp_path: Path,
    monkeypatch,
):
    dataset = _small_dataset()
    evaluation_sizes = []
    original_evaluate = train_module.evaluate_fitted_pipeline

    def tracked_evaluate(pipeline, features, target, *, threshold):
        evaluation_sizes.append(len(features))
        return original_evaluate(
            pipeline,
            features,
            target,
            threshold=threshold,
        )

    monkeypatch.setattr(
        train_module,
        "evaluate_fitted_pipeline",
        tracked_evaluate,
    )
    results = run_experiment(
        dataset,
        {"dataset_file_id": "synthetic-file"},
        persist=False,
        n_iter=1,
        n_jobs=1,
        artifact_root=tmp_path / "artifacts",
        report_root=tmp_path / "reports",
        docs_root=tmp_path / "docs",
        run_id="00000000-0000-0000-0000-000000000042",
    )

    assert len(results["models"]) == 3
    assert {model["algorithm"] for model in results["models"]} == set(MODEL_NAMES)
    assert results["selected_model"] == select_model_from_cv(results["models"])
    assert results["persistence"]["status"] == "not_requested"
    assert evaluation_sizes.count(20) == 3
    assert evaluation_sizes.count(80) == 1
    assert 100 not in evaluation_sizes

    for model in results["models"]:
        assert model["best_params"]
        assert model["best_score"] == model["cv_mean"]
        assert model["cv_std"] >= 0
        assert set(model["test_metrics"]) >= {
            "accuracy",
            "precision",
            "recall",
            "f1",
            "roc_auc",
            "average_precision",
            "confusion_matrix",
        }
        cv_results = pd.read_csv(model["cv_results_path"])
        assert len(cv_results) == 1
        assert {"params", "mean_test_score", "std_test_score"}.issubset(
            cv_results.columns
        )
        assert Path(model["artifact_path"]).exists()
        assert Path(model["curve_paths"]["roc_curve_png"]).exists()
        assert Path(model["curve_paths"]["precision_recall_curve_png"]).exists()

    results_path = Path(results["paths"]["results_json"])
    comparison_path = Path(results["paths"]["comparison_csv"])
    persisted = json.loads(results_path.read_text(encoding="utf-8"))
    comparison = pd.read_csv(comparison_path)
    assert persisted["selected_model"] == results["selected_model"]
    assert len(comparison) == 3
    assert (
        comparison.loc[
            comparison["model_status"] == "selected",
            "algorithm",
        ].item()
        == results["selected_model"]
    )
    assert (tmp_path / "docs" / "hyperparameter_selection_report.md").exists()
    editorial = (tmp_path / "docs" / "editorial_response_hyperparameters.md").read_text(
        encoding="utf-8"
    )
    assert "[INSERT" not in editorial
    assert (
        "Hyperparameter selection was performed exclusively on the training data"
        in editorial
    )


def test_traceability_migration_is_additive_and_reversible():
    up = Path("sql/migrations/002_q1_hyperparameter_traceability_up.sql").read_text(
        encoding="utf-8"
    )
    down = Path("sql/migrations/002_q1_hyperparameter_traceability_down.sql").read_text(
        encoding="utf-8"
    )

    for field in [
        "search_method",
        "optimization_metric",
        "cv_folds",
        "search_iterations",
        "search_spaces",
        "best_params",
        "best_score",
        "dataset_sha256",
        "class_distribution",
        "python_version",
        "sklearn_version",
        "git_commit",
    ]:
        assert field in up
    assert "CREATE TABLE IF NOT EXISTS mlops.model_candidates" in up
    assert "DROP TABLE IF EXISTS mlops.model_candidates" in down


def test_registry_persists_search_and_metric_payloads(monkeypatch):
    calls = []

    class FakeSession:
        def execute(self, statement, parameters=None):
            calls.append((str(statement), parameters))

    @contextmanager
    def fake_session():
        yield FakeSession()

    monkeypatch.setattr("src.ml.registry.get_db_session", fake_session)
    registry = ExperimentRegistry(enabled=True)
    metadata = {
        "optimization_metric": "average_precision",
        "search_method": "RandomizedSearchCV",
        "cv_folds": 5,
        "search_iterations": 40,
        "search_spaces": {"Algorithm": {"model__x": [1, 2]}},
        "dataset_sha256": "dataset-hash",
        "dataset_rows": 100,
        "class_distribution": {"0": 75, "1": 25},
        "random_state": 42,
        "python_version": "3.14.5",
        "sklearn_version": "1.9.0",
        "git_commit": "commit-hash",
        "dataset_file_id": "00000000-0000-0000-0000-000000000001",
    }
    registry.start_run(
        run_id="00000000-0000-0000-0000-000000000042",
        file_id=metadata["dataset_file_id"],
        model_name="quality_risk_classifier",
        model_version="version-1",
        task_name="data_quality_risk_classification",
        target_name="actual_label",
        metadata=metadata,
    )
    candidate = {
        "algorithm": "GradientBoostingClassifier",
        "model_status": "selected",
        "search_method": "RandomizedSearchCV",
        "optimization_metric": "average_precision",
        "cv_folds": 5,
        "search_iterations": 40,
        "search_space": {"model__n_estimators": [100, 200]},
        "best_params": {"model__n_estimators": 200},
        "best_score": 0.91,
        "cv_mean": 0.91,
        "cv_std": 0.01,
        "cv_f1_mean": 0.82,
        "cv_f1_std": 0.02,
        "test_metrics": {
            "accuracy": 0.90,
            "average_precision": 0.92,
            "confusion_matrix": [[70, 5], [4, 21]],
        },
        "artifact_path": "artifacts/selected.joblib",
        "cv_results_path": "reports/cv_results.csv",
    }
    registry.finish_success(
        run_id="00000000-0000-0000-0000-000000000042",
        model_name="quality_risk_classifier",
        selected_algorithm="GradientBoostingClassifier",
        selected_summary=candidate,
        candidate_records=[candidate],
        test_metrics=candidate["test_metrics"],
        train_metrics={"average_precision": 0.95},
        artifact_path="artifacts/selected.joblib",
        artifact_sha256="artifact-hash",
        storage_result={"status": "skipped", "paths": {}},
        feature_schema={"target": "actual_label"},
        train_rows=80,
        test_rows=20,
        positive_rows=25,
        positive_rate=0.25,
        duration_seconds=1.0,
        metadata=metadata,
    )

    sql = "\n".join(statement for statement, _ in calls)
    assert "search_method" in sql
    assert "dataset_sha256" in sql
    assert "INSERT INTO mlops.model_candidates" in sql
    candidate_parameters = next(
        parameters
        for statement, parameters in calls
        if "INSERT INTO mlops.model_candidates" in statement
    )
    assert json.loads(candidate_parameters["best_params"]) == {
        "model__n_estimators": 200
    }
    assert json.loads(candidate_parameters["test_metrics"])["average_precision"] == 0.92
