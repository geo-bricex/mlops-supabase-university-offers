import json
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.pipeline import Pipeline

from src.dq.rules import QUALITY_RULES
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
from src.ml.group_validation import (
    generate_grouped_oof_probabilities,
    grouped_fold_assignments,
    select_f2_threshold,
)
from src.ml.quality_risk import (
    FULL_CATEGORICAL_FEATURES,
    FULL_NUMERIC_FEATURES,
    META_COLUMNS,
    PRIMARY_SCENARIO,
    scenario_definition,
)
from src.ml.registry import ExperimentRegistry
from src.ml.train import (
    create_search,
    grouped_holdout_split,
    run_experiment,
    select_model_from_cv,
)

FILE_ID = "00000000-0000-0000-0000-000000000001"
RUN_ID = "00000000-0000-0000-0000-000000000042"


def _small_dataset(groups: int = 60, rows_per_group: int = 2) -> pd.DataFrame:
    records = []
    for group_index in range(groups):
        target = int(group_index % 4 == 0)
        for repetition in range(rows_per_group):
            row_index = group_index * rows_per_group + repetition
            record = {
                "file_id": FILE_ID,
                "row_num": row_index + 1,
                "natural_key": f"key-{group_index}",
                "actual_label": target,
                "issue_count": target,
                "issue_types": ["synthetic_issue"] if target else [],
            }
            for offset, column in enumerate(FULL_CATEGORICAL_FEATURES):
                record[column] = f"{column}-{(group_index + offset) % 4}"
            for offset, column in enumerate(FULL_NUMERIC_FEATURES):
                record[column] = float((group_index + offset) % 11)
            records.append(record)
    return pd.DataFrame(records)[
        META_COLUMNS + FULL_CATEGORICAL_FEATURES + FULL_NUMERIC_FEATURES
    ]


def test_grouped_holdout_is_approximately_80_20_and_disjoint():
    dataset = _small_dataset()
    definition = scenario_definition(PRIMARY_SCENARIO)
    features = dataset[definition["included_features"]]
    target = dataset["actual_label"]
    groups = dataset["natural_key"]

    (
        X_train,
        X_test,
        y_train,
        y_test,
        train_groups,
        test_groups,
        boundary,
    ) = grouped_holdout_split(features, target, groups)

    assert TEST_SIZE == 0.20
    assert boundary.row_test_fraction == pytest.approx(TEST_SIZE)
    assert len(X_train) == 96
    assert len(X_test) == 24
    assert set(X_train.index).isdisjoint(X_test.index)
    assert set(train_groups).isdisjoint(test_groups)
    assert y_train.mean() == y_test.mean() == target.mean()


def test_cross_validation_groups_are_disjoint_and_configuration_is_exact():
    dataset = _small_dataset()
    definition = scenario_definition(PRIMARY_SCENARIO)
    features = dataset[definition["included_features"]]
    target = dataset["actual_label"]
    groups = dataset["natural_key"]
    cv = build_cv()
    spec = build_model_specs(estimator_n_jobs=1)[0]
    search = create_search(spec, n_iter=2, n_jobs=1)

    assert isinstance(cv, StratifiedGroupKFold)
    assert CV_FOLDS == cv.n_splits == 5
    assert cv.shuffle is True
    assert cv.random_state == RANDOM_STATE == 42
    assert search.scoring == OPTIMIZATION_METRIC == "average_precision"
    assert search.refit is True
    assert search.n_jobs == 1
    assert search.cv.n_splits == 5

    for train_index, validation_index in cv.split(
        features,
        target,
        groups=groups,
    ):
        assert set(groups.iloc[train_index]).isdisjoint(groups.iloc[validation_index])


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


def test_primary_features_exclude_direct_rule_and_transformation_proxies():
    definition = scenario_definition(PRIMARY_SCENARIO)
    excluded = definition["excluded_features"]

    for feature in [
        "estado",
        "provincia_norm",
        "canton_norm",
        "geo_method",
        "geo_score_prov",
        "geo_score_canton",
        "has_nombre_ies",
        "has_nombre_carrera",
        "has_provincia_norm",
        "has_canton_norm",
        "ies_name_len",
        "carrera_name_len",
        "natural_key_token_count",
    ]:
        assert feature in excluded
        assert feature not in definition["included_features"]


def test_logistic_search_dictionaries_are_solver_and_ratio_compatible():
    by_penalty = {
        space["model__penalty"][0]: space for space in LOGISTIC_REGRESSION_SPACE
    }

    assert by_penalty["l1"]["model__solver"] == ["saga"]
    assert by_penalty["l1"]["model__l1_ratio"] == [1.0]
    assert by_penalty["l2"]["model__solver"] == [
        "lbfgs",
        "liblinear",
        "saga",
    ]
    assert by_penalty["l2"]["model__l1_ratio"] == [0.0]
    assert by_penalty["elasticnet"]["model__solver"] == ["saga"]
    assert by_penalty["elasticnet"]["model__l1_ratio"] == [
        0.1,
        0.25,
        0.5,
        0.75,
        0.9,
    ]


def test_true_oof_predictions_cover_each_training_group_once():
    dataset = _small_dataset()
    definition = scenario_definition(PRIMARY_SCENARIO)
    features = dataset[definition["included_features"]]
    target = dataset["actual_label"]
    groups = dataset["natural_key"]
    spec = build_model_specs(estimator_n_jobs=1)[0]
    estimator = spec.pipeline.set_params(
        model__C=1.0,
        model__penalty="l2",
        model__class_weight=None,
        model__solver="liblinear",
        model__l1_ratio=0.0,
        model__max_iter=500,
    )

    result = generate_grouped_oof_probabilities(
        estimator,
        features,
        target,
        groups,
        n_jobs=1,
    )
    assignments = grouped_fold_assignments(features, target, groups)

    assert len(result["scores"]) == len(dataset)
    assert np.isfinite(result["scores"]).all()
    assert assignments.groupby(groups).nunique().eq(1).all()
    assert sorted(assignments.unique()) == list(range(CV_FOLDS))
    assert len(result["fold_metrics"]) == CV_FOLDS


def test_operational_threshold_depends_only_on_oof_inputs():
    target = pd.Series([0, 0, 1, 1])
    scores = np.array([0.10, 0.40, 0.35, 0.90])

    first = select_f2_threshold(target, scores)
    second = select_f2_threshold(target, scores)

    assert first == second
    assert 0.05 <= first["selected_threshold"] <= 0.95
    assert first["objective"] == "F2"
    assert "test" not in first


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


def test_quality_rule_catalog_is_complete_and_executable():
    required_fields = {
        "rule_id",
        "dimension",
        "required_columns",
        "condition",
        "severity",
        "contributes_to_label",
        "version",
    }
    payloads = [rule.as_dict() for rule in QUALITY_RULES]

    assert len(payloads) == 6
    assert len({row["rule_id"] for row in payloads}) == len(payloads)
    assert all(required_fields.issubset(row) for row in payloads)
    assert all(row["contributes_to_label"] for row in payloads)


def test_small_end_to_end_run_persists_grouped_search_outputs(
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
        {
            "dataset_file_id": FILE_ID,
            "rule_catalog": [rule.as_dict() for rule in QUALITY_RULES],
            "rule_counts": {},
        },
        persist=False,
        n_iter=1,
        n_jobs=1,
        artifact_root=tmp_path / "artifacts",
        report_root=tmp_path / "reports",
        docs_root=tmp_path / "docs",
        run_id=RUN_ID,
    )

    assert len(results["models"]) == 3
    assert len(results["scenarios"]) == 2
    assert {model["algorithm"] for model in results["models"]} == set(MODEL_NAMES)
    assert results["selected_model"] == select_model_from_cv(results["models"])
    assert results["persistence"]["status"] == "not_requested"
    assert results["dataset"]["group_overlap_count"] == 0
    assert evaluation_sizes == [24] * 6
    assert results["prediction_provenance"]["counts"] == {
        "production_inference": 120,
        "oof_train": 96,
        "sealed_test": 24,
    }

    for scenario in results["scenarios"]:
        for model in scenario["models"]:
            assert model["best_params"]
            assert model["best_score"] == model["cv_mean"]
            assert model["cv_std"] >= 0
            assert set(model["test_metrics_at_0_5"]) >= {
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
            assert {
                "params",
                "mean_test_score",
                "std_test_score",
            }.issubset(cv_results.columns)
            assert Path(model["artifact_path"]).exists()
            assert Path(model["curve_paths"]["roc_curve_png"]).exists()
            assert Path(model["curve_paths"]["precision_recall_curve_png"]).exists()

    results_path = Path(results["paths"]["results_json"])
    comparison_path = Path(results["paths"]["comparison_csv"])
    persisted = json.loads(results_path.read_text(encoding="utf-8"))
    comparison = pd.read_csv(comparison_path)
    assert persisted["selected_model"] == results["selected_model"]
    assert len(comparison) == 6
    primary = comparison[comparison["scenario"] == PRIMARY_SCENARIO]
    assert (
        primary.loc[primary["model_status"] == "selected", "algorithm"].item()
        == results["selected_model"]
    )
    assert (tmp_path / "docs" / "hyperparameter_selection_report.md").exists()
    assert (tmp_path / "docs" / "feature_ablation_report.md").exists()
    assert (tmp_path / "docs" / "rule_catalog.md").exists()
    editorial = (tmp_path / "docs" / "editorial_response_hyperparameters.md").read_text(
        encoding="utf-8"
    )
    assert "[INSERT" not in editorial
    assert (
        "Hyperparameter selection was performed exclusively on the training "
        "data" in editorial
    )


def test_grouped_traceability_migration_is_additive_and_reversible():
    up = Path("sql/migrations/003_grouped_oof_scenarios_up.sql").read_text(
        encoding="utf-8"
    )
    down = Path("sql/migrations/003_grouped_oof_scenarios_down.sql").read_text(
        encoding="utf-8"
    )

    for field in [
        "oof_metrics",
        "operational_threshold",
        "threshold_policy",
        "prediction_origin",
        "scenario",
        "fold_id",
        "rule_catalog",
        "rule_run_counts",
        "llm_interpretation_runs",
    ]:
        assert field in up
    assert "CREATE TABLE IF NOT EXISTS mlops.scenario_evaluations" in up
    assert "DROP TABLE IF EXISTS mlops.scenario_evaluations" in down
    assert "DROP COLUMN IF EXISTS prediction_origin" in down


def test_registry_persists_grouped_oof_and_scenario_payloads(monkeypatch):
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
        "dataset_file_id": FILE_ID,
        "primary_scenario": PRIMARY_SCENARIO,
    }
    registry.start_run(
        run_id=RUN_ID,
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
        "scenario": PRIMARY_SCENARIO,
        "categorical_encoding": "onehot",
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
        "oof_metrics_at_0_5": {"average_precision": 0.90, "f1": 0.82},
        "test_metrics": {
            "accuracy": 0.90,
            "average_precision": 0.92,
            "confusion_matrix": [[70, 5], [4, 21]],
        },
        "test_metrics_at_0_5": {
            "accuracy": 0.90,
            "average_precision": 0.92,
            "confusion_matrix": [[70, 5], [4, 21]],
        },
        "test_metrics_operational": {
            "threshold": 0.31,
            "f1": 0.84,
            "confusion_matrix": [[68, 7], [3, 22]],
        },
        "artifact_path": "artifacts/selected.joblib",
        "cv_results_path": "reports/cv_results.csv",
    }
    scenario = {
        "name": PRIMARY_SCENARIO,
        "role": "primary",
        "feature_definition": {
            "included_features": ["tipo_ies"],
            "excluded_features": {"estado": "direct proxy"},
        },
        "models": [candidate],
        "duration_seconds": 1.0,
    }
    registry.finish_success(
        run_id=RUN_ID,
        model_name="quality_risk_classifier",
        selected_algorithm="GradientBoostingClassifier",
        selected_summary=candidate,
        candidate_records=[candidate],
        scenario_records=[scenario],
        test_metrics=candidate["test_metrics"],
        operational_test_metrics=candidate["test_metrics_operational"],
        oof_metrics=candidate["oof_metrics_at_0_5"],
        artifact_path="artifacts/selected.joblib",
        artifact_sha256="artifact-hash",
        storage_result={"status": "skipped", "paths": {}},
        feature_schema={"target": "actual_label"},
        train_rows=80,
        test_rows=20,
        positive_rows=25,
        positive_rate=0.25,
        duration_seconds=1.0,
        operational_threshold=0.31,
        threshold_policy="maximize F2 on grouped OOF",
        metadata=metadata,
    )

    sql = "\n".join(statement for statement, _ in calls)
    assert "oof_metrics" in sql
    assert "operational_threshold" in sql
    assert "INSERT INTO mlops.model_candidates" in sql
    assert "INSERT INTO mlops.scenario_evaluations" in sql
    candidate_parameters = next(
        parameters
        for statement, parameters in calls
        if "INSERT INTO mlops.model_candidates" in statement
    )
    assert json.loads(candidate_parameters["best_params"]) == {
        "model__n_estimators": 200
    }
    assert json.loads(candidate_parameters["oof_metrics"])["average_precision"] == 0.90
