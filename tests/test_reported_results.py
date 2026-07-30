import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from src.ml.config import MODEL_NAMES, search_spaces_for_json
from src.ml.train import select_model_from_cv


def test_published_run_is_internally_consistent():
    latest = json.loads(
        Path("reports/modeling/latest_run.json").read_text(encoding="utf-8")
    )
    results_path = Path(latest["results_json"])
    results = json.loads(results_path.read_text(encoding="utf-8"))
    run_id = results["run_id"]

    assert latest["run_id"] == run_id
    assert latest["selected_model"] == results["selected_model"]
    method = results["method"]
    assert method["cv_folds"] == 5
    assert method["cv_shuffle"] is True
    assert method["cv_strategy"] == "StratifiedGroupKFold"
    assert method["groups"] == "natural_key"
    assert method["groups_passed_to_fit"] is True
    assert method["n_jobs"] == -1
    assert method["optimization_metric"] == "average_precision"
    assert method["random_state"] == 42
    assert method["refit"] is True
    assert method["search_iterations"] == 40
    assert method["search_method"] == "RandomizedSearchCV"
    assert method["test_size_target"] == 0.2
    assert method["test_used_for_selection"] is False
    assert method["reference_threshold"] == 0.5
    assert results["dataset"]["group_overlap_count"] == 0
    assert results["primary_scenario"] == "leakage_controlled"
    assert tuple(model["algorithm"] for model in results["models"]) == MODEL_NAMES
    assert results["selected_model"] == select_model_from_cv(results["models"])
    assert [scenario["name"] for scenario in results["scenarios"]] == [
        "leakage_controlled",
        "full_feature",
    ]
    assert [scenario["role"] for scenario in results["scenarios"]] == [
        "primary",
        "sensitivity",
    ]
    assert all(len(scenario["models"]) == 3 for scenario in results["scenarios"])

    search_spaces_path = (
        Path(results["paths"]["report_dir"]) / f"{run_id}_search_spaces.json"
    )
    assert (
        json.loads(search_spaces_path.read_text(encoding="utf-8"))
        == search_spaces_for_json()
    )

    comparison = pd.read_csv(results["paths"]["comparison_csv"])
    assert len(comparison) == 6
    assert comparison["scenario"].eq("leakage_controlled").sum() == 3
    assert comparison["scenario"].eq("full_feature").sum() == 3
    for model in results["models"]:
        cv_results = pd.read_csv(model["cv_results_path"])
        assert len(cv_results) == 40
        assert cv_results["mean_test_score"].notna().all()
        comparison_row = comparison.loc[
            (comparison["scenario"] == "leakage_controlled")
            & (comparison["algorithm"] == model["algorithm"])
        ].iloc[0]
        assert comparison_row["cv_average_precision_mean"] == pytest.approx(
            model["cv_mean"],
            abs=1e-15,
        )
        assert comparison_row["test_average_precision"] == pytest.approx(
            model["test_metrics_at_0_5"]["average_precision"],
            abs=1e-15,
        )
        assert len(model["oof_fold_metrics"]) == 5
        assert Path(model["oof_fold_metrics_path"]).exists()
        for curve_path in model["curve_paths"].values():
            path = Path(curve_path)
            assert path.exists()
            assert path.name.startswith(f"{run_id}_")

    manifest = json.loads(
        Path(results["paths"]["artifact_manifest"]).read_text(encoding="utf-8")
    )
    for item in manifest["files"]:
        path = Path(item["path"])
        if not path.exists():
            assert path.parts[0] == "artifacts"
            continue
        assert path.stat().st_size == item["size_bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]

    for documentation_path in [
        Path("README.md"),
        Path("reports/article.md"),
        Path("docs/hyperparameter_selection_report.md"),
        Path("docs/editorial_response_hyperparameters.md"),
        Path("docs/feature_ablation_report.md"),
        Path("docs/rule_catalog.md"),
        Path("docs/ai_article_review_guide.md"),
    ]:
        content = documentation_path.read_text(encoding="utf-8")
        assert run_id in content
        if documentation_path.name != "rule_catalog.md":
            assert results["selected_model"] in content or "Random Forest" in content
