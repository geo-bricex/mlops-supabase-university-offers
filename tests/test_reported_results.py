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
    assert results["method"] == {
        "cv_folds": 5,
        "cv_shuffle": True,
        "cv_strategy": "StratifiedKFold",
        "decision_threshold": 0.5,
        "n_jobs": -1,
        "optimization_metric": "average_precision",
        "random_state": 42,
        "refit": True,
        "search_iterations": 40,
        "search_method": "RandomizedSearchCV",
        "split": "stratified 80/20 train/test",
        "test_size": 0.2,
        "test_used_for_selection": False,
    }
    assert tuple(model["algorithm"] for model in results["models"]) == MODEL_NAMES
    assert results["selected_model"] == select_model_from_cv(results["models"])

    search_spaces_path = (
        Path(results["paths"]["report_dir"]) / f"{run_id}_search_spaces.json"
    )
    assert (
        json.loads(search_spaces_path.read_text(encoding="utf-8"))
        == search_spaces_for_json()
    )

    comparison = pd.read_csv(results["paths"]["comparison_csv"])
    assert len(comparison) == 3
    for model in results["models"]:
        cv_results = pd.read_csv(model["cv_results_path"])
        assert len(cv_results) == 40
        assert cv_results["mean_test_score"].notna().all()
        comparison_row = comparison.loc[
            comparison["algorithm"] == model["algorithm"]
        ].iloc[0]
        assert comparison_row["cv_average_precision_mean"] == pytest.approx(
            model["cv_mean"],
            abs=1e-15,
        )
        assert comparison_row["test_average_precision"] == pytest.approx(
            model["test_metrics"]["average_precision"],
            abs=1e-15,
        )
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
    ]:
        content = documentation_path.read_text(encoding="utf-8")
        assert run_id in content
        assert results["selected_model"] in content or ("Gradient Boosting" in content)
