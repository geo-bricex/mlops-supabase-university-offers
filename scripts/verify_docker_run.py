"""Verify Docker/Supabase evidence and two-run reproducibility."""

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd


def _psql(query: str) -> list[list[str]]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-At",
            "-F",
            "\t",
            "-c",
            query,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.split("\t") for line in result.stdout.splitlines() if line.strip()]


def _load_results(run_id: str) -> dict[str, Any]:
    path = Path("reports/modeling") / run_id / f"{run_id}_results.json"
    if not path.exists():
        raise AssertionError(f"Missing public result JSON: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_runs() -> list[dict[str, Any]]:
    rows = _psql(
        """
        SELECT
            run_id::text, algorithm, best_params::text, best_score::text,
            train_rows::text, test_rows::text, dataset_rows::text,
            dataset_sha256, git_commit, primary_scenario
        FROM mlops.training_runs
        WHERE status = 'success'
        ORDER BY started_at DESC
        LIMIT 2;
        """
    )
    if len(rows) != 2:
        raise AssertionError("Exactly two recent successful runs are required.")
    keys = [
        "run_id",
        "algorithm",
        "best_params",
        "best_score",
        "train_rows",
        "test_rows",
        "dataset_rows",
        "dataset_sha256",
        "git_commit",
        "primary_scenario",
    ]
    return [dict(zip(keys, row)) for row in rows]


def _database_counts(run_id: str) -> dict[str, int]:
    row = _psql(
        f"""
        SELECT
            (SELECT COUNT(*) FROM mlops.model_candidates
             WHERE run_id = '{run_id}')::text,
            (SELECT COUNT(*) FROM mlops.model_candidates
             WHERE run_id = '{run_id}' AND model_status = 'selected')::text,
            (SELECT COUNT(*) FROM mlops.scenario_evaluations
             WHERE run_id = '{run_id}')::text,
            (SELECT COUNT(*) FROM mlops.predictions
             WHERE run_id = '{run_id}' AND prediction_origin = 'oof_train')::text,
            (SELECT COUNT(*) FROM mlops.predictions
             WHERE run_id = '{run_id}' AND prediction_origin = 'sealed_test')::text,
            (SELECT COUNT(*) FROM mlops.predictions
             WHERE run_id = '{run_id}'
               AND prediction_origin = 'production_inference')::text;
        """
    )[0]
    keys = [
        "primary_candidates",
        "selected_candidates",
        "scenario_evaluations",
        "oof_predictions",
        "sealed_test_predictions",
        "production_predictions",
    ]
    return {key: int(value) for key, value in zip(keys, row)}


def _compare_runs(
    first: dict[str, Any],
    second: dict[str, Any],
) -> dict[str, bool]:
    first_primary = next(
        row for row in first["models"] if row["algorithm"] == first["selected_model"]
    )
    second_primary = next(
        row for row in second["models"] if row["algorithm"] == second["selected_model"]
    )
    return {
        "dataset_sha256_equal": (
            first["dataset"]["dataset_sha256"] == second["dataset"]["dataset_sha256"]
        ),
        "partition_hashes_equal": (
            first["dataset"]["partition_group_sha256"]
            == second["dataset"]["partition_group_sha256"]
        ),
        "selected_model_equal": (first["selected_model"] == second["selected_model"]),
        "best_params_equal": (
            first_primary["best_params"] == second_primary["best_params"]
        ),
        "cv_metrics_equal": (
            first_primary["cv_mean"] == second_primary["cv_mean"]
            and first_primary["cv_std"] == second_primary["cv_std"]
        ),
        "test_metrics_equal": (
            first["selected_test_metrics_at_0_5"]
            == second["selected_test_metrics_at_0_5"]
        ),
        "operational_threshold_equal": (
            first["method"]["operational_threshold"]
            == second["method"]["operational_threshold"]
        ),
        "prediction_hash_equal": (
            first["prediction_provenance"]["sha256"]
            == second["prediction_provenance"]["sha256"]
        ),
    }


def verify(output_path: Path) -> dict[str, Any]:
    runs = _latest_runs()
    result_payloads = [_load_results(row["run_id"]) for row in runs]
    database = {row["run_id"]: _database_counts(row["run_id"]) for row in runs}
    for row, payload in zip(runs, result_payloads):
        counts = database[row["run_id"]]
        assert counts["primary_candidates"] == 3
        assert counts["selected_candidates"] == 1
        assert counts["scenario_evaluations"] == 6
        assert counts["oof_predictions"] == int(row["train_rows"])
        assert counts["sealed_test_predictions"] == int(row["test_rows"])
        assert counts["production_predictions"] == int(row["dataset_rows"])
        assert payload["run_id"] == row["run_id"]
        assert payload["selected_model"] == row["algorithm"]
        comparison = pd.read_csv(payload["paths"]["comparison_csv"])
        assert len(comparison) == 6
        assert comparison["scenario"].eq("leakage_controlled").sum() == 3

    comparison = _compare_runs(*result_payloads)
    if not all(comparison.values()):
        failures = [key for key, value in comparison.items() if not value]
        raise AssertionError(f"Reproducibility mismatch in: {', '.join(failures)}")
    llm = _psql(
        """
        SELECT interpretation_id::text, run_id::text, model_name,
               COALESCE(model_digest, ''), status, latency_seconds::text
        FROM mlops.llm_interpretation_runs
        ORDER BY created_at DESC
        LIMIT 1;
        """
    )
    if not llm or llm[0][4] != "success":
        raise AssertionError("No successful persisted Ollama interpretation.")
    evidence = {
        "validated_run_ids": [row["run_id"] for row in runs],
        "database_counts": database,
        "reproducibility": comparison,
        "llm_latest": {
            "interpretation_id": llm[0][0],
            "run_id": llm[0][1],
            "model_name": llm[0][2],
            "model_digest": llm[0][3],
            "status": llm[0][4],
            "latency_seconds": float(llm[0][5]),
        },
        "status": "success",
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/validation/docker_run.json"),
    )
    args = parser.parse_args()
    verify(args.output)


if __name__ == "__main__":
    main()
