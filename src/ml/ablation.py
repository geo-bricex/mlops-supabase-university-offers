"""Training-only categorical-encoding sensitivity analysis."""

import time
from typing import Any

import numpy as np
from scipy import sparse
from sklearn.base import clone

from src.ml.config import build_model_specs
from src.ml.group_validation import generate_grouped_oof_probabilities


def _matrix_memory_bytes(matrix: Any) -> int:
    if sparse.issparse(matrix):
        return int(matrix.data.nbytes + matrix.indices.nbytes + matrix.indptr.nbytes)
    return int(np.asarray(matrix).nbytes)


def _representation_profile(
    fitted_pipeline: Any,
    features,
) -> dict[str, Any]:
    transformed = fitted_pipeline.named_steps["preprocess"].transform(features)
    return {
        "output_sparse": bool(sparse.issparse(transformed)),
        "transformed_rows": int(transformed.shape[0]),
        "transformed_features": int(transformed.shape[1]),
        "transformed_matrix_bytes": _matrix_memory_bytes(transformed),
    }


def compare_categorical_encodings(
    *,
    algorithm: str,
    best_params: dict[str, Any],
    fitted_onehot_pipeline: Any,
    onehot_oof: dict[str, Any],
    scenario: str,
    features,
    target,
    groups,
    n_jobs: int,
) -> list[dict[str, Any]]:
    """Compare one-hot and ordinal representations on identical grouped folds."""
    rows = [
        {
            "algorithm": algorithm,
            "scenario": scenario,
            "encoding": "onehot",
            "compatible": True,
            "error": None,
            "oof_average_precision": onehot_oof["metrics"]["average_precision"],
            "oof_f1_at_0_5": onehot_oof["metrics"]["f1"],
            "oof_duration_seconds": onehot_oof["duration_seconds"],
            **_representation_profile(fitted_onehot_pipeline, features),
        }
    ]
    ordinal_spec = next(
        spec
        for spec in build_model_specs(
            estimator_n_jobs=n_jobs,
            scenario=scenario,
            encoding_strategy="ordinal",
        )
        if spec.name == algorithm
    )
    ordinal_pipeline = clone(ordinal_spec.pipeline).set_params(**best_params)
    started = time.perf_counter()
    try:
        ordinal_oof = generate_grouped_oof_probabilities(
            ordinal_pipeline,
            features,
            target,
            groups,
            n_jobs=n_jobs,
        )
        profile_pipeline = clone(ordinal_pipeline).fit(features, target)
        rows.append(
            {
                "algorithm": algorithm,
                "scenario": scenario,
                "encoding": "ordinal",
                "compatible": True,
                "error": None,
                "oof_average_precision": ordinal_oof["metrics"]["average_precision"],
                "oof_f1_at_0_5": ordinal_oof["metrics"]["f1"],
                "oof_duration_seconds": float(time.perf_counter() - started),
                **_representation_profile(profile_pipeline, features),
            }
        )
    except Exception as exc:  # noqa: BLE001
        rows.append(
            {
                "algorithm": algorithm,
                "scenario": scenario,
                "encoding": "ordinal",
                "compatible": False,
                "error": f"{type(exc).__name__}: {exc}",
                "oof_average_precision": None,
                "oof_f1_at_0_5": None,
                "oof_duration_seconds": float(time.perf_counter() - started),
                "output_sparse": None,
                "transformed_rows": None,
                "transformed_features": None,
                "transformed_matrix_bytes": None,
            }
        )
    return rows
