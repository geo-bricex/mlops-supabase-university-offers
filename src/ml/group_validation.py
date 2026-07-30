"""Leakage-safe grouped splitting, OOF prediction, and threshold selection."""

import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, f1_score, fbeta_score
from sklearn.model_selection import cross_val_predict

from src.ml.config import (
    RANDOM_STATE,
    REFERENCE_THRESHOLD,
    TEST_SIZE,
    build_cv,
)
from src.ml.evaluation import classification_metrics_from_scores


@dataclass(frozen=True)
class GroupedHoldout:
    """Index boundary for one reproducible group-disjoint holdout."""

    train_index: pd.Index
    test_index: pd.Index
    selected_fold: int
    row_test_fraction: float
    group_test_fraction: float
    overall_positive_rate: float
    train_positive_rate: float
    test_positive_rate: float


def assert_group_disjoint(
    train_groups: pd.Series,
    test_groups: pd.Series,
) -> None:
    """Raise when any group crosses a declared evaluation boundary."""
    overlap = set(train_groups.astype(str)) & set(test_groups.astype(str))
    if overlap:
        preview = sorted(overlap)[:5]
        raise RuntimeError(
            f"Grouped split leakage detected; shared groups include {preview}."
        )


def grouped_holdout_indices(
    features: pd.DataFrame,
    target: pd.Series,
    groups: pd.Series,
) -> GroupedHoldout:
    """Choose an approximately 80/20 stratified fold with disjoint groups."""
    if not (
        features.index.equals(target.index) and features.index.equals(groups.index)
    ):
        raise ValueError("Features, target, and groups must share the same index.")
    splitter = build_cv()
    overall_rate = float(target.mean())
    candidates = []
    for fold, (train_positions, test_positions) in enumerate(
        splitter.split(features, target, groups=groups)
    ):
        test_target = target.iloc[test_positions]
        row_fraction = len(test_positions) / len(features)
        prevalence_delta = abs(float(test_target.mean()) - overall_rate)
        size_delta = abs(row_fraction - TEST_SIZE)
        candidates.append(
            (
                size_delta + prevalence_delta,
                prevalence_delta,
                size_delta,
                fold,
                train_positions,
                test_positions,
            )
        )
    (
        _,
        _,
        _,
        selected_fold,
        train_positions,
        test_positions,
    ) = min(candidates, key=lambda item: item[:4])
    train_index = features.index[train_positions]
    test_index = features.index[test_positions]
    train_groups = groups.loc[train_index]
    test_groups = groups.loc[test_index]
    assert_group_disjoint(train_groups, test_groups)
    return GroupedHoldout(
        train_index=train_index,
        test_index=test_index,
        selected_fold=int(selected_fold),
        row_test_fraction=float(len(test_index) / len(features)),
        group_test_fraction=float(test_groups.nunique() / groups.astype(str).nunique()),
        overall_positive_rate=overall_rate,
        train_positive_rate=float(target.loc[train_index].mean()),
        test_positive_rate=float(target.loc[test_index].mean()),
    )


def grouped_fold_assignments(
    features: pd.DataFrame,
    target: pd.Series,
    groups: pd.Series,
) -> pd.Series:
    """Return each row's deterministic grouped CV validation fold."""
    assignments = pd.Series(-1, index=features.index, dtype=int)
    seen_validation_groups: set[str] = set()
    for fold, (train_positions, validation_positions) in enumerate(
        build_cv().split(features, target, groups=groups)
    ):
        train_groups = groups.iloc[train_positions]
        validation_groups = groups.iloc[validation_positions]
        assert_group_disjoint(train_groups, validation_groups)
        validation_group_set = set(validation_groups.astype(str))
        if seen_validation_groups & validation_group_set:
            raise RuntimeError("A group was assigned to more than one OOF fold.")
        seen_validation_groups.update(validation_group_set)
        assignments.iloc[validation_positions] = fold
    if (assignments < 0).any():
        raise RuntimeError("At least one training row has no OOF fold.")
    if assignments.groupby(groups.astype(str)).nunique().max() != 1:
        raise RuntimeError("Rows from the same group received different folds.")
    return assignments


def generate_grouped_oof_probabilities(
    estimator: Any,
    features: pd.DataFrame,
    target: pd.Series,
    groups: pd.Series,
    *,
    n_jobs: int,
    threshold: float = REFERENCE_THRESHOLD,
) -> dict[str, Any]:
    """Generate true out-of-fold probabilities and fold-level metrics."""
    fold_ids = grouped_fold_assignments(features, target, groups)
    started = time.perf_counter()
    probability_matrix = cross_val_predict(
        estimator,
        features,
        target,
        groups=groups,
        cv=build_cv(),
        method="predict_proba",
        n_jobs=n_jobs,
    )
    duration = time.perf_counter() - started
    scores = np.asarray(probability_matrix)[:, 1]
    if not np.isfinite(scores).all():
        raise RuntimeError("OOF prediction produced non-finite probabilities.")

    fold_metrics = []
    for fold in sorted(fold_ids.unique()):
        mask = fold_ids == fold
        fold_target = target.loc[mask]
        fold_scores = scores[mask.to_numpy()]
        fold_metrics.append(
            {
                "fold": int(fold),
                "rows": int(mask.sum()),
                "groups": int(groups.loc[mask].nunique()),
                "average_precision": float(
                    average_precision_score(fold_target, fold_scores)
                ),
                "f1": float(
                    f1_score(
                        fold_target,
                        fold_scores >= threshold,
                        zero_division=0,
                    )
                ),
            }
        )
    metrics, predictions = classification_metrics_from_scores(
        target,
        scores,
        threshold=threshold,
    )
    return {
        "scores": scores,
        "predictions": predictions,
        "fold_ids": fold_ids,
        "fold_metrics": fold_metrics,
        "metrics": metrics,
        "ap_mean": float(np.mean([row["average_precision"] for row in fold_metrics])),
        "ap_std": float(
            np.std(
                [row["average_precision"] for row in fold_metrics],
                ddof=0,
            )
        ),
        "f1_mean": float(np.mean([row["f1"] for row in fold_metrics])),
        "f1_std": float(np.std([row["f1"] for row in fold_metrics], ddof=0)),
        "duration_seconds": float(duration),
    }


def select_f2_threshold(
    target: pd.Series,
    oof_scores: np.ndarray,
) -> dict[str, Any]:
    """Select an operational threshold from OOF training scores only."""
    rows = []
    for threshold in np.round(np.arange(0.05, 0.951, 0.01), 2):
        predictions = oof_scores >= threshold
        metrics, _ = classification_metrics_from_scores(
            target,
            oof_scores,
            threshold=float(threshold),
        )
        rows.append(
            {
                "threshold": float(threshold),
                "f2": float(
                    fbeta_score(
                        target,
                        predictions,
                        beta=2,
                        zero_division=0,
                    )
                ),
                "precision": metrics["precision"],
                "recall": metrics["recall"],
                "f1": metrics["f1"],
            }
        )
    selected = max(
        rows,
        key=lambda row: (row["f2"], row["recall"], -row["threshold"]),
    )
    return {
        "policy": (
            "Maximize F2 on grouped OOF training probabilities over the "
            "predeclared grid 0.05..0.95 in increments of 0.01; ties prefer "
            "higher recall and then the lower threshold."
        ),
        "objective": "F2",
        "beta": 2,
        "grid_start": 0.05,
        "grid_stop": 0.95,
        "grid_step": 0.01,
        "selected_threshold": selected["threshold"],
        "selected_oof_metrics": selected,
        "candidates": rows,
        "random_state": RANDOM_STATE,
    }
