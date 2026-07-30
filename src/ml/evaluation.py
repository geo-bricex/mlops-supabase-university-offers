"""Leakage-safe holdout evaluation and publication-ready plots."""

from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)

from src.ml.artifacts import portable_path

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def classification_metrics_from_scores(
    target: pd.Series,
    scores: np.ndarray,
    *,
    threshold: float = 0.5,
) -> tuple[dict[str, Any], np.ndarray]:
    """Calculate binary metrics from probabilities without model inference."""
    predictions = (scores >= threshold).astype(int)
    metrics = {
        "accuracy": float(accuracy_score(target, predictions)),
        "precision": float(precision_score(target, predictions, zero_division=0)),
        "recall": float(recall_score(target, predictions, zero_division=0)),
        "f1": float(f1_score(target, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(target, scores)),
        "average_precision": float(average_precision_score(target, scores)),
        "confusion_matrix": confusion_matrix(
            target,
            predictions,
            labels=[0, 1],
        ).tolist(),
        "threshold": float(threshold),
    }
    return metrics, predictions


def evaluate_fitted_pipeline(
    pipeline: Any,
    features: pd.DataFrame,
    target: pd.Series,
    *,
    threshold: float = 0.5,
) -> dict[str, Any]:
    """Evaluate one fitted pipeline without refitting or changing its threshold."""
    scores = pipeline.predict_proba(features)[:, 1]
    metrics, predictions = classification_metrics_from_scores(
        target,
        scores,
        threshold=threshold,
    )
    false_positive_rate, true_positive_rate, roc_thresholds = roc_curve(
        target,
        scores,
    )
    precision_values, recall_values, pr_thresholds = precision_recall_curve(
        target,
        scores,
    )
    return {
        "metrics": metrics,
        "roc_curve": pd.DataFrame(
            {
                "false_positive_rate": false_positive_rate,
                "true_positive_rate": true_positive_rate,
                "threshold": roc_thresholds,
            }
        ),
        "precision_recall_curve": pd.DataFrame(
            {
                "precision": precision_values,
                "recall": recall_values,
                "threshold": list(pr_thresholds) + [float("nan")],
            }
        ),
        "scores": scores,
        "predictions": predictions,
    }


def save_evaluation_outputs(
    algorithm: str,
    evaluation: dict[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    """Persist numerical curve points and their ROC/PR visualizations."""
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = algorithm.lower().replace("classifier", "").replace("regression", "")
    prefix = output_dir.name

    roc_data_path = output_dir / f"{prefix}_{slug}_roc_curve.csv"
    pr_data_path = output_dir / f"{prefix}_{slug}_precision_recall_curve.csv"
    roc_plot_path = output_dir / f"{prefix}_{slug}_roc_curve.png"
    pr_plot_path = output_dir / f"{prefix}_{slug}_precision_recall_curve.png"

    evaluation["roc_curve"].to_csv(roc_data_path, index=False)
    evaluation["precision_recall_curve"].to_csv(pr_data_path, index=False)

    figure, axis = plt.subplots(figsize=(6.4, 4.8))
    axis.plot(
        evaluation["roc_curve"]["false_positive_rate"],
        evaluation["roc_curve"]["true_positive_rate"],
        label=f"ROC AUC = {evaluation['metrics']['roc_auc']:.4f}",
    )
    axis.plot([0, 1], [0, 1], linestyle="--", color="gray")
    axis.set(
        xlabel="False positive rate",
        ylabel="True positive rate",
        title=f"ROC curve — {algorithm}",
    )
    axis.legend(loc="lower right")
    axis.grid(alpha=0.2)
    figure.tight_layout()
    figure.savefig(roc_plot_path, dpi=160)
    plt.close(figure)

    figure, axis = plt.subplots(figsize=(6.4, 4.8))
    axis.plot(
        evaluation["precision_recall_curve"]["recall"],
        evaluation["precision_recall_curve"]["precision"],
        label=(f"Average Precision = {evaluation['metrics']['average_precision']:.4f}"),
    )
    axis.set(
        xlabel="Recall",
        ylabel="Precision",
        title=f"Precision–Recall curve — {algorithm}",
    )
    axis.legend(loc="lower left")
    axis.grid(alpha=0.2)
    figure.tight_layout()
    figure.savefig(pr_plot_path, dpi=160)
    plt.close(figure)

    return {
        "roc_curve_csv": portable_path(roc_data_path),
        "precision_recall_curve_csv": portable_path(pr_data_path),
        "roc_curve_png": portable_path(roc_plot_path),
        "precision_recall_curve_png": portable_path(pr_plot_path),
    }
