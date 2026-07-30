"""Experiment artifact persistence and integrity metadata."""

import hashlib
import json
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


def json_default(value: Any) -> Any:
    """Convert common scientific Python objects into stable JSON values."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    """Write readable, deterministic UTF-8 JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            default=json_default,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def file_sha256(path: Path) -> str:
    """Calculate the SHA-256 digest of an artifact."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def portable_path(path: Path) -> str:
    """Represent local paths consistently in cross-platform metadata."""
    return path.as_posix()


@dataclass(frozen=True)
class ExperimentPaths:
    """Filesystem layout for one immutable experiment run."""

    run_id: str
    model_dir: Path
    report_dir: Path

    @classmethod
    def create(
        cls,
        run_id: str,
        *,
        artifact_root: Path,
        report_root: Path,
    ) -> "ExperimentPaths":
        model_dir = artifact_root / run_id
        report_dir = report_root / run_id
        model_dir.mkdir(parents=True, exist_ok=False)
        report_dir.mkdir(parents=True, exist_ok=False)
        return cls(run_id=run_id, model_dir=model_dir, report_dir=report_dir)


def save_search_results(
    algorithm: str,
    search: Any,
    paths: ExperimentPaths,
) -> dict[str, str]:
    """Persist a fitted best pipeline and the complete ``cv_results_`` table."""
    slug = algorithm.lower()
    pipeline_path = paths.model_dir / f"{paths.run_id}_{slug}_best_pipeline.joblib"
    cv_path = paths.report_dir / f"{paths.run_id}_{slug}_cv_results.csv"
    joblib.dump(search.best_estimator_, pipeline_path)

    cv_results = pd.DataFrame(search.cv_results_).copy()
    if "params" in cv_results:
        cv_results["params"] = cv_results["params"].apply(
            lambda value: json.dumps(
                value,
                sort_keys=True,
                default=json_default,
            )
        )
    cv_results.to_csv(cv_path, index=False)
    return {
        "best_pipeline": portable_path(pipeline_path),
        "cv_results": portable_path(cv_path),
    }


def save_selected_model(
    selected_pipeline_path: Path,
    paths: ExperimentPaths,
) -> Path:
    """Create a stable final-model alias without fitting a second time."""
    final_path = paths.model_dir / f"{paths.run_id}_selected_model.joblib"
    shutil.copy2(selected_pipeline_path, final_path)
    return final_path


def build_manifest(paths: ExperimentPaths, files: Iterable[Path]) -> dict[str, Any]:
    """Build an integrity manifest for all material outputs."""
    entries = []
    for path in sorted(set(files), key=lambda item: str(item)):
        if not path.exists() or not path.is_file():
            continue
        entries.append(
            {
                "path": portable_path(path),
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    return {
        "run_id": paths.run_id,
        "files": entries,
    }
