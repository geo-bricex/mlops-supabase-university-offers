"""Run and persist a reproducible, interpretation-only Ollama request."""

import argparse
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import text

from src.db.init_db import ensure_schema
from src.db.session import get_db_session

logger = logging.getLogger("llm_audit")

DEFAULT_OUTPUT_DIR = Path("reports/llm")
SYSTEM_CONSTRAINT = (
    "You are a research-results interpreter. Use only the structured values "
    "provided. Do not classify records, audit data, invent causes, recommend "
    "automatic decisions, or present yourself as a source of truth. Explain "
    "the result in concise academic English and explicitly state that the "
    "deterministic rules and registered metrics remain authoritative."
)


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )


def _latest_structured_input() -> tuple[str, dict[str, Any]]:
    """Read only registered aggregate evidence for the active successful run."""
    with get_db_session() as session:
        row = (
            session.execute(
                text(
                    """
                    SELECT
                        run_id, model_name, model_version, algorithm,
                        optimization_metric, best_score, cv_metrics,
                        oof_metrics, metrics, operational_metrics,
                        operational_threshold, threshold_policy,
                        primary_scenario, dataset_sha256, dataset_rows,
                        class_distribution, random_state, sklearn_version,
                        git_commit
                    FROM mlops.training_runs
                    WHERE status = 'success'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                )
            )
            .mappings()
            .first()
        )
    if not row:
        raise RuntimeError("No successful training run is available.")
    payload = dict(row)
    payload["run_id"] = str(payload["run_id"])
    return payload["run_id"], payload


def _model_evidence(base_url: str, model_name: str) -> dict[str, Any]:
    tags_response = requests.get(f"{base_url}/api/tags", timeout=15)
    tags_response.raise_for_status()
    models = tags_response.json().get("models", [])
    tag = next(
        (
            item
            for item in models
            if item.get("name") == model_name or item.get("model") == model_name
        ),
        None,
    )
    if tag is None:
        raise RuntimeError(f"Ollama model {model_name!r} is not installed.")
    show_response = requests.post(
        f"{base_url}/api/show",
        json={"model": model_name},
        timeout=30,
    )
    show_response.raise_for_status()
    show = show_response.json()
    return {
        "name": model_name,
        "digest": tag.get("digest"),
        "size": tag.get("size"),
        "modified_at": tag.get("modified_at"),
        "details": tag.get("details"),
        "model_info": show.get("model_info"),
        "capabilities": show.get("capabilities"),
        "parameters": show.get("parameters"),
    }


def _build_prompt(structured_input: dict[str, Any]) -> str:
    """Create the fixed prompt whose exact hash is stored with the response."""
    return (
        f"{SYSTEM_CONSTRAINT}\n\n"
        "Summarize: (1) why the selected algorithm won, (2) grouped-CV and "
        "sealed-test performance, (3) the operational threshold, and (4) "
        "limitations. Do not infer record-level findings.\n\n"
        f"STRUCTURED_INPUT={_json(structured_input)}"
    )


def _persist(
    *,
    interpretation_id: str,
    run_id: str | None,
    model_evidence: dict[str, Any],
    configuration: dict[str, Any],
    prompt: str,
    structured_input: dict[str, Any],
    response: str | None,
    latency_seconds: float,
    status: str,
    error: str | None,
) -> None:
    with get_db_session() as session:
        session.execute(
            text(
                """
                INSERT INTO mlops.llm_interpretation_runs (
                    interpretation_id, run_id, model_name, model_digest,
                    model_details, configuration, prompt_sha256, prompt,
                    structured_input, response, latency_seconds, status, error
                )
                VALUES (
                    :interpretation_id, :run_id, :model_name, :model_digest,
                    CAST(:model_details AS JSONB),
                    CAST(:configuration AS JSONB), :prompt_sha256, :prompt,
                    CAST(:structured_input AS JSONB), :response,
                    :latency_seconds, :status, :error
                )
                """
            ),
            {
                "interpretation_id": interpretation_id,
                "run_id": run_id,
                "model_name": model_evidence["name"],
                "model_digest": model_evidence.get("digest"),
                "model_details": _json(model_evidence),
                "configuration": _json(configuration),
                "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                "prompt": prompt,
                "structured_input": _json(structured_input),
                "response": response,
                "latency_seconds": latency_seconds,
                "status": status,
                "error": error,
            },
        )


def run_interpretation(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    """Call Ollama once and persist full provenance, including failures."""
    ensure_schema()
    run_id, structured_input = _latest_structured_input()
    base_url = os.getenv(
        "OLLAMA_INTERNAL_URL",
        os.getenv("OLLAMA_URL", "http://ollama:11434"),
    ).rstrip("/")
    model_name = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
    configuration = {
        "temperature": 0.0,
        "seed": 42,
        "num_predict": int(os.getenv("OLLAMA_NUM_PREDICT", "220")),
        "stream": False,
        "keep_alive": os.getenv("OLLAMA_KEEP_ALIVE", "30m"),
    }
    timeout = float(os.getenv("OLLAMA_TIMEOUT", "180"))
    interpretation_id = str(uuid.uuid4())
    prompt = _build_prompt(structured_input)
    model_evidence: dict[str, Any] = {
        "name": model_name,
        "digest": None,
    }
    response_text = None
    error = None
    status = "failed"
    started = time.perf_counter()
    try:
        model_evidence = _model_evidence(base_url, model_name)
        response = requests.post(
            f"{base_url}/api/generate",
            json={
                "model": model_name,
                "prompt": prompt,
                "stream": False,
                "keep_alive": configuration["keep_alive"],
                "options": {
                    "temperature": configuration["temperature"],
                    "seed": configuration["seed"],
                    "num_predict": configuration["num_predict"],
                },
            },
            timeout=timeout,
        )
        response.raise_for_status()
        response_text = (response.json().get("response") or "").strip()
        if not response_text:
            raise RuntimeError("Ollama returned an empty response.")
        status = "success"
    except (requests.RequestException, RuntimeError) as exc:
        error = f"{type(exc).__name__}: {exc}"
        logger.exception("Ollama interpretation failed")
    latency_seconds = time.perf_counter() - started
    _persist(
        interpretation_id=interpretation_id,
        run_id=run_id,
        model_evidence=model_evidence,
        configuration=configuration,
        prompt=prompt,
        structured_input=structured_input,
        response=response_text,
        latency_seconds=latency_seconds,
        status=status,
        error=error,
    )
    result = {
        "interpretation_id": interpretation_id,
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "model": model_evidence,
        "configuration": configuration,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "structured_input": structured_input,
        "response": response_text,
        "latency_seconds": latency_seconds,
        "error": error,
        "role": "textual interpretation only",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{interpretation_id}_ollama.json"
    output_path.write_text(_json(result) + "\n", encoding="utf-8")
    logger.info("Ollama evidence written to %s", output_path)
    if status != "success":
        raise RuntimeError(error)
    return result


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Persist an interpretation-only Ollama reproducibility run."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    args = parser.parse_args()
    result = run_interpretation(args.output_dir)
    print(
        _json(
            {
                "interpretation_id": result["interpretation_id"],
                "run_id": result["run_id"],
                "status": result["status"],
                "model": result["model"]["name"],
                "digest": result["model"].get("digest"),
                "latency_seconds": result["latency_seconds"],
            }
        )
    )


if __name__ == "__main__":
    main()
