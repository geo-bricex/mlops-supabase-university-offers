import argparse
import json
import logging
import os
import time
import urllib.request
from typing import Dict, Tuple

from sqlalchemy import text

from src.db.session import get_db_session

logger = logging.getLogger("ops_monitor")

DEFAULT_ENDPOINTS = {
    "auth": "http://auth:9999/health",
    "rest": "http://rest:3000/",
    "realtime": "http://realtime:4000/",
    "storage": "http://storage:5000/status",
    "studio": "http://studio:3000/",
    "meta": "http://meta:8080/",
    "analytics": "http://analytics:4000/",
    "kong": "http://kong:8000/",
}


def _load_endpoints() -> Dict[str, str]:
    raw = os.getenv("SUPABASE_HEALTH_ENDPOINTS")
    if raw:
        try:
            parsed = json.loads(raw)
            return {str(k): str(v) for k, v in parsed.items()}
        except json.JSONDecodeError:
            logger.warning("SUPABASE_HEALTH_ENDPOINTS is not valid JSON. Using defaults.")
    return DEFAULT_ENDPOINTS


def _probe(url: str, timeout: int) -> Tuple[str, int, float, dict]:
    start = time.time()
    status_code = None
    detail = {}
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status_code = response.getcode()
    except Exception as exc:
        detail["error"] = str(exc)
    latency_ms = (time.time() - start) * 1000.0
    if status_code is None:
        status = "down"
    else:
        status = "up" if status_code < 500 else "down"
    return status, status_code or 0, latency_ms, detail


def _record_health(service: str, url: str, status: str, code: int, latency_ms: float, detail: dict) -> None:
    with get_db_session() as session:
        session.execute(
            text(
                "INSERT INTO ops.service_health (service_name, endpoint, status, status_code, latency_ms, detail) "
                "VALUES (:service, :endpoint, :status, :code, :latency, :detail)"
            ),
            {
                "service": service,
                "endpoint": url,
                "status": status,
                "code": code,
                "latency": latency_ms,
                "detail": json.dumps(detail),
            },
        )


def run_once(timeout: int = 5) -> None:
    endpoints = _load_endpoints()
    for name, url in endpoints.items():
        status, code, latency_ms, detail = _probe(url, timeout)
        _record_health(name, url, status, code, latency_ms, detail)
        logger.info(f"{name}: {status} ({code}) {latency_ms:.1f}ms")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=0, help="Seconds between checks (0 = once).")
    parser.add_argument("--timeout", type=int, default=5, help="HTTP timeout in seconds.")
    args = parser.parse_args()

    if args.interval and args.interval > 0:
        while True:
            run_once(timeout=args.timeout)
            time.sleep(args.interval)
    else:
        run_once(timeout=args.timeout)


if __name__ == "__main__":
    main()
