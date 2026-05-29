import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional

try:
    import jwt
except ImportError:
    jwt = None

try:
    from storage3 import create_client as create_storage_client
except ImportError:
    create_storage_client = None

logger = logging.getLogger("supabase_storage")
MAX_STORAGE_RETRIES = int(os.getenv("SUPABASE_STORAGE_RETRIES", "6"))
STORAGE_RETRY_SLEEP_SECONDS = float(os.getenv("SUPABASE_STORAGE_RETRY_SLEEP_SECONDS", "2"))


def _bool_env(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.lower() in ("1", "true", "yes")


def _response_data(resp):
    return getattr(resp, "data", resp) or []


def _build_storage_headers() -> Optional[Dict[str, str]]:
    if jwt is None:
        logger.warning("PyJWT is not installed; skipping storage uploads.")
        return None
    secret = os.getenv("JWT_SECRET")
    if not secret:
        logger.warning("JWT_SECRET missing; skipping storage uploads.")
        return None
    role = os.getenv("SUPABASE_STORAGE_ROLE", "supabase_storage_admin")
    now = int(time.time())
    token = jwt.encode(
        {
            "iss": "supabase",
            "ref": "local",
            "role": role,
            "iat": now,
            "exp": now + 3600,
        },
        secret,
        algorithm="HS256",
    )
    return {
        "Authorization": f"Bearer {token}",
        "apikey": token,
    }


def get_supabase_client():
    if create_storage_client is None:
        logger.warning("storage3 client not installed; skipping storage uploads.")
        return None
    url = os.getenv("SUPABASE_STORAGE_URL_INTERNAL", "http://storage:5000")
    headers = _build_storage_headers()
    if headers is None:
        return None
    if not url.endswith("/"):
        url = f"{url}/"
    return create_storage_client(url, headers, is_async=False, timeout=20)


def ensure_bucket(client, bucket: str, public: bool) -> bool:
    if client is None:
        return False
    for attempt in range(1, MAX_STORAGE_RETRIES + 1):
        try:
            resp = client.list_buckets()
            buckets = _response_data(resp)
            for item in buckets:
                if isinstance(item, dict):
                    name = item.get("name")
                else:
                    name = getattr(item, "name", None)
                if name == bucket:
                    return True
            client.create_bucket(bucket, options={"public": public})
            return True
        except Exception as exc:
            if attempt == MAX_STORAGE_RETRIES:
                logger.warning("Storage bucket check failed after %s attempts: %s", attempt, exc)
                return False
            time.sleep(STORAGE_RETRY_SLEEP_SECONDS)
    return False


def _public_url(base_url: str, bucket: str, object_path: str) -> str:
    return f"{base_url}/storage/v1/object/public/{bucket}/{object_path}"


def upload_file(client, bucket: str, local_path: Path, remote_path: str, content_type: str, public: bool) -> Dict[str, str]:
    data = local_path.read_bytes()
    client.from_(bucket).upload(
        remote_path,
        data,
        file_options={"content-type": content_type, "upsert": "true"},
    )
    info = {"path": remote_path}
    if public:
        base_url = os.getenv("SUPABASE_PUBLIC_URL") or os.getenv("SUPABASE_URL", "")
        base_url = base_url.rstrip("/")
        if base_url:
            info["url"] = _public_url(base_url, bucket, remote_path)
    return info


def upload_artifacts(file_id: str, source_path: str, report_dir: Path) -> Dict[str, object]:
    client = get_supabase_client()
    if client is None:
        return {"status": "skipped", "paths": {}}

    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "etl-artifacts")
    public = _bool_env(os.getenv("SUPABASE_STORAGE_PUBLIC", "false"))

    try:
        if not ensure_bucket(client, bucket, public):
            return {"status": "skipped", "paths": {}}
    except Exception as exc:
        logger.error(f"Failed to ensure bucket: {exc}")
        return {"status": "failed", "paths": {}}

    paths: Dict[str, Dict[str, str]] = {}
    try:
        source = Path(source_path)
        if source.exists():
            remote = f"sources/{file_id}/{source.name}"
            paths["source_file"] = upload_file(
                client,
                bucket,
                source,
                remote,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                public,
            )

        reports = {
            "data_quality_json": report_dir / "data_quality.json",
            "data_quality_html": report_dir / "data_quality.html",
            "inconsistencies_csv": report_dir / "inconsistencies.csv",
        }
        for key, path in reports.items():
            if not path.exists():
                continue
            if path.suffix == ".json":
                content_type = "application/json"
            elif path.suffix == ".html":
                content_type = "text/html"
            else:
                content_type = "text/csv"
            remote = f"reports/{file_id}/{path.name}"
            paths[key] = upload_file(client, bucket, path, remote, content_type, public)

        return {"status": "success", "paths": json.loads(json.dumps(paths))}
    except Exception as exc:
        logger.error(f"Storage upload failed: {exc}")
        return {"status": "failed", "paths": paths}


def upload_model_artifacts(run_id: str, model_version: str, artifact_paths: Dict[str, Path]) -> Dict[str, object]:
    client = get_supabase_client()
    if client is None:
        return {"status": "skipped", "paths": {}}

    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "etl-artifacts")
    public = _bool_env(os.getenv("SUPABASE_STORAGE_PUBLIC", "false"))

    try:
        if not ensure_bucket(client, bucket, public):
            return {"status": "skipped", "paths": {}}
    except Exception as exc:
        logger.error(f"Failed to ensure bucket for model upload: {exc}")
        return {"status": "failed", "paths": {}}

    paths: Dict[str, Dict[str, str]] = {}
    try:
        for key, path in artifact_paths.items():
            if not path.exists():
                continue
            if path.suffix == ".joblib":
                content_type = "application/octet-stream"
            elif path.suffix == ".json":
                content_type = "application/json"
            else:
                content_type = "text/plain"
            remote = f"models/{model_version}/{run_id}/{path.name}"
            paths[key] = upload_file(client, bucket, path, remote, content_type, public)
        return {"status": "success", "paths": json.loads(json.dumps(paths))}
    except Exception as exc:
        logger.error(f"Model storage upload failed: {exc}")
        return {"status": "failed", "paths": paths}
