import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional

try:
    from supabase import create_client
except ImportError:
    create_client = None

logger = logging.getLogger("supabase_storage")


def _bool_env(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.lower() in ("1", "true", "yes")


def _response_data(resp):
    return getattr(resp, "data", resp) or []


def get_supabase_client():
    if create_client is None:
        logger.warning("supabase client not installed; skipping storage uploads.")
        return None
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        logger.warning("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing; skipping storage uploads.")
        return None
    if not url.endswith("/"):
        url = f"{url}/"
    return create_client(url, key)


def ensure_bucket(client, bucket: str, public: bool) -> bool:
    if client is None:
        return False
    resp = client.storage.list_buckets()
    buckets = _response_data(resp)
    for item in buckets:
        if isinstance(item, dict):
            name = item.get("name")
        else:
            name = getattr(item, "name", None)
        if name == bucket:
            return True
    client.storage.create_bucket(bucket, options={"public": public})
    return True


def _public_url(base_url: str, bucket: str, object_path: str) -> str:
    return f"{base_url}/storage/v1/object/public/{bucket}/{object_path}"


def upload_file(client, bucket: str, local_path: Path, remote_path: str, content_type: str, public: bool) -> Dict[str, str]:
    data = local_path.read_bytes()
    client.storage.from_(bucket).upload(
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
        ensure_bucket(client, bucket, public)
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
