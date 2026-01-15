import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from src.db.session import engine

logger = logging.getLogger("db_init")


def _default_sql_path() -> Path:
    return Path(__file__).resolve().parents[2] / "sql" / "init.sql"


def _default_bootstrap_path() -> Path:
    return Path(__file__).resolve().parents[2] / "sql" / "supabase_bootstrap.sql"


def _table_exists(conn, schema: str, table: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
            """
        ),
        {"schema": schema, "table": table},
    ).first()
    return result is not None


def _routine_exists(conn, schema: str, routine: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.routines
            WHERE routine_schema = :schema AND routine_name = :routine
            """
        ),
        {"schema": schema, "routine": routine},
    ).first()
    return result is not None


def _execute_sql_file(conn, sql_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    for statement in statements:
        conn.exec_driver_sql(statement)


def _split_sql_statements(sql_text: str) -> list:
    statements = []
    buf = []
    i = 0
    length = len(sql_text)
    in_single = False
    in_double = False
    dollar_tag = None

    while i < length:
        ch = sql_text[i]

        if dollar_tag:
            end = sql_text.find(dollar_tag, i)
            if end == -1:
                buf.append(sql_text[i:])
                i = length
            else:
                buf.append(sql_text[i:end + len(dollar_tag)])
                i = end + len(dollar_tag)
                dollar_tag = None
            continue

        if not in_single and not in_double:
            if sql_text.startswith("--", i):
                end = sql_text.find("\n", i)
                if end == -1:
                    break
                buf.append(sql_text[i:end])
                i = end
                continue
            if sql_text.startswith("/*", i):
                end = sql_text.find("*/", i + 2)
                if end == -1:
                    break
                buf.append(sql_text[i:end + 2])
                i = end + 2
                continue

        if not in_double and ch == "'":
            if in_single and i + 1 < length and sql_text[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue
        if not in_single and ch == '"':
            if in_double and i + 1 < length and sql_text[i + 1] == '"':
                buf.append('""')
                i += 2
                continue
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        if not in_single and not in_double and ch == "$":
            end = sql_text.find("$", i + 1)
            if end != -1:
                tag = sql_text[i:end + 1]
                buf.append(tag)
                i = end + 1
                dollar_tag = tag
                continue

        if ch == ";" and not in_single and not in_double:
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def ensure_schema(engine_to_use=engine, sql_path: Optional[Path] = None) -> bool:
    """
    Ensure required schemas/tables exist. Returns True if initialization ran.
    """
    path = sql_path or _default_sql_path()
    bootstrap_path = _default_bootstrap_path()
    if not path.exists():
        raise FileNotFoundError(f"Init SQL not found at {path}")

    with engine_to_use.begin() as conn:
        if bootstrap_path.exists():
            _execute_sql_file(conn, bootstrap_path)
        core_exists = _table_exists(conn, "core", "dim_territory")
        ops_exists = _table_exists(conn, "ops", "service_health")
        ops_steps_exists = _table_exists(conn, "ops", "etl_step_metrics")
        rpc_exists = _routine_exists(conn, "core", "rpc_ingestion_series")
        if core_exists and ops_exists and ops_steps_exists and rpc_exists:
            logger.info("Schema already present. Skipping init.")
            return False
        logger.info("Schema missing or needs update. Running init SQL.")
        _execute_sql_file(conn, path)
    logger.info("Schema initialization complete.")
    return True


def main() -> None:
    ensure_schema()


if __name__ == "__main__":
    main()
