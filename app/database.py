"""Delta table operations via Databricks SDK StatementExecution API."""
from datetime import date
from typing import Optional
import uuid

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

from .config import settings


def _client() -> WorkspaceClient:
    # When running as a Databricks App, token is empty and SDK uses OAuth automatically.
    # Locally, it uses the PAT from config / ~/.databrickscfg.
    if settings.databricks_token:
        return WorkspaceClient(host=settings.databricks_host, token=settings.databricks_token)
    return WorkspaceClient(host=settings.databricks_host)


def _esc(val) -> str:
    """Escape a Python value for safe SQL string interpolation."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"


def _run(sql: str, catalog: str = None, schema: str = None) -> tuple[list, list]:
    """Execute SQL; return (rows, column_names).
    Pass catalog/schema only when needed — omitting them avoids USE CATALOG
    permission checks for system.* queries on the app service principal.
    """
    w = _client()
    kwargs = dict(
        warehouse_id=settings.databricks_sql_warehouse_id,
        statement=sql,
        wait_timeout="30s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema

    result = w.statement_execution.execute_statement(**kwargs)
    if result.status.state.value not in ("SUCCEEDED",):
        err = result.status.error
        raise RuntimeError(f"SQL error [{result.status.state}]: {err}")

    rows = result.result.data_array or [] if result.result else []
    cols = []
    if result.manifest and result.manifest.schema and result.manifest.schema.columns:
        cols = [c.name for c in result.manifest.schema.columns]
    return rows, cols


_CAT = settings.databricks_catalog
_SCH = settings.databricks_schema


def _df(sql: str, catalog: str = None, schema: str = None) -> pd.DataFrame:
    rows, cols = _run(sql, catalog=catalog, schema=schema)
    df = pd.DataFrame(rows, columns=cols)
    # SDK returns everything as strings; coerce numeric-looking columns automatically.
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


# ── Budget CRUD ───────────────────────────────────────────────────────────────

def get_user_budget(user_id: str) -> dict:
    rows, _ = _run(
        f"SELECT daily_token_limit, slack_user_id, email, is_active "
        f"FROM ai_user_budgets WHERE user_id = {_esc(user_id)}",
        catalog=_CAT, schema=_SCH,
    )
    if rows:
        r = rows[0]
        return {
            "user_id": user_id,
            "daily_token_limit": int(r[0]) if r[0] is not None else settings.default_daily_token_limit,
            "slack_user_id": r[1],
            "email": r[2],
            "is_active": r[3] == "true" if isinstance(r[3], str) else bool(r[3]),
        }
    # Auto-create with default limit
    upsert_user_budget(
        user_id=user_id,
        daily_limit=settings.default_daily_token_limit,
        email=user_id if "@" in user_id else None,
    )
    return {
        "user_id": user_id,
        "daily_token_limit": settings.default_daily_token_limit,
        "slack_user_id": None,
        "email": user_id if "@" in user_id else None,
        "is_active": True,
    }


def upsert_user_budget(
    user_id: str,
    daily_limit: int,
    slack_user_id: Optional[str] = None,
    email: Optional[str] = None,
):
    rows, _ = _run(
        f"SELECT user_id FROM ai_user_budgets WHERE user_id = {_esc(user_id)}",
        catalog=_CAT, schema=_SCH,
    )
    if rows:
        _run(
            f"UPDATE ai_user_budgets "
            f"SET daily_token_limit = {_esc(daily_limit)}, "
            f"    slack_user_id = {_esc(slack_user_id)}, "
            f"    updated_at = current_timestamp() "
            f"WHERE user_id = {_esc(user_id)}",
            catalog=_CAT, schema=_SCH,
        )
    else:
        _run(
            f"INSERT INTO ai_user_budgets "
            f"(user_id, daily_token_limit, slack_user_id, email, is_active, created_at, updated_at) "
            f"VALUES ({_esc(user_id)}, {_esc(daily_limit)}, {_esc(slack_user_id)}, "
            f"        {_esc(email)}, true, current_timestamp(), current_timestamp())",
            catalog=_CAT, schema=_SCH,
        )


def list_all_budgets() -> pd.DataFrame:
    return _df(
        "SELECT user_id, daily_token_limit, slack_user_id, email, is_active, updated_at "
        "FROM ai_user_budgets ORDER BY user_id",
        catalog=_CAT, schema=_SCH,
    )


# ── Usage logging ─────────────────────────────────────────────────────────────

def get_daily_usage(user_id: str, for_date: Optional[date] = None) -> int:
    d = (for_date or date.today()).isoformat()
    rows, _ = _run(
        f"SELECT COALESCE(SUM(total_tokens), 0) "
        f"FROM ai_usage_logs "
        f"WHERE user_id = {_esc(user_id)} AND request_date = {_esc(d)}",
        catalog=_CAT, schema=_SCH,
    )
    return int(rows[0][0]) if rows and rows[0][0] is not None else 0


def log_usage(
    user_id: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    endpoint_name: str,
):
    total = input_tokens + output_tokens
    _run(
        f"INSERT INTO ai_usage_logs "
        f"(log_id, user_id, model_name, input_tokens, output_tokens, total_tokens, "
        f" request_date, request_timestamp, endpoint_name) "
        f"VALUES ({_esc(str(uuid.uuid4()))}, {_esc(user_id)}, {_esc(model_name)}, "
        f"        {_esc(input_tokens)}, {_esc(output_tokens)}, {_esc(total)}, "
        f"        current_date(), current_timestamp(), {_esc(endpoint_name)})",
        catalog=_CAT, schema=_SCH,
    )


# ── Alert deduplication ───────────────────────────────────────────────────────

def was_alert_sent_today(user_id: str, alert_type: str) -> bool:
    rows, _ = _run(
        f"SELECT COUNT(*) FROM ai_alert_logs "
        f"WHERE user_id = {_esc(user_id)} "
        f"  AND alert_type = {_esc(alert_type)} "
        f"  AND alert_date = current_date()",
        catalog=_CAT, schema=_SCH,
    )
    return bool(rows and int(rows[0][0]) > 0)


def log_alert(
    user_id: str,
    alert_type: str,
    tokens_used: int,
    daily_limit: int,
    slack_sent: bool,
):
    usage_pct = tokens_used / daily_limit if daily_limit > 0 else 0.0
    _run(
        f"INSERT INTO ai_alert_logs "
        f"(alert_id, user_id, alert_type, tokens_used, daily_limit, usage_pct, "
        f" alert_date, alert_timestamp, slack_sent) "
        f"VALUES ({_esc(str(uuid.uuid4()))}, {_esc(user_id)}, {_esc(alert_type)}, "
        f"        {_esc(tokens_used)}, {_esc(daily_limit)}, {_esc(usage_pct)}, "
        f"        current_date(), current_timestamp(), {_esc(slack_sent)})",
        catalog=_CAT, schema=_SCH,
    )


# ── Dashboard queries ─────────────────────────────────────────────────────────

def get_all_usage_today() -> pd.DataFrame:
    return _df(
        f"""
        SELECT
            l.user_id,
            CAST(COALESCE(SUM(l.total_tokens), 0) AS BIGINT)              AS tokens_used,
            CAST(COALESCE(b.daily_token_limit,
                 {settings.default_daily_token_limit}) AS INT)            AS daily_limit,
            ROUND(
                COALESCE(SUM(l.total_tokens), 0)
                / COALESCE(b.daily_token_limit,
                  {settings.default_daily_token_limit}) * 100, 1)         AS usage_pct
        FROM ai_usage_logs l
        LEFT JOIN ai_user_budgets b ON l.user_id = b.user_id
        WHERE l.request_date = current_date()
        GROUP BY l.user_id, b.daily_token_limit
        ORDER BY tokens_used DESC
        """,
        catalog=_CAT, schema=_SCH,
    )


def get_usage_trend(days: int = 7) -> pd.DataFrame:
    return _df(
        f"""
        SELECT request_date, user_id, SUM(total_tokens) AS tokens_used
        FROM ai_usage_logs
        WHERE request_date >= current_date() - INTERVAL {days} DAYS
        GROUP BY request_date, user_id
        ORDER BY request_date, user_id
        """,
        catalog=_CAT, schema=_SCH,
    )


def get_recent_alerts(limit: int = 20) -> pd.DataFrame:
    return _df(
        f"""
        SELECT user_id, alert_type, tokens_used, daily_limit,
               ROUND(usage_pct * 100, 1) AS usage_pct,
               alert_timestamp, slack_sent
        FROM ai_alert_logs
        ORDER BY alert_timestamp DESC
        LIMIT {limit}
        """,
        catalog=_CAT, schema=_SCH,
    )
