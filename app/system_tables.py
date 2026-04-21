"""Queries against Databricks system tables for workspace-wide AI usage."""
import pandas as pd
from .database import _df, _run, _CAT, _SCH


def get_workspace_summary(days: int = 7) -> pd.DataFrame:
    return _df(f"""
        SELECT
            DATE(event_time)              AS day,
            COUNT(DISTINCT requester)     AS active_users,
            COUNT(DISTINCT endpoint_name) AS endpoints_used,
            COUNT(*)                      AS total_requests,
            COALESCE(SUM(total_tokens), 0)AS total_tokens
        FROM system.ai_gateway.usage
        WHERE event_time >= current_date() - INTERVAL {days} DAYS
        GROUP BY 1
        ORDER BY 1 DESC
    """)


def get_user_usage_today() -> pd.DataFrame:
    return _df("""
        SELECT
            requester                          AS user_id,
            COUNT(*)                           AS requests,
            COUNT(DISTINCT endpoint_name)      AS models_used,
            COALESCE(SUM(input_tokens),  0)    AS input_tokens,
            COALESCE(SUM(output_tokens), 0)    AS output_tokens,
            COALESCE(SUM(total_tokens),  0)    AS total_tokens,
            MAX(event_time)                    AS last_seen
        FROM system.ai_gateway.usage
        WHERE DATE(event_time) = current_date()
        GROUP BY requester
        ORDER BY total_tokens DESC
    """)


def get_user_usage_trend(days: int = 7) -> pd.DataFrame:
    return _df(f"""
        SELECT
            DATE(event_time)           AS day,
            requester                  AS user_id,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            COUNT(*)                   AS requests
        FROM system.ai_gateway.usage
        WHERE event_time >= current_date() - INTERVAL {days} DAYS
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)


def get_model_breakdown_today() -> pd.DataFrame:
    return _df("""
        SELECT
            COALESCE(destination_model, endpoint_name) AS model,
            endpoint_name,
            COUNT(DISTINCT requester)      AS unique_users,
            COUNT(*)                       AS requests,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            COALESCE(AVG(latency_ms), 0)   AS avg_latency_ms
        FROM system.ai_gateway.usage
        WHERE DATE(event_time) = current_date()
        GROUP BY 1, 2
        ORDER BY total_tokens DESC
    """)


def get_user_model_heatmap(days: int = 7) -> pd.DataFrame:
    return _df(f"""
        SELECT
            requester                                   AS user_id,
            COALESCE(destination_model, endpoint_name)  AS model,
            COALESCE(SUM(total_tokens), 0)              AS total_tokens,
            COUNT(*)                                    AS requests
        FROM system.ai_gateway.usage
        WHERE event_time >= current_date() - INTERVAL {days} DAYS
        GROUP BY 1, 2
        ORDER BY total_tokens DESC
    """)


def get_hourly_trend_today() -> pd.DataFrame:
    return _df("""
        SELECT
            DATE_TRUNC('hour', event_time)     AS hour,
            COUNT(DISTINCT requester)          AS active_users,
            COUNT(*)                           AS requests,
            COALESCE(SUM(total_tokens), 0)     AS total_tokens
        FROM system.ai_gateway.usage
        WHERE DATE(event_time) = current_date()
        GROUP BY 1
        ORDER BY 1
    """)


def get_users_with_budget(default_limit: int) -> pd.DataFrame:
    """Join live system-table usage with configured budgets in Python to avoid cross-catalog SQL."""
    usage = _df("""
        SELECT
            requester                          AS user_id,
            COALESCE(SUM(total_tokens), 0)     AS total_tokens,
            COUNT(*)                           AS requests,
            COUNT(DISTINCT endpoint_name)      AS models_used,
            MAX(event_time)                    AS last_seen
        FROM system.ai_gateway.usage
        WHERE DATE(event_time) = current_date()
        GROUP BY requester
    """)

    rows, _ = _run(
        "SELECT user_id, daily_token_limit FROM ai_user_budgets",
        catalog=_CAT, schema=_SCH,
    )
    budgets = {r[0]: int(r[1]) for r in rows}

    if usage.empty:
        return usage

    usage["total_tokens"] = usage["total_tokens"].astype(float).astype(int)
    usage["daily_limit"]  = usage["user_id"].map(lambda u: budgets.get(u, default_limit))
    usage["pct_used"]     = (usage["total_tokens"] / usage["daily_limit"] * 100).round(1)
    return usage.sort_values("pct_used", ascending=False).reset_index(drop=True)
