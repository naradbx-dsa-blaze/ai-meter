"""Run once to create the three Delta tables in Unity Catalog."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config import settings
from app.database import _run

CATALOG = settings.databricks_catalog
SCHEMA = settings.databricks_schema

TABLES = [
    (
        "ai_usage_logs",
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ai_usage_logs (
            log_id            STRING    NOT NULL,
            user_id           STRING    NOT NULL,
            model_name        STRING,
            input_tokens      INT,
            output_tokens     INT,
            total_tokens      INT,
            request_date      DATE,
            request_timestamp TIMESTAMP,
            endpoint_name     STRING
        )
        USING DELTA
        PARTITIONED BY (request_date)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """,
    ),
    (
        "ai_user_budgets",
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ai_user_budgets (
            user_id           STRING  NOT NULL,
            daily_token_limit INT     NOT NULL,
            slack_user_id     STRING,
            email             STRING,
            is_active         BOOLEAN,
            created_at        TIMESTAMP,
            updated_at        TIMESTAMP
        )
        USING DELTA
        """,
    ),
    (
        "ai_alert_logs",
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ai_alert_logs (
            alert_id        STRING  NOT NULL,
            user_id         STRING  NOT NULL,
            alert_type      STRING  NOT NULL,
            tokens_used     INT,
            daily_limit     INT,
            usage_pct       DOUBLE,
            alert_date      DATE,
            alert_timestamp TIMESTAMP,
            slack_sent      BOOLEAN
        )
        USING DELTA
        PARTITIONED BY (alert_date)
        """,
    ),
]


def main():
    print(f"Initializing tables in {CATALOG}.{SCHEMA} ...")
    for name, ddl in TABLES:
        print(f"  Creating {CATALOG}.{SCHEMA}.{name} ...", end=" ", flush=True)
        _run(ddl)
        print("ok")
    print("\nAll tables ready.")


if __name__ == "__main__":
    main()
