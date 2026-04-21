import os
from pathlib import Path
from typing import Optional

# Load .env manually (no python-dotenv needed)
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())


def _load_dbx_profile(profile: str = "nara_dbx") -> tuple[str, str]:
    """Read host+token from ~/.databrickscfg as fallback when env vars aren't set."""
    cfg = Path.home() / ".databrickscfg"
    if not cfg.exists():
        return "", ""
    host, token, in_section = "", "", False
    for line in cfg.read_text().splitlines():
        if line.strip() == f"[{profile}]":
            in_section = True
            continue
        if in_section:
            if line.startswith("["):
                break
            if line.startswith("host"):
                host = line.split("=", 1)[1].strip()
            elif line.startswith("token"):
                token = line.split("=", 1)[1].strip()
    return host, token


_dbx_host_default, _dbx_token_default = _load_dbx_profile()


class Settings:
    # Databricks workspace
    databricks_host: str = os.getenv("DATABRICKS_HOST", _dbx_host_default or "https://e2-demo-field-eng.cloud.databricks.com")
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", _dbx_token_default)
    # Serverless warehouse required for Unity Catalog DDL
    databricks_sql_warehouse_id: str = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "e9b34f7a2e4b0561")

    # Unity Catalog location
    databricks_catalog: str = os.getenv("DATABRICKS_CATALOG", "nara_demo")
    databricks_schema: str = os.getenv("DATABRICKS_SCHEMA", "default")

    # FM serving endpoint to proxy
    fm_endpoint_name: str = os.getenv("FM_ENDPOINT_NAME", "databricks-meta-llama-3-1-70b-instruct")

    # Budget defaults
    default_daily_token_limit: int = int(os.getenv("DEFAULT_DAILY_TOKEN_LIMIT", "2000"))
    soft_alert_threshold: float = float(os.getenv("SOFT_ALERT_THRESHOLD", "0.8"))

    # Slack
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    slack_bot_token: Optional[str] = os.getenv("SLACK_BOT_TOKEN")
    slack_default_channel: str = os.getenv("SLACK_DEFAULT_CHANNEL", "#ai-usage-alerts")


settings = Settings()
