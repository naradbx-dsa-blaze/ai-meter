"""Token budget tracking — called before and after each proxy request."""
from . import database as db
from . import alerting
from .models import BudgetStatus
from .config import settings


def check_budget(user_id: str) -> BudgetStatus:
    """Return current budget status for the user without logging anything."""
    budget = db.get_user_budget(user_id)
    daily_limit = budget["daily_token_limit"]
    tokens_used = db.get_daily_usage(user_id)
    usage_pct = tokens_used / daily_limit if daily_limit > 0 else 0.0

    return BudgetStatus(
        user_id=user_id,
        tokens_used=tokens_used,
        daily_limit=daily_limit,
        usage_pct=usage_pct,
        exceeded=usage_pct >= 1.0,
        soft_alert_needed=False,
        hard_alert_needed=False,
    )


def record_usage_and_alert(
    user_id: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    endpoint_name: str,
) -> BudgetStatus:
    """Log usage, then evaluate and fire Slack alerts if thresholds are crossed."""
    budget = db.get_user_budget(user_id)
    daily_limit = budget["daily_token_limit"]

    db.log_usage(user_id, model_name, input_tokens, output_tokens, endpoint_name)

    tokens_used = db.get_daily_usage(user_id)
    usage_pct = tokens_used / daily_limit if daily_limit > 0 else 0.0

    soft_needed = (
        usage_pct >= settings.soft_alert_threshold
        and not db.was_alert_sent_today(user_id, "warning")
    )
    hard_needed = (
        usage_pct >= 1.0
        and not db.was_alert_sent_today(user_id, "exceeded")
    )

    # Determine which alert to send (hard takes priority)
    if hard_needed:
        sent = alerting.send_alert(
            user_id=user_id,
            alert_type="exceeded",
            tokens_used=tokens_used,
            daily_limit=daily_limit,
            slack_user_id=budget.get("slack_user_id"),
        )
        db.log_alert(user_id, "exceeded", tokens_used, daily_limit, sent)
    elif soft_needed:
        sent = alerting.send_alert(
            user_id=user_id,
            alert_type="warning",
            tokens_used=tokens_used,
            daily_limit=daily_limit,
            slack_user_id=budget.get("slack_user_id"),
        )
        db.log_alert(user_id, "warning", tokens_used, daily_limit, sent)

    return BudgetStatus(
        user_id=user_id,
        tokens_used=tokens_used,
        daily_limit=daily_limit,
        usage_pct=usage_pct,
        exceeded=usage_pct >= 1.0,
        soft_alert_needed=soft_needed,
        hard_alert_needed=hard_needed,
    )
