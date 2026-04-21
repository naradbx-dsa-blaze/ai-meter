"""Slack alerting — uses httpx directly so no extra SDK needed."""
import json
import logging
from typing import Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)


def send_alert(
    user_id: str,
    alert_type: str,       # "warning" | "exceeded"
    tokens_used: int,
    daily_limit: int,
    slack_user_id: Optional[str] = None,
) -> bool:
    if not (settings.slack_webhook_url or settings.slack_bot_token):
        logger.warning("No Slack credentials configured — skipping alert for %s", user_id)
        return False

    usage_pct = round(tokens_used / daily_limit * 100, 1) if daily_limit else 0
    mention = f"<@{slack_user_id}>" if slack_user_id else f"`{user_id}`"

    if alert_type == "warning":
        emoji, title, color = "⚠️", "AI Token Budget Warning", "#FFA500"
        body = f"{mention} You've used *{usage_pct}%* of your daily token budget ({tokens_used:,} / {daily_limit:,} tokens)."
    else:
        emoji, title, color = "🚨", "AI Token Budget Exceeded", "#FF0000"
        body = f"{mention} You've *exceeded* your daily token budget ({tokens_used:,} / {daily_limit:,} tokens). Further requests will be blocked."

    payload = {
        "text": f"{emoji} {title}: {user_id} at {usage_pct}%",
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"*{emoji} {title}*\n{body}"},
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"User: `{user_id}` · Used: {tokens_used:,} · Limit: {daily_limit:,} tokens/day · {usage_pct}%",
                            }
                        ],
                    },
                ],
            }
        ],
    }

    try:
        if settings.slack_webhook_url:
            resp = httpx.post(settings.slack_webhook_url, content=json.dumps(payload), timeout=10)
            return resp.status_code == 200
        elif settings.slack_bot_token:
            resp = httpx.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {settings.slack_bot_token}"},
                json={**payload, "channel": settings.slack_default_channel},
                timeout=10,
            )
            return resp.json().get("ok", False)
    except Exception as e:
        logger.error("Slack alert failed: %s", e)

    return False
