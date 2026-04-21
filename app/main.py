"""FastAPI proxy — sits in front of a Databricks FM serving endpoint."""
import logging
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import settings
from .models import BudgetUpsert, BudgetResponse, UsageResponse
from . import database as db
from . import tracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Meter", version="1.0.0")

FM_ENDPOINT_URL = (
    f"{settings.databricks_host.rstrip('/')}"
    f"/serving-endpoints/{settings.fm_endpoint_name}/invocations"
)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "endpoint": FM_ENDPOINT_URL}


# ── Proxy ─────────────────────────────────────────────────────────────────────

@app.post("/v1/chat/completions")
async def proxy_chat(request: Request):
    """
    Drop-in OpenAI-compatible proxy.
    Pass `X-User-ID: <email_or_id>` in the request header.
    """
    user_id = request.headers.get("X-User-ID", "anonymous")
    body: Dict[str, Any] = await request.json()

    # ── Pre-flight budget check ───────────────────────────────────────────────
    status = tracker.check_budget(user_id)
    if status.exceeded:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "daily_token_limit_exceeded",
                "user_id": user_id,
                "tokens_used": status.tokens_used,
                "daily_limit": status.daily_limit,
                "message": "You have exceeded your daily token budget. Try again tomorrow.",
            },
        )

    # ── Forward to FM endpoint ────────────────────────────────────────────────
    headers = {
        "Authorization": f"Bearer {settings.databricks_token}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(FM_ENDPOINT_URL, json=body, headers=headers)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="FM endpoint timed out")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"FM endpoint unreachable: {e}")

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    result = resp.json()

    # ── Extract token counts and log ──────────────────────────────────────────
    usage = result.get("usage", {})
    input_tokens = usage.get("prompt_tokens", 0)
    output_tokens = usage.get("completion_tokens", 0)
    model_name = result.get("model", body.get("model", settings.fm_endpoint_name))

    tracker.record_usage_and_alert(
        user_id=user_id,
        model_name=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        endpoint_name=settings.fm_endpoint_name,
    )

    logger.info(
        "user=%s tokens=%d (in=%d out=%d)",
        user_id, input_tokens + output_tokens, input_tokens, output_tokens,
    )

    return JSONResponse(content=result)


# ── Usage endpoints ───────────────────────────────────────────────────────────

@app.get("/v1/usage/{user_id}", response_model=UsageResponse)
async def get_usage(user_id: str):
    budget = db.get_user_budget(user_id)
    tokens_used = db.get_daily_usage(user_id)
    daily_limit = budget["daily_token_limit"]
    usage_pct = tokens_used / daily_limit if daily_limit > 0 else 0.0

    if usage_pct >= 1.0:
        status_str = "exceeded"
    elif usage_pct >= settings.soft_alert_threshold:
        status_str = "warning"
    else:
        status_str = "ok"

    return UsageResponse(
        user_id=user_id,
        tokens_used_today=tokens_used,
        daily_limit=daily_limit,
        usage_pct=round(usage_pct * 100, 1),
        status=status_str,
    )


@app.get("/v1/usage")
async def get_all_usage():
    df = db.get_all_usage_today()
    return df.to_dict(orient="records")


# ── Budget endpoints ──────────────────────────────────────────────────────────

@app.post("/v1/budgets", response_model=BudgetResponse)
async def set_budget(payload: BudgetUpsert):
    db.upsert_user_budget(
        user_id=payload.user_id,
        daily_limit=payload.daily_token_limit,
        slack_user_id=payload.slack_user_id,
        email=payload.email,
    )
    budget = db.get_user_budget(payload.user_id)
    return BudgetResponse(**budget)


@app.get("/v1/budgets/{user_id}", response_model=BudgetResponse)
async def get_budget(user_id: str):
    return BudgetResponse(**db.get_user_budget(user_id))


@app.get("/v1/budgets")
async def list_budgets():
    df = db.list_all_budgets()
    return df.to_dict(orient="records")
