from pydantic import BaseModel
from typing import Optional, List, Any, Dict


# ── Inbound proxy request (OpenAI-compatible) ────────────────────────────────

class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    model: Optional[str] = None
    messages: List[ChatMessage]
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    stream: Optional[bool] = False

    class Config:
        extra = "allow"   # pass any extra fields through to the endpoint


# ── Budget management ─────────────────────────────────────────────────────────

class BudgetUpsert(BaseModel):
    user_id: str
    daily_token_limit: int
    slack_user_id: Optional[str] = None
    email: Optional[str] = None


class BudgetResponse(BaseModel):
    user_id: str
    daily_token_limit: int
    slack_user_id: Optional[str]
    email: Optional[str]
    is_active: bool


# ── Usage ─────────────────────────────────────────────────────────────────────

class UsageResponse(BaseModel):
    user_id: str
    tokens_used_today: int
    daily_limit: int
    usage_pct: float
    status: str   # "ok" | "warning" | "exceeded"


# ── Token tracking result (internal) ─────────────────────────────────────────

class BudgetStatus(BaseModel):
    user_id: str
    tokens_used: int
    daily_limit: int
    usage_pct: float
    exceeded: bool
    soft_alert_needed: bool   # first time crossing 80% today
    hard_alert_needed: bool   # first time crossing 100% today
