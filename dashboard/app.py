"""AI Meter — workspace-wide AI usage dashboard powered by system.ai_gateway.usage."""
import sys, os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.config import settings
from app import database as db
from app import system_tables as st_tables

st.set_page_config(page_title="AI Meter", page_icon="🤖", layout="wide")

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🤖 AI Meter")
st.caption(f"Live workspace AI usage · `{settings.databricks_host.rstrip('/')}`")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    trend_days = st.slider("Trend window (days)", 3, 30, 7)
    st.divider()

    st.subheader("⚙️ Set User Budget")
    with st.form("budget_form"):
        uid   = st.text_input("User email")
        limit = st.number_input("Daily token limit", min_value=100,
                                value=settings.default_daily_token_limit, step=500)
        slack = st.text_input("Slack user ID (optional)", placeholder="U0123ABCD")
        if st.form_submit_button("Save"):
            if uid:
                db.upsert_user_budget(uid, int(limit), slack or None,
                                      uid if "@" in uid else None)
                st.success(f"Saved budget for `{uid}`")

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load():
    return {
        "today":   st_tables.get_user_usage_today(),
        "summary": st_tables.get_workspace_summary(trend_days),
        "models":  st_tables.get_model_breakdown_today(),
        "trend":   st_tables.get_user_usage_trend(trend_days),
        "hourly":  st_tables.get_hourly_trend_today(),
        "budgets": st_tables.get_users_with_budget(settings.default_daily_token_limit),
    }

try:
    data = load()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    st.stop()

today_df   = data["today"]
summary_df = data["summary"]
models_df  = data["models"]
trend_df   = data["trend"]
hourly_df  = data["hourly"]
budgets_df = data["budgets"]

# ── KPI row ───────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)

def _n(val) -> int:
    """Safely coerce any value to int."""
    try: return int(float(val))
    except: return 0

total_users    = len(today_df)
total_tokens   = _n(pd.to_numeric(today_df["total_tokens"], errors="coerce").sum()) if not today_df.empty else 0
total_requests = _n(pd.to_numeric(today_df["requests"],     errors="coerce").sum()) if not today_df.empty else 0

yesterday   = summary_df[summary_df["day"].astype(str) != str(date.today())]
prev_tokens = _n(pd.to_numeric(yesterday["total_tokens"], errors="coerce").iloc[0]) if not yesterday.empty else 0
token_delta = total_tokens - prev_tokens if prev_tokens else None

at_risk  = int((pd.to_numeric(budgets_df["pct_used"], errors="coerce") >= 80).sum())  if not budgets_df.empty else 0
exceeded = int((pd.to_numeric(budgets_df["pct_used"], errors="coerce") >= 100).sum()) if not budgets_df.empty else 0

c1.metric("Active Users Today",       total_users)
c2.metric("Total Tokens Today",       f"{total_tokens:,}",
          delta=f"{token_delta:+,} vs yesterday" if token_delta else None)
c3.metric("Total Requests",           f"{total_requests:,}")
c4.metric("At Budget Warning (≥80%)", at_risk,  delta_color="inverse")
c5.metric("Budget Exceeded",          exceeded, delta_color="inverse")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["👥 Users", "🤖 Models", "📈 Trends", "⚠️ Budgets & Alerts", "📋 Raw Data"]
)

# ─── Users tab ────────────────────────────────────────────────────────────────
with tab1:
    st.subheader("User Token Consumption — Today")

    if today_df.empty:
        st.info("No AI activity recorded today yet.")
    else:
        # Top-10 bar chart
        top = today_df.head(15).copy()
        top["total_tokens"] = pd.to_numeric(top["total_tokens"], errors="coerce").fillna(0)
        top["label"] = top["user_id"].str.split("@").str[0]
        fig = px.bar(
            top, x="label", y="total_tokens",
            text=top["total_tokens"].apply(lambda x: f"{_n(x):,}"),
            color="total_tokens",
            color_continuous_scale="Blues",
            labels={"label": "User", "total_tokens": "Tokens"},
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(height=380, margin=dict(t=10), coloraxis_showscale=False,
                          xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

        # Hourly activity
        if not hourly_df.empty:
            st.subheader("Hourly Activity Today")
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=hourly_df["hour"], y=pd.to_numeric(hourly_df["total_tokens"], errors="coerce").fillna(0),
                name="Tokens", marker_color="#5C6BC0", yaxis="y"
            ))
            fig2.add_trace(go.Scatter(
                x=hourly_df["hour"], y=pd.to_numeric(hourly_df["active_users"], errors="coerce").fillna(0),
                name="Active Users", mode="lines+markers",
                marker_color="#FF7043", yaxis="y2"
            ))
            fig2.update_layout(
                height=300, margin=dict(t=10),
                yaxis=dict(title="Tokens"),
                yaxis2=dict(title="Users", overlaying="y", side="right"),
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Full table
        st.subheader("All Users")
        disp = today_df.copy()
        for col in ["total_tokens", "input_tokens", "output_tokens", "requests"]:
            disp[col] = pd.to_numeric(disp[col], errors="coerce").fillna(0).apply(lambda x: f"{_n(x):,}")
        st.dataframe(disp, use_container_width=True, hide_index=True)

# ─── Models tab ───────────────────────────────────────────────────────────────
with tab2:
    st.subheader("AI Models in Use — Today")

    if models_df.empty:
        st.info("No model data today.")
    else:
        col_a, col_b = st.columns(2)
        with col_a:
            fig3 = px.pie(
                models_df.head(10), values="total_tokens", names="model",
                title="Token Share by Model",
                hole=0.4,
            )
            fig3.update_layout(height=360, margin=dict(t=40))
            st.plotly_chart(fig3, use_container_width=True)

        with col_b:
            fig4 = px.bar(
                models_df.head(10), x="model", y="unique_users",
                title="Unique Users per Model",
                color="unique_users", color_continuous_scale="Greens",
                labels={"model": "Model", "unique_users": "Users"},
            )
            fig4.update_layout(height=360, margin=dict(t=40),
                               coloraxis_showscale=False, xaxis_tickangle=-30)
            st.plotly_chart(fig4, use_container_width=True)

        st.subheader("Model Details")
        disp_m = models_df.copy()
        disp_m["total_tokens"]   = pd.to_numeric(disp_m["total_tokens"],   errors="coerce").fillna(0).apply(lambda x: f"{_n(x):,}")
        disp_m["avg_latency_ms"] = pd.to_numeric(disp_m["avg_latency_ms"], errors="coerce").fillna(0).apply(lambda x: f"{_n(x):,} ms")
        st.dataframe(disp_m, use_container_width=True, hide_index=True)

# ─── Trends tab ───────────────────────────────────────────────────────────────
with tab3:
    st.subheader(f"Workspace Activity — Last {trend_days} Days")

    if summary_df.empty:
        st.info("Not enough data for trend view.")
    else:
        # Workspace totals bar
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            x=summary_df["day"], y=pd.to_numeric(summary_df["total_tokens"], errors="coerce").fillna(0),
            name="Total Tokens", marker_color="#5C6BC0", yaxis="y"
        ))
        fig5.add_trace(go.Scatter(
            x=summary_df["day"], y=pd.to_numeric(summary_df["active_users"], errors="coerce").fillna(0),
            name="Active Users", mode="lines+markers",
            marker_color="#FF7043", yaxis="y2"
        ))
        fig5.update_layout(
            height=320, margin=dict(t=10),
            yaxis=dict(title="Total Tokens"),
            yaxis2=dict(title="Active Users", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig5, use_container_width=True)

        # Per-user trend (top 10 users)
        if not trend_df.empty:
            top_users = (
                trend_df.groupby("user_id")["total_tokens"]
                .sum().nlargest(10).index.tolist()
            )
            trend_top = trend_df[trend_df["user_id"].isin(top_users)].copy()
            trend_top["user_short"] = trend_top["user_id"].str.split("@").str[0]

            fig6 = px.line(
                trend_top, x="day", y="total_tokens", color="user_short",
                markers=True,
                labels={"day": "Date", "total_tokens": "Tokens", "user_short": "User"},
                title="Top 10 Users — Daily Token Usage",
            )
            fig6.update_layout(height=380, margin=dict(t=40))
            st.plotly_chart(fig6, use_container_width=True)

# ─── Budgets & Alerts tab ─────────────────────────────────────────────────────
with tab4:
    st.subheader("Live Usage vs Configured Budgets")

    if budgets_df.empty:
        st.info("No usage data today.")
    else:
        def _status(pct):
            if pct >= 100: return "🔴 Exceeded"
            if pct >= 80:  return "🟡 Warning"
            return "🟢 OK"

        disp_b = budgets_df.copy()
        disp_b["pct_used"]     = pd.to_numeric(disp_b["pct_used"],     errors="coerce").fillna(0)
        disp_b["total_tokens"] = pd.to_numeric(disp_b["total_tokens"], errors="coerce").fillna(0)
        disp_b["daily_limit"]  = pd.to_numeric(disp_b["daily_limit"],  errors="coerce").fillna(0)
        disp_b.insert(0, "Status", disp_b["pct_used"].apply(_status))
        disp_b["total_tokens"] = disp_b["total_tokens"].apply(lambda x: f"{_n(x):,}")
        disp_b["daily_limit"]  = disp_b["daily_limit"].apply(lambda x: f"{_n(x):,}")
        disp_b["pct_used"]     = disp_b["pct_used"].apply(lambda x: f"{float(x):.1f}%")
        st.dataframe(disp_b, use_container_width=True, hide_index=True)

    st.subheader("Configured Budgets")
    budgets_table = db.list_all_budgets()
    if budgets_table.empty:
        st.info("No custom budgets set yet. Use the sidebar to add one.")
    else:
        st.dataframe(budgets_table, use_container_width=True, hide_index=True)

    st.subheader("Recent Alerts")
    alerts = db.get_recent_alerts(20)
    if alerts.empty:
        st.info("No alerts fired yet.")
    else:
        st.dataframe(alerts, use_container_width=True, hide_index=True)

# ─── Raw Data tab ─────────────────────────────────────────────────────────────
with tab5:
    st.subheader("Workspace Summary (last 7 days)")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    if st.checkbox("Show model heatmap data"):
        heatmap = st_tables.get_user_model_heatmap(trend_days)
        if not heatmap.empty:
            pivot = heatmap.pivot_table(
                index="user_id", columns="model",
                values="total_tokens", fill_value=0
            )
            fig7 = px.imshow(
                pivot, aspect="auto",
                color_continuous_scale="Blues",
                title="Token Usage Heatmap: User × Model",
                labels=dict(x="Model", y="User", color="Tokens"),
            )
            fig7.update_layout(height=max(400, len(pivot) * 18))
            st.plotly_chart(fig7, use_container_width=True)
