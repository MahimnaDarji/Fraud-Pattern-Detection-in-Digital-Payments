"""Fraud Investigation Dashboard — Streamlit entry point.

Run with:
    streamlit run dashboard/app.py

The dashboard reads directly from the SQLite audit database shared with the
streaming pipeline. It does not require the FastAPI server to be running.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from data import (
    load_alerts,
    load_high_risk_transactions,
    load_metrics,
    load_recent_transactions,
)

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Fraud Investigation Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS — dark investigation-focused design
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* ── Global ── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: #0d1117;
        color: #e6edf3;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: #161b22;
        border-right: 1px solid #21262d;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stTextInput label,
    [data-testid="stSidebar"] .stDateInput label,
    [data-testid="stSidebar"] .stSlider label {
        color: #8b949e;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        font-weight: 600;
    }

    /* ── KPI metric cards ── */
    [data-testid="metric-container"] {
        background: #161b22;
        border: 1px solid #21262d;
        border-radius: 10px;
        padding: 18px 22px;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #58a6ff;
    }
    [data-testid="metric-container"] [data-testid="stMetricLabel"] {
        color: #8b949e;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* ── Section headings ── */
    .section-header {
        font-size: 0.72rem;
        font-weight: 700;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin: 0 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #21262d;
    }

    /* ── Severity badges ── */
    .badge-high   { background:#da3633;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600; }
    .badge-medium { background:#d29922;color:#000;padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600; }
    .badge-low    { background:#238636;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600; }

    /* ── DataFrames ── */
    [data-testid="stDataFrame"] {
        border: 1px solid #21262d;
        border-radius: 8px;
    }

    /* ── Divider ── */
    hr { border-color: #21262d; margin: 24px 0; }

    /* ── Alert banner ── */
    .alert-banner {
        background: linear-gradient(135deg, #da363322, #da363305);
        border: 1px solid #da363355;
        border-radius: 10px;
        padding: 14px 18px;
        margin-bottom: 16px;
        font-weight: 600;
        color: #ff7b72;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar — filters + controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🛡️ Fraud Investigation")
    st.markdown("---")

    st.markdown('<p class="section-header">Alert Filters</p>', unsafe_allow_html=True)

    severity_filter = st.selectbox(
        "Severity",
        options=["All", "High", "Medium", "Low"],
        index=0,
    )
    user_filter = st.text_input("User ID / Account ID", placeholder="partial match…")
    merchant_filter = st.text_input("Merchant ID", placeholder="partial match…")

    st.markdown('<p class="section-header" style="margin-top:20px">Date Range</p>', unsafe_allow_html=True)
    default_from = (datetime.now(timezone.utc) - timedelta(days=7)).date()
    default_to = datetime.now(timezone.utc).date()
    date_from = st.date_input("From", value=default_from)
    date_to = st.date_input("To", value=default_to)

    st.markdown('<p class="section-header" style="margin-top:20px">High-Risk Threshold</p>', unsafe_allow_html=True)
    min_fraud_prob = st.slider(
        "Min fraud_probability",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.05,
        format="%.2f",
    )

    st.markdown("---")
    st.markdown('<p class="section-header">Auto-Refresh</p>', unsafe_allow_html=True)
    auto_refresh = st.checkbox("Enable auto-refresh", value=False)
    refresh_interval = st.selectbox(
        "Interval (seconds)",
        options=[15, 30, 60, 120],
        index=1,
        disabled=not auto_refresh,
    )

    st.markdown("---")
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ---------------------------------------------------------------------------
# Data loading (cached per filter combination)
# ---------------------------------------------------------------------------

severity_arg = None if severity_filter == "All" else severity_filter
date_from_str = date_from.isoformat() if date_from else None
date_to_str = (date_to.isoformat() + "T23:59:59") if date_to else None


@st.cache_data(ttl=15, show_spinner=False)
def _cached_metrics() -> dict:
    return load_metrics()


@st.cache_data(ttl=15, show_spinner=False)
def _cached_alerts(severity, user, merchant, dfrom, dto) -> pd.DataFrame:
    return load_alerts(
        severity=severity,
        user_filter=user or None,
        merchant_filter=merchant or None,
        date_from=dfrom,
        date_to=dto,
    )


@st.cache_data(ttl=15, show_spinner=False)
def _cached_high_risk(min_prob) -> pd.DataFrame:
    return load_high_risk_transactions(min_fraud_probability=min_prob)


@st.cache_data(ttl=15, show_spinner=False)
def _cached_timeline() -> pd.DataFrame:
    return load_recent_transactions(limit=200)


metrics = _cached_metrics()
alerts_df = _cached_alerts(severity_arg, user_filter, merchant_filter, date_from_str, date_to_str)
high_risk_df = _cached_high_risk(min_fraud_prob)
timeline_df = _cached_timeline()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

col_title, col_ts = st.columns([5, 1])
with col_title:
    st.markdown("# 🛡️ Fraud Investigation Dashboard")
with col_ts:
    st.markdown(
        f"<p style='color:#8b949e;font-size:0.78rem;text-align:right;margin-top:14px'>"
        f"Last updated<br><strong>{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}</strong></p>",
        unsafe_allow_html=True,
    )

# Active high-severity alert banner
if metrics["high_count"] > 0:
    st.markdown(
        f'<div class="alert-banner">⚠️ {metrics["high_count"]} High-severity alert'
        f'{"s" if metrics["high_count"] != 1 else ""} active</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Section 1 — KPI metrics
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">Overview</p>', unsafe_allow_html=True)

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Alerts", f"{metrics['total_alerts']:,}")
k2.metric("Scored Transactions", f"{metrics['total_scored']:,}")
k3.metric("High Severity", metrics["high_count"], delta=None)
k4.metric("Avg Fraud Prob", f"{metrics['avg_fraud_probability']:.3f}")
k5.metric("Avg Anomaly Score", f"{metrics['avg_anomaly_score']:.3f}")
k6.metric("Avg Risk Score", f"{metrics['avg_propagated_risk']:.3f}")

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 2 — Severity breakdown + top merchants
# ---------------------------------------------------------------------------

col_sev, col_merch = st.columns([1, 1])

with col_sev:
    st.markdown('<p class="section-header">Alert Severity Breakdown</p>', unsafe_allow_html=True)
    sev_data = {
        "Severity": ["High", "Medium", "Low"],
        "Count": [metrics["high_count"], metrics["medium_count"], metrics["low_count"]],
    }
    sev_df = pd.DataFrame(sev_data)
    sev_df["Share %"] = (
        sev_df["Count"] / sev_df["Count"].sum() * 100
        if sev_df["Count"].sum() > 0
        else 0.0
    ).round(1)

    # Coloured severity rows via styled DataFrame
    def _severity_style(row: pd.Series) -> list[str]:
        colours = {"High": "background-color:#da363320", "Medium": "background-color:#d2992220", "Low": "background-color:#23863620"}
        c = colours.get(row["Severity"], "")
        return [c] * len(row)

    st.dataframe(
        sev_df.style.apply(_severity_style, axis=1).format({"Share %": "{:.1f}%"}),
        use_container_width=True,
        hide_index=True,
        height=142,
    )

with col_merch:
    st.markdown('<p class="section-header">Top Merchants by Alert Count</p>', unsafe_allow_html=True)
    if metrics["top_merchants"]:
        merch_df = pd.DataFrame(metrics["top_merchants"])
        merch_df.columns = ["Merchant ID", "Alert Count"]
        st.dataframe(merch_df, use_container_width=True, hide_index=True, height=200)
    else:
        st.info("No merchant data yet.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 3 — Recent fraud alerts (filterable)
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">Recent Fraud Alerts</p>', unsafe_allow_html=True)

if alerts_df.empty:
    st.info("No alerts match the current filters.")
else:
    # Select and rename display columns
    _ALERT_DISPLAY_COLS = {
        "transaction_id": "Transaction ID",
        "user_id": "User ID",
        "account_id": "Account ID",
        "merchant_id": "Merchant ID",
        "amount": "Amount ($)",
        "timestamp": "Txn Timestamp",
        "fraud_probability": "Fraud Prob",
        "anomaly_score": "Anomaly Score",
        "propagated_risk_score": "Risk Score",
        "alert_reason": "Trigger Reason",
        "alert_severity": "Severity",
        "alert_timestamp": "Alert Time",
        "received_at": "Stored At",
    }
    available = [c for c in _ALERT_DISPLAY_COLS if c in alerts_df.columns]
    display_df = alerts_df[available].rename(columns=_ALERT_DISPLAY_COLS)

    # Format numeric columns
    for col in ["Fraud Prob", "Anomaly Score", "Risk Score"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda v: f"{v:.4f}" if pd.notna(v) else "—"
            )
    if "Amount ($)" in display_df.columns:
        display_df["Amount ($)"] = display_df["Amount ($)"].apply(
            lambda v: f"${v:,.2f}" if pd.notna(v) else "—"
        )

    def _style_severity(val: str) -> str:
        return {
            "High": "color:#ff7b72;font-weight:700",
            "Medium": "color:#e3b341;font-weight:600",
            "Low": "color:#3fb950;font-weight:500",
        }.get(str(val), "")

    styled = display_df.style
    if "Severity" in display_df.columns:
        styled = styled.applymap(_style_severity, subset=["Severity"])

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=400,
    )
    st.caption(f"Showing {len(alerts_df):,} alerts — adjust filters to narrow results.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 4 — Top high-risk transactions
# ---------------------------------------------------------------------------

st.markdown(
    f'<p class="section-header">Top High-Risk Transactions '
    f'(fraud_probability ≥ {min_fraud_prob:.2f})</p>',
    unsafe_allow_html=True,
)

if high_risk_df.empty:
    st.info(f"No transactions with fraud_probability ≥ {min_fraud_prob:.2f} found.")
else:
    _RISK_DISPLAY_COLS = {
        "transaction_id": "Transaction ID",
        "user_id": "User ID",
        "merchant_id": "Merchant ID",
        "amount": "Amount ($)",
        "fraud_probability": "Fraud Prob",
        "anomaly_score": "Anomaly Score",
        "propagated_risk_score": "Risk Score",
        "composite_risk": "Composite Risk",
        "alert_severity": "Severity",
        "alert_reason": "Alert Reason",
        "processed_at": "Processed At",
    }
    avail = [c for c in _RISK_DISPLAY_COLS if c in high_risk_df.columns]
    hr_display = high_risk_df[avail].rename(columns=_RISK_DISPLAY_COLS)

    for col in ["Fraud Prob", "Anomaly Score", "Risk Score", "Composite Risk"]:
        if col in hr_display.columns:
            hr_display[col] = hr_display[col].apply(
                lambda v: f"{v:.4f}" if pd.notna(v) else "—"
            )
    if "Amount ($)" in hr_display.columns:
        hr_display["Amount ($)"] = hr_display["Amount ($)"].apply(
            lambda v: f"${v:,.2f}" if pd.notna(v) else "—"
        )

    styled_hr = hr_display.style
    if "Severity" in hr_display.columns:
        styled_hr = styled_hr.applymap(_style_severity, subset=["Severity"])

    st.dataframe(styled_hr, use_container_width=True, hide_index=True, height=360)
    st.caption(f"Showing top {len(high_risk_df):,} high-risk transactions by composite score.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 5 — Recent transaction timeline
# ---------------------------------------------------------------------------

st.markdown('<p class="section-header">Recent Transaction Timeline</p>', unsafe_allow_html=True)

if timeline_df.empty:
    st.info("No scored transactions in the database yet.")
else:
    _TIMELINE_COLS = {
        "transaction_id": "Transaction ID",
        "user_id": "User ID",
        "merchant_id": "Merchant ID",
        "amount": "Amount ($)",
        "fraud_probability": "Fraud Prob",
        "anomaly_score": "Anomaly Score",
        "propagated_risk_score": "Risk Score",
        "alert_triggered": "Alert?",
        "alert_severity": "Severity",
        "processed_at": "Processed At",
        "batch_id": "Batch",
    }
    tl_avail = [c for c in _TIMELINE_COLS if c in timeline_df.columns]
    tl_display = timeline_df[tl_avail].rename(columns=_TIMELINE_COLS)

    for col in ["Fraud Prob", "Anomaly Score", "Risk Score"]:
        if col in tl_display.columns:
            tl_display[col] = tl_display[col].apply(
                lambda v: f"{v:.4f}" if pd.notna(v) else "—"
            )
    if "Amount ($)" in tl_display.columns:
        tl_display["Amount ($)"] = tl_display["Amount ($)"].apply(
            lambda v: f"${v:,.2f}" if pd.notna(v) else "—"
        )
    if "Alert?" in tl_display.columns:
        tl_display["Alert?"] = tl_display["Alert?"].apply(
            lambda v: "✅ Yes" if v else "—"
        )

    styled_tl = tl_display.style
    if "Severity" in tl_display.columns:
        styled_tl = styled_tl.applymap(_style_severity, subset=["Severity"])

    st.dataframe(styled_tl, use_container_width=True, hide_index=True, height=380)
    st.caption(
        f"Most recent {len(timeline_df):,} scored transactions — newest first."
    )

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------

if auto_refresh:
    time.sleep(refresh_interval)
    st.cache_data.clear()
    st.rerun()
