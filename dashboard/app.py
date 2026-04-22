"""Fraud Investigation Console.

Run with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import plotly.graph_objects as go
import streamlit as st


st.set_page_config(
    page_title="Fraud Investigation Console",
    page_icon="FD",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _today_utc():
    return _now_utc().date()


def _apply_quick_range(range_name: str) -> None:
    today = _today_utc()
    if range_name == "Last 24 Hours":
        start_date = today - timedelta(days=1)
        end_date = today
    elif range_name == "Last 7 Days":
        start_date = today - timedelta(days=7)
        end_date = today
    elif range_name == "Last 30 Days":
        start_date = today - timedelta(days=30)
        end_date = today
    elif range_name == "MTD":
        start_date = today.replace(day=1)
        end_date = today
    elif range_name == "Last 90 Days":
        start_date = today - timedelta(days=90)
        end_date = today
    else:
        start_date = st.session_state["start_date"]
        end_date = st.session_state["end_date"]

    st.session_state["quick_range"] = range_name
    st.session_state["start_date"] = start_date
    st.session_state["end_date"] = end_date


def _clear_filters() -> None:
    today = _today_utc()
    st.session_state["severity"] = "All"
    st.session_state["user_account"] = ""
    st.session_state["merchant"] = ""
    st.session_state["transaction_id"] = ""
    st.session_state["start_date"] = today - timedelta(days=7)
    st.session_state["end_date"] = today
    st.session_state["quick_range"] = "Last 7 Days"
    st.session_state["last_refresh"] = _now_utc().strftime("%H:%M:%S UTC")


defaults = {
    "severity": "All",
    "user_account": "",
    "merchant": "",
    "transaction_id": "",
    "start_date": _today_utc() - timedelta(days=7),
    "end_date": _today_utc(),
    "quick_range": "Last 7 Days",
    "auto_refresh": False,
    "refresh_interval": "30s",
    "system_health": "Healthy",
    "last_refresh": "Not refreshed",
}
for key, value in defaults.items():
    st.session_state.setdefault(key, value)

if st.session_state["refresh_interval"] not in {"10s", "30s", "60s", "120s"}:
    st.session_state["refresh_interval"] = "30s"


def _plotly_layout(title: str) -> dict:
    return {
        "title": {"text": title, "font": {"size": 12, "color": "#9fb2c9"}, "x": 0.01, "y": 0.96},
        "template": "none",
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"family": "Inter, Roboto, sans-serif", "size": 11, "color": "#b3c2d7"},
        "margin": {"l": 36, "r": 16, "t": 36, "b": 32},
        "xaxis": {
            "showgrid": True,
            "gridcolor": "rgba(130,153,179,0.18)",
            "linecolor": "rgba(130,153,179,0.25)",
            "tickfont": {"size": 10, "color": "#8ea2ba"},
            "zeroline": False,
        },
        "yaxis": {
            "showgrid": True,
            "gridcolor": "rgba(130,153,179,0.18)",
            "linecolor": "rgba(130,153,179,0.25)",
            "tickfont": {"size": 10, "color": "#8ea2ba"},
            "zeroline": False,
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
            "font": {"size": 10, "color": "#8ea2ba"},
            "bgcolor": "rgba(0,0,0,0)",
            "borderwidth": 0,
        },
    }


st.markdown(
    """
    <style>
    :root {
        --bg-main: #0a0f17;
        --bg-sidebar: #0d1420;
        --bg-card: #111a28;
        --bg-control: #142032;
        --border-soft: #223247;
        --text-title: #e8eef9;
        --text-card: #b3c2d7;
        --text-body: #8ea2ba;
        --accent-primary: #2f6fed;
        --positive: #2ea874;
        --negative: #bf5f5f;
        --panel-padding: 0.62rem;
        --layout-gap: 1rem;
    }

    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}

    html, body, [class*="css"], [class*="st-"] {
        font-family: "Inter", "Roboto", sans-serif;
    }

    .stApp {
        background: var(--bg-main);
        color: var(--text-card);
        overflow-x: hidden;
    }

    [data-testid="stSidebar"] {
        background: var(--bg-sidebar);
        border-right: 1px solid var(--border-soft);
        width: 312px !important;
        min-width: 300px !important;
        max-width: 320px !important;
    }

    [data-testid="stSidebar"] .block-container {
        padding: 0.75rem 0.85rem 0.65rem 0.85rem;
    }

    [data-testid="stAppViewContainer"] .main .block-container {
        max-width: 1240px;
        padding: 0.72rem 0.9rem 0.9rem 0.9rem;
    }

    .t-section {
        margin: 0;
        color: var(--text-title);
        font-size: 1.22rem;
        line-height: 1.2;
        font-weight: 650;
    }

    .t-card {
        margin: 0;
        color: #9fb2c9;
        font-size: 0.7rem;
        font-weight: 600;
        line-height: 1.2;
    }

    .t-body {
        margin: 0;
        color: #98abc2;
        font-size: 0.78rem;
        line-height: 1.3;
    }

    .sidebar-title {
        margin: 0;
        color: var(--text-title);
        font-size: 0.98rem;
        font-weight: 700;
        letter-spacing: 0.02em;
    }

    .sidebar-subtitle {
        margin: 0.18rem 0 0.5rem 0;
        color: var(--text-body);
        font-size: 0.74rem;
    }

    .section-label {
        margin: 0.56rem 0 0.28rem 0;
        color: var(--text-body);
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-size: 0.66rem;
        font-weight: 700;
    }

    [data-testid="stSidebar"] .stSelectbox > div > div,
    [data-testid="stSidebar"] .stDateInput > div > div,
    [data-testid="stSidebar"] .stTextInput > div > div > input {
        background: var(--bg-control);
        border: 1px solid var(--border-soft);
        border-radius: 8px;
        color: var(--text-title);
    }

    [data-testid="stSidebar"] .stButton > button {
        background: var(--bg-control);
        border: 1px solid var(--border-soft);
        color: var(--text-title);
        border-radius: 8px;
        min-height: 1.88rem;
        font-size: 0.74rem;
        font-weight: 600;
        transition: border-color 140ms ease, transform 140ms ease;
    }

    [data-testid="stSidebar"] .stButton > button:hover {
        border-color: var(--accent-primary);
    }

    .sidebar-footer-row {
        margin-top: 0.5rem;
        border-top: 1px solid var(--border-soft);
        padding-top: 0.5rem;
        color: var(--text-body);
        font-size: 0.7rem;
    }

    .control-strip {
        background: rgba(17, 26, 40, 0.82);
        border: 1px solid rgba(135, 158, 185, 0.24);
        border-radius: 10px;
        padding: 0.28rem 0.58rem;
    }

    .status-inline {
        display: inline-flex;
        align-items: center;
        gap: 0.44rem;
        min-height: 1.74rem;
        white-space: nowrap;
    }

    .status-dot {
        width: 7px;
        height: 7px;
        border-radius: 999px;
        background: var(--positive);
        box-shadow: 0 0 0 3px rgba(46, 168, 116, 0.14);
    }

    .status-title {
        margin: 0;
        color: var(--text-title);
        font-size: 0.69rem;
        font-weight: 700;
        letter-spacing: 0.04em;
    }

    .status-sep {
        width: 1px;
        height: 0.86rem;
        background: rgba(142, 162, 186, 0.35);
        margin: 0 0.08rem;
    }

    .status-note {
        margin: 0;
        color: var(--text-body);
        font-size: 0.68rem;
        font-weight: 500;
    }

    .auto-label {
        margin: 0;
        color: var(--text-body);
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        line-height: 1.72rem;
        white-space: nowrap;
    }

    .top-controls [data-testid="stHorizontalBlock"] {
        align-items: center;
    }

    .top-controls .stToggle,
    .top-controls .stSelectbox,
    .top-controls .stButton {
        margin: 0;
    }

    .top-controls .stToggle [data-baseweb="switch"] {
        transform: scale(0.78);
        transform-origin: center;
    }

    .top-controls .stSelectbox [data-baseweb="select"] {
        min-height: 1.72rem;
        border-radius: 7px;
        border: 1px solid rgba(135, 158, 185, 0.3);
        background: rgba(20, 32, 50, 0.78);
        font-size: 0.68rem;
    }

    .top-controls .stButton > button {
        background: rgba(20, 32, 50, 0.78);
        border: 1px solid rgba(135, 158, 185, 0.3);
        color: var(--text-title);
        border-radius: 7px;
        min-height: 1.72rem;
        width: 1.72rem;
        padding: 0;
        font-size: 0.74rem;
        font-weight: 700;
    }

    .top-controls .stButton > button:hover {
        border-color: rgba(95, 150, 232, 0.7);
    }

    .header-block {
        margin-top: 0.06rem;
        padding: 0;
    }

    .header-title {
        margin: 0;
        color: var(--text-title);
        font-size: 1.24rem;
        line-height: 1.18;
        font-weight: 680;
    }

    .header-subtitle {
        margin: 0.18rem 0 0 0;
        color: var(--text-body);
        font-size: 0.76rem;
        line-height: 1.25;
        font-weight: 500;
    }

    .shell-card,
    .analytics-card,
    .table-card,
    .heatmap-card,
    .hero-card,
    .kpi-card {
        background: var(--bg-card);
        border: 1px solid var(--border-soft);
        border-radius: 10px;
        padding: var(--panel-padding);
    }

    [data-testid="stPlotlyChart"] {
        background: var(--bg-card);
        border: 1px solid var(--border-soft);
        border-radius: 10px;
        padding: var(--panel-padding);
    }

    .hero-card {
        padding: var(--panel-padding);
    }

    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(6, minmax(0, 1fr));
        gap: 12px;
        width: 100%;
    }

    .kpi-card {
        min-height: 94px;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        gap: 0.34rem;
        padding: var(--panel-padding);
        background: rgba(22, 34, 50, 0.58);
        border: 1px solid rgba(141, 166, 194, 0.22);
        box-shadow: 0 0 0 1px rgba(168, 191, 216, 0.03), 0 6px 18px rgba(5, 9, 15, 0.14);
        transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease, background-color 120ms ease;
    }

    .kpi-card:hover {
        transform: translateY(-1px);
        background: rgba(24, 37, 54, 0.68);
        border-color: rgba(96, 139, 199, 0.38);
        box-shadow: 0 0 0 1px rgba(157, 189, 224, 0.08), 0 10px 20px rgba(5, 9, 15, 0.18);
    }

    .kpi-head {
        display: flex;
        align-items: center;
        gap: 0.38rem;
        min-width: 0;
    }

    .kpi-icon {
        width: 1.22rem;
        height: 1.22rem;
        border-radius: 999px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.56rem;
        font-weight: 700;
        flex: 0 0 auto;
    }

    .kpi-icon-blue {
        background: rgba(47, 111, 237, 0.2);
        color: #8fb3ff;
    }

    .kpi-icon-red {
        background: rgba(191, 95, 95, 0.22);
        color: #efaaaa;
    }

    .kpi-icon-green {
        background: rgba(46, 168, 116, 0.2);
        color: #86dfb8;
    }

    .kpi-icon-orange {
        background: rgba(216, 137, 43, 0.22);
        color: #f1be79;
    }

    .kpi-label {
        margin: 0;
        color: #8aa0b8;
        font-size: 0.57rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }

    .kpi-number {
        margin: 0;
        color: var(--text-title);
        font-size: 1.72rem;
        line-height: 1.02;
        font-weight: 800;
    }

    .kpi-delta {
        margin: 0;
        font-size: 0.56rem;
        line-height: 1.1;
        color: #8297b0;
    }

    .delta-arrow {
        display: inline-block;
        width: 0.72rem;
        font-size: 0.56rem;
        font-weight: 700;
    }

    .kpi-delta-up { color: var(--positive); }
    .kpi-delta-down { color: var(--negative); }
    .kpi-delta-flat { color: #91a4ba; }

    [data-baseweb="tab-list"] {
        gap: 0.8rem;
        border-bottom: 1px solid var(--border-soft);
        margin-top: 0.05rem;
        margin-bottom: 0.1rem;
    }

    [data-baseweb="tab"] {
        background: transparent;
        border: none;
        border-bottom: 2px solid transparent;
        border-radius: 0;
        color: var(--text-card);
        padding: 0.3rem 0.12rem;
        min-height: 1.65rem;
        font-size: 0.72rem;
        font-weight: 600;
    }

    [aria-selected="true"][data-baseweb="tab"] {
        color: var(--text-title);
        border-bottom-color: var(--accent-primary);
    }

    [data-testid="stDataFrame"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-soft) !important;
        border-radius: 10px;
    }

    [data-testid="stDataFrame"] * {
        color: var(--text-card) !important;
        background: transparent !important;
    }

    .styled-table {
        width: 100%;
        border-collapse: collapse;
        border-spacing: 0;
        overflow: hidden;
        border: 1px solid var(--border-soft);
        border-radius: 10px;
    }

    .styled-table th,
    .styled-table td {
        padding: 0.42rem 0.56rem;
        text-align: left;
        border-bottom: 1px solid rgba(34, 50, 71, 0.65);
        font-size: 0.74rem;
        color: var(--text-card);
    }

    .styled-table th {
        color: #9fb2c9;
        background: rgba(20, 32, 50, 0.72);
        font-weight: 600;
    }

    .styled-table tbody tr:nth-child(even) {
        background: rgba(20, 32, 50, 0.45);
    }

    .styled-table tr {
        transition: background-color 120ms ease;
    }

    .styled-table tr:hover {
        background: rgba(47, 111, 237, 0.1);
    }

    .sp-16 { margin-top: var(--layout-gap); }
    .sp-20 { margin-top: var(--layout-gap); }

    @media (max-width: 1260px) {
        .kpi-grid {
            grid-template-columns: repeat(3, minmax(0, 1fr));
        }
    }

    @media (max-width: 1200px) {
        [data-testid="stSidebar"] {
            width: 300px !important;
            min-width: 300px !important;
            max-width: 300px !important;
        }
        [data-testid="stAppViewContainer"] .main .block-container {
            max-width: 100%;
            padding-left: 0.75rem;
            padding-right: 0.75rem;
        }
    }

    @media (max-width: 760px) {
        .kpi-grid {
            grid-template-columns: 1fr;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


with st.sidebar:
    st.markdown('<p class="sidebar-title">Fraud Investigation Console</p>', unsafe_allow_html=True)
    st.markdown('<p class="sidebar-subtitle">Operations Workbench</p>', unsafe_allow_html=True)
    st.markdown('<p class="t-body">Use controls to scope and prioritize investigative views.</p>', unsafe_allow_html=True)

    if st.button("Clear All Filters", use_container_width=True):
        _clear_filters()
        st.rerun()

    st.markdown('<p class="section-label">Scope Filters</p>', unsafe_allow_html=True)
    st.selectbox("Severity", ["All", "High", "Medium", "Low"], key="severity")
    st.text_input("User or Account", key="user_account", placeholder="Identifier")
    st.text_input("Merchant", key="merchant", placeholder="Identifier")
    st.text_input("Transaction ID", key="transaction_id", placeholder="Transaction key")

    st.markdown('<p class="section-label">Time Window</p>', unsafe_allow_html=True)
    st.date_input("Start Date", key="start_date")
    st.date_input("End Date", key="end_date")

    st.markdown('<p class="section-label">Quick Ranges</p>', unsafe_allow_html=True)
    q1, q2, q3 = st.columns(3)
    if q1.button("Last 24 Hours", use_container_width=True):
        _apply_quick_range("Last 24 Hours")
        st.rerun()
    if q2.button("Last 7 Days", use_container_width=True):
        _apply_quick_range("Last 7 Days")
        st.rerun()
    if q3.button("Last 30 Days", use_container_width=True):
        _apply_quick_range("Last 30 Days")
        st.rerun()

    q4, q5, q6 = st.columns(3)
    if q4.button("MTD", use_container_width=True):
        _apply_quick_range("MTD")
        st.rerun()
    if q5.button("Last 90 Days", use_container_width=True):
        _apply_quick_range("Last 90 Days")
        st.rerun()
    if q6.button("Custom", use_container_width=True):
        _apply_quick_range("Custom")
        st.rerun()

    st.markdown(
        f'<div class="sidebar-footer-row">Last refresh: {st.session_state["last_refresh"]}</div>',
        unsafe_allow_html=True,
    )


st.markdown('<div class="control-strip">', unsafe_allow_html=True)
status_left, status_right = st.columns([4.6, 3.4], gap="small")

with status_left:
    st.markdown(
        '<div class="status-inline">'
        '<span class="status-dot"></span>'
        '<span class="status-title">System Health</span>'
        '<span class="status-sep"></span>'
        '<span class="status-note">All systems operational</span>'
        '</div>',
        unsafe_allow_html=True,
    )

with status_right:
    st.markdown('<div class="top-controls">', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns([1.1, 0.65, 0.95, 0.34], gap="small")
    with c1:
        st.markdown('<span class="auto-label">Auto refresh</span>', unsafe_allow_html=True)
    with c2:
        st.toggle("Auto Refresh Toggle", key="auto_refresh", label_visibility="collapsed")
    with c3:
        st.selectbox("Refresh Interval", ["10s", "30s", "60s", "120s"], key="refresh_interval", label_visibility="collapsed")
    with c4:
        if st.button("↻", use_container_width=True):
            st.session_state["last_refresh"] = _now_utc().strftime("%H:%M:%S UTC")
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)


st.markdown('<div class="sp-16"></div>', unsafe_allow_html=True)
st.markdown(
    """
    <div class="header-block">
        <p class="header-title">Fraud Investigation Dashboard</p>
        <p class="header-subtitle">Unified monitoring surface for alert triage, risk signals, and analyst investigation context.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


st.markdown('<div class="sp-16"></div>', unsafe_allow_html=True)
st.markdown(
    """
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-head">
                <span class="kpi-icon kpi-icon-blue">A</span>
                <p class="kpi-label">Total Alerts</p>
            </div>
            <p class="kpi-number">18,432</p>
            <p class="kpi-delta kpi-delta-up"><span class="delta-arrow">&uarr;</span>+2.4% vs prev 7 days</p>
        </div>
        <div class="kpi-card">
            <div class="kpi-head">
                <span class="kpi-icon kpi-icon-red">C</span>
                <p class="kpi-label">Critical Alerts</p>
            </div>
            <p class="kpi-number">1,148</p>
            <p class="kpi-delta kpi-delta-up"><span class="delta-arrow">&uarr;</span>+0.9% vs prev 7 days</p>
        </div>
        <div class="kpi-card">
            <div class="kpi-head">
                <span class="kpi-icon kpi-icon-orange">P</span>
                <p class="kpi-label">Avg Fraud Prob</p>
            </div>
            <p class="kpi-number">0.71</p>
            <p class="kpi-delta kpi-delta-down"><span class="delta-arrow">&darr;</span>-1.1% vs prev 7 days</p>
        </div>
        <div class="kpi-card">
            <div class="kpi-head">
                <span class="kpi-icon kpi-icon-green">N</span>
                <p class="kpi-label">Avg Anomaly Score</p>
            </div>
            <p class="kpi-number">0.63</p>
            <p class="kpi-delta kpi-delta-flat"><span class="delta-arrow">&rarr;</span>No material change</p>
        </div>
        <div class="kpi-card">
            <div class="kpi-head">
                <span class="kpi-icon kpi-icon-orange">R</span>
                <p class="kpi-label">High-Risk Txns</p>
            </div>
            <p class="kpi-number">7,392</p>
            <p class="kpi-delta kpi-delta-up"><span class="delta-arrow">&uarr;</span>+0.3% vs prev 7 days</p>
        </div>
        <div class="kpi-card">
            <div class="kpi-head">
                <span class="kpi-icon kpi-icon-green">U</span>
                <p class="kpi-label">Users Flagged</p>
            </div>
            <p class="kpi-number">482</p>
            <p class="kpi-delta kpi-delta-down"><span class="delta-arrow">&darr;</span>-0.5% vs prev 7 days</p>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


st.markdown('<div class="sp-20"></div>', unsafe_allow_html=True)
tab_overview, tab_alerts, tab_graph, tab_timeline = st.tabs(["Overview", "Alerts", "Graph Signals", "Timeline"])


hours = ["00", "04", "08", "12", "16", "20"]
alerts = [35, 62, 58, 91, 74, 49]
risk = [0.38, 0.42, 0.47, 0.61, 0.55, 0.49]

COLOR_BLUE = "#2f6fed"
COLOR_RED = "#bf5f5f"
COLOR_ORANGE = "#d8892b"
COLOR_GREEN = "#2ea874"

trend_fig = go.Figure()
trend_fig.add_trace(
    go.Scatter(
        x=hours,
        y=alerts,
        mode="lines+markers",
        name="Alerts",
        line={"color": COLOR_BLUE, "width": 2},
        marker={"size": 6, "color": COLOR_BLUE},
    )
)
trend_fig.update_layout(**_plotly_layout("Alert Volume Trend"))

risk_fig = go.Figure()
risk_fig.add_trace(
    go.Bar(
        x=["Segment A", "Segment B", "Segment C", "Segment D"],
        y=[0.62, 0.48, 0.39, 0.27],
        name="Mean Risk",
        marker={"color": COLOR_ORANGE},
    )
)
risk_fig.update_layout(**_plotly_layout("Segment Risk Score"))

mix_fig = go.Figure()
mix_fig.add_trace(
    go.Pie(
        labels=["Critical", "Medium", "Low"],
        values=[26, 49, 25],
        marker={"colors": [COLOR_RED, COLOR_ORANGE, COLOR_GREEN]},
        textinfo="label+percent",
        hole=0.55,
        sort=False,
    )
)
mix_fig.update_layout(
    **_plotly_layout("Severity Mix"),
    showlegend=False,
)

heatmap_fig = go.Figure(
    data=go.Heatmap(
        z=[[2, 3, 4, 2], [3, 5, 6, 3], [2, 4, 5, 2], [1, 2, 3, 1]],
        x=["A", "B", "C", "D"],
        y=["Q1", "Q2", "Q3", "Q4"],
        colorscale=[
            [0.0, COLOR_BLUE],
            [0.5, COLOR_ORANGE],
            [1.0, COLOR_GREEN],
        ],
        showscale=False,
    )
)
heatmap_fig.update_layout(**_plotly_layout("Risk Density Heatmap"))


for tab_name, tab in [
    ("overview", tab_overview),
    ("alerts", tab_alerts),
    ("graph", tab_graph),
    ("timeline", tab_timeline),
]:
    with tab:
        a1, a2, a3 = st.columns([1, 1, 1], gap="small")
        with a1:
            st.plotly_chart(
                trend_fig,
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"trend_chart_{tab_name}",
            )
        with a2:
            st.plotly_chart(
                risk_fig,
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"risk_chart_{tab_name}",
            )
        with a3:
            st.plotly_chart(
                mix_fig,
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"mix_chart_{tab_name}",
            )

        st.markdown('<div class="sp-16"></div>', unsafe_allow_html=True)
        l1, l2 = st.columns([2.1, 1], gap="small")
        with l1:
            st.markdown('<div class="table-card">', unsafe_allow_html=True)
            st.markdown('<p class="t-card">Investigation Queue</p>', unsafe_allow_html=True)
            st.markdown(
                """
                <table class="styled-table">
                    <thead>
                        <tr><th>Transaction ID</th><th>User</th><th>Score</th><th>Status</th></tr>
                    </thead>
                    <tbody>
                        <tr><td>TX-9011</td><td>U-118</td><td>0.92</td><td>Escalated</td></tr>
                        <tr><td>TX-9012</td><td>U-271</td><td>0.89</td><td>Escalated</td></tr>
                        <tr><td>TX-9013</td><td>U-554</td><td>0.84</td><td>Review</td></tr>
                        <tr><td>TX-9014</td><td>U-119</td><td>0.81</td><td>Review</td></tr>
                        <tr><td>TX-9015</td><td>U-044</td><td>0.79</td><td>Monitor</td></tr>
                    </tbody>
                </table>
                """,
                unsafe_allow_html=True,
            )
            st.markdown('</div>', unsafe_allow_html=True)
        with l2:
            st.plotly_chart(
                heatmap_fig,
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"heatmap_chart_{tab_name}",
            )
