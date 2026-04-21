"""Dashboard data-access layer — isolates all DB queries from Streamlit rendering."""

from __future__ import annotations

import os
import sqlite3
from typing import Any

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

_DB_PATH: str = os.getenv("API_DB_PATH", "artifacts/fraud_alerts.db")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    """Open a read-only SQLite connection to the audit database."""
    path = _DB_PATH.strip() if _DB_PATH.strip() else ":memory:"
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_df(rows: list[sqlite3.Row]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Public data-loading functions
# ---------------------------------------------------------------------------

def load_metrics() -> dict[str, Any]:
    """Return summary counts from both tables."""
    try:
        conn = _get_conn()
        alert_row = conn.execute(
            """
            SELECT
                COUNT(*)                                                       AS total_alerts,
                AVG(fraud_probability)                                         AS avg_fraud_probability,
                AVG(anomaly_score)                                             AS avg_anomaly_score,
                AVG(propagated_risk_score)                                     AS avg_propagated_risk_score,
                SUM(CASE WHEN alert_severity = 'High'   THEN 1 ELSE 0 END)    AS high_count,
                SUM(CASE WHEN alert_severity = 'Medium' THEN 1 ELSE 0 END)    AS medium_count,
                SUM(CASE WHEN alert_severity = 'Low'    THEN 1 ELSE 0 END)    AS low_count
            FROM fraud_alerts
            """
        ).fetchone()

        txn_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM scored_transactions"
        ).fetchone()

        top_merchants = conn.execute(
            """
            SELECT merchant_id, COUNT(*) AS alert_count
            FROM fraud_alerts WHERE merchant_id IS NOT NULL
            GROUP BY merchant_id ORDER BY alert_count DESC LIMIT 5
            """
        ).fetchall()

        conn.close()
        return {
            "total_alerts": int(alert_row["total_alerts"] or 0),
            "total_scored": int(txn_row["cnt"] or 0),
            "avg_fraud_probability": float(alert_row["avg_fraud_probability"] or 0.0),
            "avg_anomaly_score": float(alert_row["avg_anomaly_score"] or 0.0),
            "avg_propagated_risk": float(alert_row["avg_propagated_risk_score"] or 0.0),
            "high_count": int(alert_row["high_count"] or 0),
            "medium_count": int(alert_row["medium_count"] or 0),
            "low_count": int(alert_row["low_count"] or 0),
            "top_merchants": [dict(r) for r in top_merchants],
        }
    except Exception:
        return {
            "total_alerts": 0, "total_scored": 0,
            "avg_fraud_probability": 0.0, "avg_anomaly_score": 0.0,
            "avg_propagated_risk": 0.0,
            "high_count": 0, "medium_count": 0, "low_count": 0,
            "top_merchants": [],
        }


def load_alerts(
    severity: str | None = None,
    user_filter: str | None = None,
    merchant_filter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Return fraud_alerts rows as a DataFrame with all applied filters."""
    try:
        conn = _get_conn()
        conditions: list[str] = []
        params: list[Any] = []

        if severity:
            conditions.append("alert_severity = ?")
            params.append(severity)
        if user_filter and user_filter.strip():
            token = f"%{user_filter.strip()}%"
            conditions.append("(user_id LIKE ? OR account_id LIKE ?)")
            params.extend([token, token])
        if merchant_filter and merchant_filter.strip():
            conditions.append("merchant_id LIKE ?")
            params.append(f"%{merchant_filter.strip()}%")
        if date_from:
            conditions.append("received_at >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("received_at <= ?")
            params.append(date_to)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = conn.execute(
            f"SELECT * FROM fraud_alerts{where} ORDER BY received_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        conn.close()
        return _rows_to_df(rows)
    except Exception:
        return pd.DataFrame()


def load_high_risk_transactions(
    limit: int = 100,
    min_fraud_probability: float = 0.5,
) -> pd.DataFrame:
    """Return top-N riskiest scored transactions by composite risk score."""
    try:
        conn = _get_conn()
        rows = conn.execute(
            """
            SELECT *,
                   COALESCE(fraud_probability, 0)
                   + COALESCE(anomaly_score, 0)
                   + COALESCE(propagated_risk_score, 0) AS composite_risk
            FROM scored_transactions
            WHERE COALESCE(fraud_probability, 0) >= ?
            ORDER BY composite_risk DESC, processed_at DESC
            LIMIT ?
            """,
            (min_fraud_probability, limit),
        ).fetchall()
        conn.close()
        return _rows_to_df(rows)
    except Exception:
        return pd.DataFrame()


def load_recent_transactions(limit: int = 200) -> pd.DataFrame:
    """Return the most recent scored transactions for the timeline view."""
    try:
        conn = _get_conn()
        rows = conn.execute(
            """
            SELECT transaction_id, user_id, account_id, merchant_id,
                   amount, timestamp, fraud_probability, anomaly_score,
                   propagated_risk_score, alert_triggered, alert_severity,
                   processed_at, batch_id
            FROM scored_transactions
            ORDER BY processed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        conn.close()
        return _rows_to_df(rows)
    except Exception:
        return pd.DataFrame()
