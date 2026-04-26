from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_PATH = ROOT_DIR / "final_dataset.parquet"

app = FastAPI(title="Fraud Ops Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATASET_PATH)

    if "alert_severity" not in df.columns:
        if "risk_level" in df.columns:
            df["alert_severity"] = df["risk_level"]
        elif "fraud_prediction" in df.columns:
            df["alert_severity"] = df["fraud_prediction"].map(
                lambda x: "High" if int(x) == 1 else "Low"
            )
        else:
            df["alert_severity"] = "Low"

    if "fraud_probability" not in df.columns:
        df["fraud_probability"] = df.get("risk_score", 0.0)

    if "anomaly_score" not in df.columns:
        df["anomaly_score"] = 0.0

    if "propagated_risk" not in df.columns:
        df["propagated_risk"] = df.get("propagated_risk_score", 0.0)

    if "alert_reason" not in df.columns:
        df["alert_reason"] = df["alert_severity"].map(
            lambda x: "High anomaly score" if x in ["Critical", "High"] else "Normal activity"
        )

    return df


def apply_filters(
    df: pd.DataFrame,
    severity: Optional[str] = None,
    user_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    transaction_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    filtered = df.copy()

    if severity and severity != "All":
        filtered = filtered[
            filtered["alert_severity"].astype(str).str.lower()
            == severity.lower()
        ]

    if user_id and "user_id" in filtered.columns:
        filtered = filtered[
            filtered["user_id"].astype(str).str.contains(user_id, case=False, na=False)
        ]

    if merchant_id and "merchant_id" in filtered.columns:
        filtered = filtered[
            filtered["merchant_id"].astype(str).str.contains(merchant_id, case=False, na=False)
        ]

    if transaction_id and "transaction_id" in filtered.columns:
        filtered = filtered[
            filtered["transaction_id"].astype(str).str.contains(transaction_id, case=False, na=False)
        ]

    timestamp_col = next(
        (col for col in ["timestamp", "event_timestamp", "transaction_timestamp"] if col in filtered.columns),
        None,
    )

    if timestamp_col:
        filtered[timestamp_col] = pd.to_datetime(filtered[timestamp_col], errors="coerce")

        if start_date:
            filtered = filtered[filtered[timestamp_col] >= pd.to_datetime(start_date)]

        if end_date:
            filtered = filtered[filtered[timestamp_col] <= pd.to_datetime(end_date)]

    return filtered


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/metrics")
def metrics(
    severity: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    merchant_id: Optional[str] = Query(default=None),
    transaction_id: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = apply_filters(load_data(), severity, user_id, merchant_id, transaction_id, start_date, end_date)

    total_alerts = len(df)
    critical_alerts = int((df["alert_severity"].astype(str).str.lower() == "critical").sum())
    high_risk = int(df["alert_severity"].astype(str).str.lower().isin(["critical", "high"]).sum())

    active_users = (
        df["user_id"].nunique()
        if "user_id" in df.columns
        else df["account_id"].nunique()
        if "account_id" in df.columns
        else 0
    )

    return {
        "kpis": [
            {"label": "Total Alerts", "value": f"{total_alerts:,}", "delta": 0, "direction": "flat", "accent": "blue"},
            {"label": "Critical Alerts", "value": f"{critical_alerts:,}", "delta": 0, "direction": "flat", "accent": "red"},
            {"label": "Mean Fraud Probability", "value": round(float(df["fraud_probability"].mean() or 0), 3), "delta": 0, "direction": "flat", "accent": "blue"},
            {"label": "Mean Anomaly Score", "value": round(float(df["anomaly_score"].mean() or 0), 3), "delta": 0, "direction": "flat", "accent": "orange"},
            {"label": "High Risk Transactions", "value": f"{high_risk:,}", "delta": 0, "direction": "flat", "accent": "green"},
            {"label": "Active Users Flagged", "value": f"{active_users:,}", "delta": 0, "direction": "flat", "accent": "orange"},
        ]
    }


@app.get("/charts")
def charts(
    severity: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    merchant_id: Optional[str] = Query(default=None),
    transaction_id: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = apply_filters(load_data(), severity, user_id, merchant_id, transaction_id, start_date, end_date)

    timestamp_col = next(
        (col for col in ["timestamp", "event_timestamp", "transaction_timestamp"] if col in df.columns),
        None,
    )

    if timestamp_col:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
        df["day"] = df[timestamp_col].dt.strftime("%b %d")
    else:
        df["day"] = "Unknown"

    trend_df = (
        df.groupby(["day", "alert_severity"])
        .size()
        .reset_index(name="count")
    )

    trend = []
    for day in sorted(df["day"].dropna().unique())[:10]:
        day_data = trend_df[trend_df["day"] == day]
        item = {
            "day": day,
            "all": int(day_data["count"].sum()),
            "critical": int(day_data[day_data["alert_severity"].astype(str).str.lower() == "critical"]["count"].sum()),
            "high": int(day_data[day_data["alert_severity"].astype(str).str.lower() == "high"]["count"].sum()),
            "medium": int(day_data[day_data["alert_severity"].astype(str).str.lower() == "medium"]["count"].sum()),
            "low": int(day_data[day_data["alert_severity"].astype(str).str.lower() == "low"]["count"].sum()),
        }
        trend.append(item)

    severity_counts = df["alert_severity"].astype(str).value_counts().to_dict()

    reasons = (
        df["alert_reason"]
        .astype(str)
        .value_counts()
        .head(5)
        .reset_index()
        .rename(columns={"index": "reason", "alert_reason": "count"})
    )

    return {
        "trend": trend,
        "severity": [
            {"name": "Critical", "value": int(severity_counts.get("Critical", 0)), "color": "#ef6a6a"},
            {"name": "High", "value": int(severity_counts.get("High", 0)), "color": "#f5a84b"},
            {"name": "Medium", "value": int(severity_counts.get("Medium", 0)), "color": "#d6a23f"},
            {"name": "Low", "value": int(severity_counts.get("Low", 0)), "color": "#4fc48d"},
        ],
        "reasons": [
            {"reason": str(row.iloc[0]), "count": int(row.iloc[1])}
            for _, row in reasons.iterrows()
        ],
    }


@app.get("/alerts")
def alerts(
    severity: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    merchant_id: Optional[str] = Query(default=None),
    transaction_id: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = apply_filters(load_data(), severity, user_id, merchant_id, transaction_id, start_date, end_date)

    id_col = "transaction_id" if "transaction_id" in df.columns else df.index.name
    user_col = "user_id" if "user_id" in df.columns else "account_id" if "account_id" in df.columns else None
    merchant_col = "merchant_id" if "merchant_id" in df.columns else None
    amount_col = "amount" if "amount" in df.columns else None
    timestamp_col = next((col for col in ["timestamp", "event_timestamp", "transaction_timestamp"] if col in df.columns), None)

    df = df.sort_values("fraud_probability", ascending=False).head(50)

    output = []
    for idx, row in df.iterrows():
        output.append(
            {
                "time": str(row[timestamp_col]) if timestamp_col else "",
                "transaction_id": str(row[id_col]) if id_col in df.columns else f"TXN-{idx}",
                "user_id": str(row[user_col]) if user_col else "",
                "merchant_id": str(row[merchant_col]) if merchant_col else "",
                "amount": f"${float(row[amount_col]):,.2f}" if amount_col else "$0.00",
                "fraud_probability": float(row["fraud_probability"] or 0),
                "anomaly_score": float(row["anomaly_score"] or 0),
                "propagated_risk": float(row["propagated_risk"] or 0),
                "severity": str(row["alert_severity"]),
                "reason": str(row["alert_reason"]),
            }
        )

    return output