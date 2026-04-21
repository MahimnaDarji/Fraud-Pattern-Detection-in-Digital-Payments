"""Unified SQLite audit-trail repository for scored transactions and fraud alerts.

Design principles:
- Single connection per repository instance, opened once and reused.
- WAL journal mode for safe concurrent readers (API) alongside a single writer
  (streaming pipeline / notification service).
- All writes are batch-oriented: ``executemany`` inside a single BEGIN/COMMIT
  transaction eliminates per-row connection overhead.
- A module-level threading.Lock serialises concurrent write calls so the
  connection can be shared safely across threads.
- No ORM — only stdlib ``sqlite3``.
- In-memory fallback when ``db_path`` is empty (development / testing).
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from notifications.models import AlertPayload
from utils.logger import get_logger

_logger = get_logger(__name__)

_WRITE_LOCK = threading.Lock()
_REPO_SINGLETON: AuditRepository | None = None


# ---------------------------------------------------------------------------
# DDL — scored_transactions
# ---------------------------------------------------------------------------

_CREATE_SCORED_TRANSACTIONS_SQL = """
CREATE TABLE IF NOT EXISTS scored_transactions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Transaction identifiers
    transaction_id          TEXT,
    user_id                 TEXT,
    account_id              TEXT,
    merchant_id             TEXT,
    amount                  REAL,
    timestamp               TEXT,
    -- Risk scores
    fraud_probability       REAL,
    fraud_prediction        INTEGER,
    anomaly_score           REAL,
    is_anomaly              INTEGER,
    propagated_risk_score   REAL,
    -- Graph signals
    pagerank_score          REAL,
    community_id            REAL,
    triangle_count          INTEGER,
    component_id            REAL,
    component_size          INTEGER,
    -- Alert fields (NULL when no alert)
    alert_triggered         INTEGER,
    alert_reason            TEXT,
    alert_severity          TEXT,
    alert_timestamp         TEXT,
    -- Processing metadata
    batch_id                INTEGER NOT NULL,
    processed_at            TEXT NOT NULL
);
"""

_CREATE_SCORED_TXN_IDX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_scored_transactions_txn_id
    ON scored_transactions (transaction_id)
    WHERE transaction_id IS NOT NULL;
"""

_CREATE_SCORED_TXN_BATCH_IDX_SQL = """
CREATE INDEX IF NOT EXISTS idx_scored_transactions_batch_id
    ON scored_transactions (batch_id);
"""

# ---------------------------------------------------------------------------
# DDL — fraud_alerts
# ---------------------------------------------------------------------------

_CREATE_FRAUD_ALERTS_SQL = """
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Transaction identifiers
    transaction_id          TEXT,
    user_id                 TEXT,
    account_id              TEXT,
    merchant_id             TEXT,
    amount                  REAL,
    timestamp               TEXT,
    -- Risk scores
    fraud_probability       REAL,
    anomaly_score           REAL,
    propagated_risk_score   REAL,
    -- Graph signals
    pagerank_score          REAL,
    community_id            REAL,
    triangle_count          INTEGER,
    component_id            REAL,
    component_size          INTEGER,
    -- Alert fields
    alert_reason            TEXT,
    alert_severity          TEXT,
    alert_timestamp         TEXT,
    -- Processing metadata
    batch_id                INTEGER,
    received_at             TEXT NOT NULL
);
"""

_CREATE_FRAUD_ALERTS_IDX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_fraud_alerts_transaction_id
    ON fraud_alerts (transaction_id)
    WHERE transaction_id IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Column lists used by batch inserts
# ---------------------------------------------------------------------------

_SCORED_TXN_COLUMNS: tuple[str, ...] = (
    "transaction_id", "user_id", "account_id", "merchant_id",
    "amount", "timestamp",
    "fraud_probability", "fraud_prediction", "anomaly_score", "is_anomaly",
    "propagated_risk_score",
    "pagerank_score", "community_id", "triangle_count", "component_id", "component_size",
    "alert_triggered", "alert_reason", "alert_severity", "alert_timestamp",
    "batch_id", "processed_at",
)

_FRAUD_ALERT_COLUMNS: tuple[str, ...] = (
    "transaction_id", "user_id", "account_id", "merchant_id",
    "amount", "timestamp",
    "fraud_probability", "anomaly_score", "propagated_risk_score",
    "pagerank_score", "community_id", "triangle_count", "component_id", "component_size",
    "alert_reason", "alert_severity", "alert_timestamp",
    "batch_id", "received_at",
)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class AuditRepository:
    """Persist and query the full audit trail of scored transactions and alerts."""

    def __init__(self, db_path: str = "") -> None:
        resolved = db_path.strip() if db_path.strip() else ":memory:"
        self._db_path = resolved
        try:
            self._conn = sqlite3.connect(
                resolved,
                check_same_thread=False,
                isolation_level=None,   # manual transaction control
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._initialise_schema()
            _logger.info("AuditRepository initialised db_path=%s", resolved)
        except Exception as exc:
            _logger.exception(
                "AuditRepository failed to open database db_path=%s", resolved
            )
            raise RuntimeError(
                f"Cannot open audit database at '{resolved}'."
            ) from exc

    # ------------------------------------------------------------------
    # Batch writes — scored transactions
    # ------------------------------------------------------------------

    def insert_transaction_batch(
        self,
        frame: pd.DataFrame,
        batch_id: int,
    ) -> int:
        """Persist all rows from *frame* as a single SQLite transaction.

        Returns the number of rows actually inserted (duplicates on
        ``transaction_id`` are silently skipped via INSERT OR IGNORE).
        """
        if frame.empty:
            return 0

        processed_at = datetime.now(timezone.utc).isoformat()
        placeholders = ", ".join(["?"] * len(_SCORED_TXN_COLUMNS))
        sql = (
            f"INSERT OR IGNORE INTO scored_transactions "
            f"({', '.join(_SCORED_TXN_COLUMNS)}) "
            f"VALUES ({placeholders})"
        )

        params_list = [
            _extract_scored_txn_params(row, batch_id, processed_at)
            for _, row in frame.iterrows()
        ]

        try:
            with _WRITE_LOCK:
                self._conn.execute("BEGIN")
                cursor = self._conn.executemany(sql, params_list)
                self._conn.execute("COMMIT")
                inserted = cursor.rowcount
        except Exception as exc:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            _logger.exception(
                "Transaction batch write failed batch_id=%d rows_attempted=%d",
                batch_id,
                len(params_list),
            )
            raise RuntimeError(
                f"Failed to persist scored transaction batch batch_id={batch_id}."
            ) from exc

        _logger.info(
            "Scored transactions persisted batch_id=%d rows_inserted=%d rows_total=%d",
            batch_id,
            inserted,
            len(params_list),
        )
        return inserted

    # ------------------------------------------------------------------
    # Batch writes — fraud alerts
    # ------------------------------------------------------------------

    def insert_alert_batch(
        self,
        frame: pd.DataFrame,
        batch_id: int,
    ) -> int:
        """Persist alert-only rows from *frame* as a single SQLite transaction.

        Only rows where ``alert_triggered`` is truthy are written.
        Returns the number of rows actually inserted.
        """
        alert_frame = frame[frame["alert_triggered"].fillna(False).astype(bool)]
        if alert_frame.empty:
            return 0

        received_at = datetime.now(timezone.utc).isoformat()
        placeholders = ", ".join(["?"] * len(_FRAUD_ALERT_COLUMNS))
        sql = (
            f"INSERT OR IGNORE INTO fraud_alerts "
            f"({', '.join(_FRAUD_ALERT_COLUMNS)}) "
            f"VALUES ({placeholders})"
        )

        params_list = [
            _extract_alert_params(row, batch_id, received_at)
            for _, row in alert_frame.iterrows()
        ]

        try:
            with _WRITE_LOCK:
                self._conn.execute("BEGIN")
                cursor = self._conn.executemany(sql, params_list)
                self._conn.execute("COMMIT")
                inserted = cursor.rowcount
        except Exception as exc:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            _logger.exception(
                "Alert batch write failed batch_id=%d rows_attempted=%d",
                batch_id,
                len(params_list),
            )
            raise RuntimeError(
                f"Failed to persist alert batch batch_id={batch_id}."
            ) from exc

        _logger.info(
            "Fraud alerts persisted batch_id=%d rows_inserted=%d rows_attempted=%d",
            batch_id,
            inserted,
            len(params_list),
        )
        return inserted

    # ------------------------------------------------------------------
    # Single-row alert write (used by notification service / Kafka path)
    # ------------------------------------------------------------------

    def insert_alert(self, payload: AlertPayload) -> int | None:
        """Persist one triggered alert from an AlertPayload; ignore duplicates."""
        received_at = datetime.now(timezone.utc).isoformat()
        sql = (
            f"INSERT OR IGNORE INTO fraud_alerts "
            f"({', '.join(_FRAUD_ALERT_COLUMNS)}) "
            f"VALUES ({', '.join(['?'] * len(_FRAUD_ALERT_COLUMNS))})"
        )
        params = (
            payload.transaction_id,
            payload.user_id,
            payload.account_id,
            payload.merchant_id,
            payload.amount,
            payload.timestamp,
            payload.fraud_probability,
            payload.anomaly_score,
            payload.propagated_risk_score,
            None,   # pagerank_score — not available from Kafka payload
            None,   # community_id
            None,   # triangle_count
            None,   # component_id
            None,   # component_size
            payload.alert_reason,
            payload.alert_severity,
            None,   # alert_timestamp
            None,   # batch_id
            received_at,
        )
        try:
            with _WRITE_LOCK:
                cursor = self._conn.execute(sql, params)
                row_id: int | None = cursor.lastrowid if cursor.rowcount > 0 else None
        except Exception:
            _logger.exception(
                "Single alert insert failed transaction_id=%s",
                payload.transaction_id,
            )
            return None

        if row_id:
            _logger.info(
                "Alert stored id=%d transaction_id=%s severity=%s",
                row_id,
                payload.transaction_id,
                payload.alert_severity,
            )
        return row_id

    # ------------------------------------------------------------------
    # Read — alert list  (used by GET /alerts)
    # ------------------------------------------------------------------

    def list_alerts(
        self,
        limit: int = 50,
        offset: int = 0,
        severity: str | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Return (total_count, page_of_rows) from fraud_alerts, newest first."""
        where_clause, params = _severity_filter(severity)
        count_row = self._conn.execute(
            f"SELECT COUNT(*) AS cnt FROM fraud_alerts{where_clause}", params
        ).fetchone()
        total = int(count_row["cnt"])

        page_params = list(params) + [limit, offset]
        rows = self._conn.execute(
            f"""
            SELECT * FROM fraud_alerts{where_clause}
            ORDER BY received_at DESC
            LIMIT ? OFFSET ?
            """,
            page_params,
        ).fetchall()

        return total, [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Read — single alert  (used by GET /alerts/{transaction_id})
    # ------------------------------------------------------------------

    def get_alert_by_transaction_id(self, transaction_id: str) -> dict[str, Any] | None:
        """Return one fraud_alerts row by transaction_id, or None."""
        row = self._conn.execute(
            "SELECT * FROM fraud_alerts WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Read — metrics  (used by GET /metrics)
    # ------------------------------------------------------------------

    def get_metrics(self) -> dict[str, Any]:
        """Return aggregate statistics from both tables."""
        alert_totals = self._conn.execute(
            """
            SELECT
                COUNT(*)                                                          AS total_alerts,
                AVG(fraud_probability)                                            AS avg_fraud_probability,
                AVG(anomaly_score)                                                AS avg_anomaly_score,
                AVG(propagated_risk_score)                                        AS avg_propagated_risk_score,
                SUM(CASE WHEN alert_severity = 'High'   THEN 1 ELSE 0 END)       AS high_count,
                SUM(CASE WHEN alert_severity = 'Medium' THEN 1 ELSE 0 END)       AS medium_count,
                SUM(CASE WHEN alert_severity = 'Low'    THEN 1 ELSE 0 END)       AS low_count,
                SUM(CASE WHEN alert_severity NOT IN ('High','Medium','Low')
                          OR alert_severity IS NULL       THEN 1 ELSE 0 END)      AS unknown_count
            FROM fraud_alerts
            """
        ).fetchone()

        top_merchant_rows = self._conn.execute(
            """
            SELECT merchant_id, COUNT(*) AS alert_count
            FROM fraud_alerts
            WHERE merchant_id IS NOT NULL
            GROUP BY merchant_id
            ORDER BY alert_count DESC
            LIMIT 5
            """
        ).fetchall()

        txn_count_row = self._conn.execute(
            "SELECT COUNT(*) AS cnt FROM scored_transactions"
        ).fetchone()
        total_scored = int(txn_count_row["cnt"] or 0)

        return {
            "total_alerts": int(alert_totals["total_alerts"] or 0),
            "total_scored_transactions": total_scored,
            "avg_fraud_probability": alert_totals["avg_fraud_probability"],
            "avg_anomaly_score": alert_totals["avg_anomaly_score"],
            "avg_propagated_risk_score": alert_totals["avg_propagated_risk_score"],
            "high_count": int(alert_totals["high_count"] or 0),
            "medium_count": int(alert_totals["medium_count"] or 0),
            "low_count": int(alert_totals["low_count"] or 0),
            "unknown_count": int(alert_totals["unknown_count"] or 0),
            "top_merchants": [
                {"merchant_id": row["merchant_id"], "alert_count": int(row["alert_count"])}
                for row in top_merchant_rows
            ],
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _initialise_schema(self) -> None:
        with _WRITE_LOCK:
            self._conn.execute("BEGIN")
            self._conn.execute(_CREATE_SCORED_TRANSACTIONS_SQL)
            self._conn.execute(_CREATE_SCORED_TXN_IDX_SQL)
            self._conn.execute(_CREATE_SCORED_TXN_BATCH_IDX_SQL)
            self._conn.execute(_CREATE_FRAUD_ALERTS_SQL)
            self._conn.execute(_CREATE_FRAUD_ALERTS_IDX_SQL)
            self._conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Backward-compatible alias — existing code imports AlertRepository
# ---------------------------------------------------------------------------

AlertRepository = AuditRepository


# ---------------------------------------------------------------------------
# Parameter extraction helpers
# ---------------------------------------------------------------------------

def _safe(value: Any) -> Any:
    """Coerce pandas/numpy scalars to Python natives; map NaN/NaT → None."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:    # NaN check
        return None
    try:
        import numpy as np
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            f = float(value)
            return None if f != f else f
        if isinstance(value, (np.bool_,)):
            return bool(value)
    except ImportError:
        pass
    if hasattr(value, "item"):                          # remaining numpy scalars
        return value.item()
    return value


def _bool_int(value: Any) -> int | None:
    """Convert truthy/falsy values to 0/1 for SQLite INTEGER columns."""
    raw = _safe(value)
    if raw is None:
        return None
    return 1 if raw else 0


def _extract_scored_txn_params(
    row: pd.Series,
    batch_id: int,
    processed_at: str,
) -> tuple:
    return (
        _safe(row.get("transaction_id")),
        _safe(row.get("user_id")),
        _safe(row.get("account_id")),
        _safe(row.get("merchant_id")),
        _safe(row.get("amount")),
        _safe(row.get("timestamp")),
        _safe(row.get("fraud_probability")),
        _bool_int(row.get("fraud_prediction")),
        _safe(row.get("anomaly_score")),
        _bool_int(row.get("is_anomaly")),
        _safe(row.get("propagated_risk_score")),
        _safe(row.get("pagerank_score")),
        _safe(row.get("community_id")),
        _safe(row.get("triangle_count")),
        _safe(row.get("component_id")),
        _safe(row.get("component_size")),
        _bool_int(row.get("alert_triggered")),
        _safe(row.get("alert_reason")),
        _safe(row.get("alert_severity")),
        _safe(row.get("alert_timestamp")),
        batch_id,
        processed_at,
    )


def _extract_alert_params(
    row: pd.Series,
    batch_id: int,
    received_at: str,
) -> tuple:
    return (
        _safe(row.get("transaction_id")),
        _safe(row.get("user_id")),
        _safe(row.get("account_id")),
        _safe(row.get("merchant_id")),
        _safe(row.get("amount")),
        _safe(row.get("timestamp")),
        _safe(row.get("fraud_probability")),
        _safe(row.get("anomaly_score")),
        _safe(row.get("propagated_risk_score")),
        _safe(row.get("pagerank_score")),
        _safe(row.get("community_id")),
        _safe(row.get("triangle_count")),
        _safe(row.get("component_id")),
        _safe(row.get("component_size")),
        _safe(row.get("alert_reason")),
        _safe(row.get("alert_severity")),
        _safe(row.get("alert_timestamp")),
        batch_id,
        received_at,
    )


def _severity_filter(severity: str | None) -> tuple[str, list]:
    if severity:
        return " WHERE alert_severity = ?", [severity]
    return "", []


# ---------------------------------------------------------------------------
# Module-level singleton accessor
# ---------------------------------------------------------------------------

def get_repository(db_path: str = "") -> AuditRepository:
    """Return the process-level AuditRepository singleton."""
    global _REPO_SINGLETON
    if _REPO_SINGLETON is None:
        _REPO_SINGLETON = AuditRepository(db_path=db_path)
    return _REPO_SINGLETON
