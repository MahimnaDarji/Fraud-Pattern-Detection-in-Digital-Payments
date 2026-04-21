"""SMTP email delivery channel for triggered fraud alerts."""

from __future__ import annotations

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from notifications.models import AlertPayload
from utils.logger import get_logger


class SmtpEmailChannel:
    """Send fraud alert notifications over SMTP.

    Supports SMTP_SSL (port 465) and STARTTLS (port 587) transports,
    selected by the ``use_tls`` flag. The connection is opened fresh for
    each delivery call so the channel is safe to use in long-running
    consumer loops without stale-connection problems.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        sender: str,
        recipients: list[str],
        use_tls: bool = True,
    ) -> None:
        if not recipients:
            raise ValueError("SmtpEmailChannel requires at least one recipient address.")
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._sender = sender
        self._recipients = list(recipients)
        self._use_tls = use_tls
        self._logger = get_logger(__name__)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def deliver(self, payload: AlertPayload) -> bool:
        """Send an email for *payload*. Returns True on success, False on failure."""
        subject = self._build_subject(payload)
        body_html = self._build_html_body(payload)
        body_text = self._build_text_body(payload)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._sender
        msg["To"] = ", ".join(self._recipients)
        msg.attach(MIMEText(body_text, "plain"))
        msg.attach(MIMEText(body_html, "html"))

        try:
            self._send(msg)
            self._logger.info(
                "Email alert sent channel=smtp recipients=%d %s",
                len(self._recipients),
                payload.identity,
            )
            return True
        except Exception:
            self._logger.exception(
                "Email alert failed channel=smtp %s",
                payload.identity,
            )
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _send(self, msg: MIMEMultipart) -> None:
        """Open a fresh SMTP connection and deliver the message."""
        if self._use_tls:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self._host, self._port, context=context) as server:
                server.login(self._username, self._password)
                server.sendmail(self._sender, self._recipients, msg.as_string())
        else:
            with smtplib.SMTP(self._host, self._port) as server:
                server.ehlo()
                server.starttls()
                server.login(self._username, self._password)
                server.sendmail(self._sender, self._recipients, msg.as_string())

    @staticmethod
    def _build_subject(payload: AlertPayload) -> str:
        severity = payload.alert_severity or "Unknown"
        txn = payload.transaction_id or "N/A"
        return f"[Fraud Alert] {severity} severity — transaction {txn}"

    @staticmethod
    def _build_text_body(payload: AlertPayload) -> str:
        d = payload.to_dict()
        lines = ["Fraud Alert Triggered\n", "=" * 40]
        for key, value in d.items():
            lines.append(f"{key}: {value}")
        return "\n".join(lines)

    @staticmethod
    def _build_html_body(payload: AlertPayload) -> str:
        d = payload.to_dict()
        severity = payload.alert_severity or "Unknown"
        severity_colour = {
            "High": "#c0392b",
            "Medium": "#e67e22",
            "Low": "#2980b9",
        }.get(severity, "#555555")

        rows: list[str] = []
        for key, value in d.items():
            display = "" if value is None else str(value)
            rows.append(
                f"<tr><td style='padding:6px 12px;font-weight:600;color:#555'>{key}</td>"
                f"<td style='padding:6px 12px;color:#222'>{display}</td></tr>"
            )

        table_rows = "\n".join(rows)
        return f"""<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;background:#f4f4f4;padding:24px">
  <div style="max-width:640px;margin:0 auto;background:#fff;border-radius:8px;
              border-top:4px solid {severity_colour};padding:24px">
    <h2 style="margin:0 0 4px;color:{severity_colour}">&#9888; Fraud Alert</h2>
    <p style="margin:0 0 16px;color:#888;font-size:13px">Severity: <strong>{severity}</strong></p>
    <table style="width:100%;border-collapse:collapse;font-size:14px">
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </div>
</body>
</html>"""
