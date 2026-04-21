"""HTTP webhook delivery channel for triggered fraud alerts."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from notifications.models import AlertPayload
from utils.logger import get_logger


class HttpWebhookChannel:
    """POST fraud alert payloads to one or more HTTP endpoints as JSON.

    Each URL in *endpoints* receives an independent POST. Failures for
    individual endpoints are logged and counted without stopping delivery
    to the remaining URLs.

    The payload shape is FastAPI-compatible: a flat JSON object whose
    keys are the 11 standard alert fields.
    """

    def __init__(
        self,
        endpoints: list[str],
        timeout_seconds: int = 10,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        if not endpoints:
            raise ValueError("HttpWebhookChannel requires at least one endpoint URL.")
        self._endpoints = list(endpoints)
        self._timeout = timeout_seconds
        self._extra_headers: dict[str, str] = extra_headers or {}
        self._logger = get_logger(__name__)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def deliver(self, payload: AlertPayload) -> bool:
        """POST *payload* to every configured endpoint.

        Returns True when all POSTs succeed, False if any fail.
        """
        body = json.dumps(payload.to_dict(), default=str).encode("utf-8")
        all_ok = True

        for url in self._endpoints:
            ok = self._post(url, body, payload)
            if not ok:
                all_ok = False

        return all_ok

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _post(self, url: str, body: bytes, payload: AlertPayload) -> bool:
        """Send a single HTTP POST and return True on 2xx, False otherwise."""
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": "fraud-detection-alert/1.0",
        }
        headers.update(self._extra_headers)

        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
            if 200 <= status < 300:
                self._logger.info(
                    "Webhook alert sent channel=webhook url=%s status=%d %s",
                    url,
                    status,
                    payload.identity,
                )
                return True
            self._logger.error(
                "Webhook alert rejected channel=webhook url=%s status=%d %s",
                url,
                status,
                payload.identity,
            )
            return False
        except urllib.error.HTTPError as exc:
            self._logger.exception(
                "Webhook HTTP error channel=webhook url=%s status=%d %s",
                url,
                exc.code,
                payload.identity,
            )
            return False
        except Exception:
            self._logger.exception(
                "Webhook delivery failed channel=webhook url=%s %s",
                url,
                payload.identity,
            )
            return False
