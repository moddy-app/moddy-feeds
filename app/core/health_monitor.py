"""Heartbeat vers le Moddy Health Monitor — fire-and-forget, jamais bloquant.

Toutes les 20 s, pousse un état `{status, checks, meta}` construit par
`build_checks()` (cf. `app/schedulers.py`). Si `HM_URL`/`HM_INGEST_TOKEN` sont
absents, le client se désactive proprement (utile en dev local).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import httpx

from app.logging_config import get_logger

log = get_logger(__name__)

HEARTBEAT_INTERVAL_SECONDS = 20


class HeartbeatClient:
    """Boucle asyncio isolée qui POST un heartbeat périodique au monitor."""

    def __init__(
        self,
        service: str,
        *,
        url: str | None,
        token: str | None,
        version: str = "0.0.0",
        build: Callable[[], Awaitable[dict]] | None = None,
        interval: int = HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self.service = service
        self.url = (url or "").rstrip("/")
        self.token = token or ""
        self.version = version
        # Coroutine renvoyant {"status": ..., "checks": {...}, "meta": {...}}.
        self._build = build
        self._interval = interval
        self._started = time.monotonic()
        self._task: asyncio.Task | None = None
        self._http = httpx.AsyncClient(timeout=5)
        # Renseigné par la réponse du monitor : permet au service de couper
        # les notifications non critiques pendant un incident.
        self.incident_active = False

    def start(self) -> None:
        if self._task is not None:
            return
        if not self.url or not self.token:
            log.warning("HM_URL ou HM_INGEST_TOKEN absent : heartbeat monitor désactivé")
            return
        self._task = asyncio.create_task(self._loop(), name=f"health-monitor:{self.service}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._http.aclose()

    async def _payload(self) -> dict:
        extra = await self._build() if self._build else {}
        return {
            "service": self.service,
            "status": extra.get("status", "ok"),
            "version": self.version,
            "uptime_s": int(time.monotonic() - self._started),
            "checks": extra.get("checks", {}),
            "meta": extra.get("meta", {}),
        }

    async def _loop(self) -> None:
        while True:
            try:
                response = await self._http.post(
                    f"{self.url}/ingest/heartbeat",
                    json=await self._payload(),
                    headers={"X-Health-Token": self.token},
                )
                if response.is_success:
                    self.incident_active = bool(response.json().get("incident_active"))
                else:
                    log.warning("heartbeat monitor refusé (%s)", response.status_code)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — un échec ne fait que logger
                log.warning("heartbeat monitor failed: %s", exc)
            await asyncio.sleep(self._interval)
