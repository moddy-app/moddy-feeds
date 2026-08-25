"""Heartbeat sortant vers Better Stack — ping périodique (cron/heartbeat monitor).

Toutes les BETTERSTACK_HEARTBEAT_INTERVAL_SECONDS (déf. 180 s = 3 min), un GET
est envoyé sur l'URL secrète du heartbeat Better Stack tant que le service se
considère en bonne santé (cf. `build_health_checks` dans `app/schedulers.py`).
Si le service est `down`, un GET est envoyé sur `<url>/fail` à la place, pour
déclencher l'incident sans attendre l'expiration du délai de grâce. Si
`BETTERSTACK_HEARTBEAT_URL` est absent, le client se désactive proprement
(utile en dev local).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx

from app.logging_config import get_logger

log = get_logger(__name__)

BETTERSTACK_HEARTBEAT_INTERVAL_SECONDS = 180


class BetterStackHeartbeat:
    """Boucle asyncio isolée qui ping périodiquement un heartbeat Better Stack."""

    def __init__(
        self,
        *,
        url: str | None,
        build_status: Callable[[], Awaitable[str]] | None = None,
        interval: int = BETTERSTACK_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self.url = (url or "").rstrip("/")
        # Coroutine renvoyant "ok"/"degraded"/"down" (cf. build_health_checks).
        self._build_status = build_status
        self._interval = interval
        self._task: asyncio.Task | None = None
        self._http = httpx.AsyncClient(timeout=5)

    def start(self) -> None:
        if self._task is not None:
            return
        if not self.url:
            log.warning("BETTERSTACK_HEARTBEAT_URL absent : heartbeat Better Stack désactivé")
            return
        self._task = asyncio.create_task(self._loop(), name="betterstack-heartbeat")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._http.aclose()

    async def _loop(self) -> None:
        while True:
            try:
                status = await self._build_status() if self._build_status else "ok"
                target = self.url if status == "ok" else f"{self.url}/fail"
                response = await self._http.get(target)
                if not response.is_success:
                    log.warning("betterstack heartbeat refusé (%s)", response.status_code)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — un échec ne fait que logger
                log.warning("betterstack heartbeat failed: %s", exc)
            await asyncio.sleep(self._interval)
