"""Schedulers — boucle par tick (poll des cibles dues) + heartbeat Railway.

Au lieu d'une boucle globale par plateforme, un tick unique (10 s par défaut)
sélectionne les cibles dont `last_poll_at + poll_interval <= now()` et les poll.
Twitch est traité par batch de 100 (Helix /streams) ; les autres par cible.

Concurrence bornée par un sémaphore pour rester scalable sans saturer la cible
ni l'event loop.
"""

from __future__ import annotations

import asyncio
import time

from app.config import POLL_BOUNDS, POLLED_PLATFORMS, settings
from app.connectors import available_platforms, get_connector
from app.connectors.base import ResolveError
from app.core import db, redis as r
from app.core.events import publish_event
from app.logging_config import get_logger

log = get_logger(__name__)

# Concurrence max de polls HTTP simultanés (par tick).
_POLL_CONCURRENCY = 20
_FAIL_DISABLE_AFTER = 50

# Horodatage (monotonic) du dernier tick scheduler qui s'est terminé sans lever
# — utilisé par `build_health_checks()` pour détecter un scheduler figé.
_last_tick_at: float | None = None
# Un scheduler considéré figé au-delà de ce multiple du tick (cf. build_health_checks).
_STALL_FACTOR = 3


def _default_intervals() -> dict[str, int]:
    """Défaut plateforme injecté dans la requête SQL des cibles dues."""
    return {p: POLL_BOUNDS[p].default for p in POLLED_PLATFORMS if not POLL_BOUNDS[p].realtime}


async def run_scheduler() -> None:
    """Boucle principale du scheduler (tick périodique)."""
    sem = asyncio.Semaphore(_POLL_CONCURRENCY)
    tick = settings.scheduler_tick_seconds
    log.info("scheduler started (tick=%ss)", tick)

    while True:
        try:
            await _scheduler_tick(sem)
            global _last_tick_at
            _last_tick_at = time.monotonic()
        except Exception:  # noqa: BLE001 — un tick raté ne doit pas tuer la boucle
            log.exception("scheduler tick failed")
        await asyncio.sleep(tick)


async def _scheduler_tick(sem: asyncio.Semaphore) -> None:
    platforms = [p for p in available_platforms() if p in POLLED_PLATFORMS]
    if not platforms:
        return

    due = await db.fetch_due_targets(platforms, _default_intervals(), settings.scheduler_batch_limit)
    if not due:
        return

    # Twitch : traitement groupé (batching /streams).
    twitch_targets = [t for t in due if t.platform == "twitch"]
    other_targets = [t for t in due if t.platform != "twitch"]

    tasks: list[asyncio.Task] = []
    if twitch_targets:
        tasks.append(asyncio.create_task(_poll_twitch(twitch_targets)))
    for target in other_targets:
        tasks.append(asyncio.create_task(_poll_one(sem, target)))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _poll_twitch(targets: list) -> None:
    connector = get_connector("twitch")
    try:
        await connector.poll_batch(targets)  # gère lui-même publish + save_state
    except Exception:  # noqa: BLE001
        log.exception("twitch batch poll failed (%d targets)", len(targets))


async def _poll_one(sem: asyncio.Semaphore, target) -> None:
    connector = get_connector(target.platform)
    async with sem:
        try:
            events = await connector.poll(target)
        except ResolveError as exc:
            log.warning("poll soft-fail %s:%s — %s", target.platform, target.target_id, exc.code)
            await db.register_failure(target, disable_after=_FAIL_DISABLE_AFTER)
            return
        except Exception:  # noqa: BLE001
            log.exception("poll crashed %s:%s", target.platform, target.target_id)
            await db.register_failure(target, disable_after=_FAIL_DISABLE_AFTER)
            return

    published_any = False
    for event in events:
        if await publish_event(event):
            published_any = True

    await db.save_target_state(target, mark_polled=True, had_event=published_any)


async def build_health_checks() -> dict:
    """Construit `{status, checks, meta}` pour le Moddy Health Monitor.

    Dépendances vitales (postgres, redis) mortes → `down`. Scheduler figé
    (aucun tick réussi depuis `_STALL_FACTOR` × l'intervalle) → `degraded` :
    le process est vivant mais ne fait plus son travail.
    """
    checks: dict[str, dict] = {}

    start = time.perf_counter()
    try:
        async with asyncio.timeout(2):
            async with db.get_pool().acquire() as conn:
                await conn.fetchval("SELECT 1")
        checks["postgres"] = {"ok": True, "latency_ms": round((time.perf_counter() - start) * 1000)}
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = {"ok": False, "error": str(exc)[:120]}

    start = time.perf_counter()
    try:
        async with asyncio.timeout(2):
            await r.get_redis().ping()
        checks["redis"] = {"ok": True, "latency_ms": round((time.perf_counter() - start) * 1000)}
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = {"ok": False, "error": str(exc)[:120]}

    tick = settings.scheduler_tick_seconds
    stall_age = None
    if _last_tick_at is not None:
        stall_age = time.monotonic() - _last_tick_at
    scheduler_ok = stall_age is not None and stall_age <= tick * _STALL_FACTOR
    checks["scheduler"] = {"ok": scheduler_ok, "last_tick_age_s": round(stall_age) if stall_age is not None else None}

    return {
        "status": _status_from_checks(checks),
        "checks": checks,
        "meta": {"last_tick_age_s": checks["scheduler"]["last_tick_age_s"]},
    }


def _status_from_checks(checks: dict[str, dict]) -> str:
    """Décide le statut global à partir des checks (pure, testable sans infra).

    `postgres`/`redis` sont vitaux (indisponibles → `down`). `scheduler` est
    secondaire : le process répond mais ne fait plus son travail → `degraded`.
    """
    if not checks["postgres"]["ok"] or not checks["redis"]["ok"]:
        return "down"
    if not all(c["ok"] for c in checks.values()):
        return "degraded"
    return "ok"


async def build_health_status() -> str:
    """Statut seul (`ok`/`degraded`/`down`), pour le heartbeat Better Stack."""
    checks = await build_health_checks()
    return checks["status"]


async def run_heartbeat() -> None:
    """Écrit `feeds:heartbeat` périodiquement (healthcheck surveillé par le backend)."""
    client = r.get_redis()
    interval = settings.heartbeat_seconds
    while True:
        try:
            await client.set(r.KEY_HEARTBEAT, str(asyncio.get_event_loop().time()), ex=interval * 3)
        except Exception:  # noqa: BLE001
            log.warning("heartbeat write failed")
        await asyncio.sleep(interval)
