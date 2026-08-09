"""Schedulers — boucle par tick (poll des cibles dues) + heartbeat Railway.

Un tick sélectionne les cibles dont l'intervalle *effectif* est écoulé et les
poll. L'intervalle effectif n'est pas celui demandé par le bot : il est modulé
en base (cf. `db.claim_due_targets`) selon deux régimes — ralenti si la cible
est couverte par un transport push, accéléré si elle a publié récemment.

Trois propriétés tiennent la latence sous contrôle quand le volume monte :

1. **Drain complet** — un tick redemande des lots tant qu'il en trouve, au lieu
   de s'arrêter à un lot fixe. Sans ça, la capacité est plafonnée à
   `batch_limit / tick` cibles/s : au-delà, la file ne se vide plus et
   l'intervalle réel dérive silencieusement.
2. **Cadence fixe** — on dort `tick - durée_du_tick`, pas `tick` tout court,
   pour que la période reste la période même quand les polls sont lents.
3. **Retard mesuré** — le backlog est échantillonné et loggé en `warn` dès
   qu'il dépasse le seuil, parce qu'une dérive silencieuse est invisible autrement.

Twitch est traité par batch de 100 (Helix /streams) ; les autres par cible.
Concurrence bornée par un sémaphore pour ne pas saturer l'event loop ni le pool DB.
"""

from __future__ import annotations

import asyncio

from app.config import (
    HOT_FACTOR,
    HOT_WINDOW_SECONDS,
    POLL_BOUNDS,
    POLLED_PLATFORMS,
    PUSH_FALLBACK_INTERVAL,
    settings,
)
from app.connectors import available_platforms, get_connector
from app.connectors.base import ResolveError
from app.core import db, redis as r
from app.core.events import publish_events
from app.logging_config import get_logger

log = get_logger(__name__)

_FAIL_DISABLE_AFTER = 50

# Échantillonnage du retard : toutes les N secondes (requête d'agrégat non gratuite).
_BACKLOG_SAMPLE_EVERY = 60.0
# Au-delà de ce retard, le service ne tient plus la cadence annoncée.
_BACKLOG_WARN_SECONDS = 30.0


def _bounds_map(key: str) -> dict[str, int]:
    """Défauts ou minimums par plateforme, injectés dans la requête SQL."""
    return {
        p: getattr(POLL_BOUNDS[p], key)
        for p in POLLED_PLATFORMS
        if not POLL_BOUNDS[p].realtime
    }


async def run_scheduler() -> None:
    """Boucle principale du scheduler (tick périodique, cadence fixe)."""
    sem = asyncio.Semaphore(settings.poll_concurrency)
    tick = settings.scheduler_tick_seconds
    loop = asyncio.get_running_loop()
    log.info(
        "scheduler started (tick=%ss, batch=%d, concurrency=%d)",
        tick,
        settings.scheduler_batch_limit,
        settings.poll_concurrency,
    )

    last_sample = 0.0
    while True:
        started = loop.time()
        try:
            await _scheduler_tick(sem)
            if started - last_sample >= _BACKLOG_SAMPLE_EVERY:
                last_sample = started
                await _log_backlog()
        except Exception:  # noqa: BLE001 — un tick raté ne doit pas tuer la boucle
            log.exception("scheduler tick failed")

        # Cadence fixe : la durée du tick est absorbée, pas ajoutée.
        await asyncio.sleep(max(0.0, tick - (loop.time() - started)))


async def _scheduler_tick(sem: asyncio.Semaphore) -> None:
    """Draine la file des cibles dues, par lots, jusqu'à épuisement."""
    platforms = [p for p in available_platforms() if p in POLLED_PLATFORMS]
    if not platforms:
        return

    limit = settings.scheduler_batch_limit
    for _ in range(settings.scheduler_max_batches):
        due = await db.claim_due_targets(
            platforms,
            _bounds_map("default"),
            _bounds_map("min"),
            push_fallback=PUSH_FALLBACK_INTERVAL,
            hot_window=HOT_WINDOW_SECONDS,
            hot_factor=HOT_FACTOR,
            limit=limit,
        )
        if not due:
            return
        await _process_batch(sem, due)
        if len(due) < limit:
            return  # file vidée : inutile de redemander


async def _process_batch(sem: asyncio.Semaphore, due: list) -> None:
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


async def _log_backlog() -> None:
    """Mesure le retard du scheduler et alerte s'il décroche."""
    platforms = [p for p in available_platforms() if p in POLLED_PLATFORMS]
    if not platforms:
        return
    count, overdue = await db.scheduler_backlog(
        platforms,
        _bounds_map("default"),
        _bounds_map("min"),
        push_fallback=PUSH_FALLBACK_INTERVAL,
        hot_window=HOT_WINDOW_SECONDS,
        hot_factor=HOT_FACTOR,
    )
    if overdue >= _BACKLOG_WARN_SECONDS:
        log.warning(
            "scheduler behind: %d targets due, oldest overdue by %.0fs "
            "(augmenter POLL_CONCURRENCY/SCHEDULER_BATCH_LIMIT ou scaler)",
            count,
            overdue,
        )
    elif count:
        log.debug("scheduler backlog: %d due, oldest overdue %.1fs", count, overdue)


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

    published_any = await publish_events(events) > 0
    # `mark_polled=False` : la réservation dans `claim_due_targets` a déjà estampillé.
    await db.save_target_state(target, mark_polled=False, had_event=published_any)


async def run_heartbeat() -> None:
    """Écrit `feeds:heartbeat` périodiquement (healthcheck surveillé par le backend)."""
    client = r.get_redis()
    interval = settings.heartbeat_seconds
    loop = asyncio.get_running_loop()
    while True:
        try:
            await client.set(r.KEY_HEARTBEAT, str(loop.time()), ex=interval * 3)
        except Exception:  # noqa: BLE001
            log.warning("heartbeat write failed")
        await asyncio.sleep(interval)
