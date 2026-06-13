"""Point d'entrée — lance tous les workers asyncio (aucun serveur HTTP).

Tâches lancées en parallèle :
  • consumer feeds:commands  (abonnements / désabonnements)
  • scheduler tick           (polling YouTube / RSS / Twitch / Instagram)
  • worker Bluesky Jetstream (websocket temps réel)
  • heartbeat                (healthcheck Railway via Redis)

Arrêt propre sur SIGINT/SIGTERM : annulation des tâches puis fermeture des pools.
"""

from __future__ import annotations

import asyncio
import signal

from app.commands import run_commands_consumer
from app.config import settings
from app.connectors import BlueskyConnector, get_connector
from app.core.db import close_db, init_db
from app.core.http import close_http
from app.core.redis import close_redis, get_redis
from app.logging_config import get_logger, setup_logging
from app.schedulers import run_heartbeat, run_scheduler

log = get_logger("moddy-feeds")


async def _bootstrap() -> None:
    """Vérifie les connexions infra avant de lancer les workers."""
    await init_db()
    await get_redis().ping()
    log.info("infrastructure ready (db + redis)")


async def main() -> None:
    setup_logging()
    log.info("starting moddy-feeds…")
    await _bootstrap()

    tasks: list[asyncio.Task] = [
        asyncio.create_task(run_commands_consumer(), name="commands"),
        asyncio.create_task(run_scheduler(), name="scheduler"),
        asyncio.create_task(run_heartbeat(), name="heartbeat"),
    ]

    if settings.bluesky_enabled:
        bluesky: BlueskyConnector = get_connector("bluesky")  # type: ignore[assignment]
        tasks.append(asyncio.create_task(bluesky.run(), name="bluesky"))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover (non-unix)
            pass

    log.info("running %d workers", len(tasks))

    # Si une tâche meurt, on arrête tout (Railway redémarre le process).
    done_task = asyncio.create_task(stop.wait(), name="stop")
    await asyncio.wait({*tasks, done_task}, return_when=asyncio.FIRST_COMPLETED)

    log.info("shutting down…")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await _shutdown()


async def _shutdown() -> None:
    await close_http()
    await close_redis()
    await close_db()
    log.info("bye")


if __name__ == "__main__":
    asyncio.run(main())
