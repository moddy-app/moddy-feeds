"""Consumer du stream `feeds:commands` → réponses sur `feeds:replies`.

Protocole (cf. PROMPT §2) :
  subscribe   : résout l'identifiant, clamp poll_interval, upsert la cible,
                amorce la dédup (premier poll), répond avec la forme canonique.
  unsubscribe : recalcule poll_interval restant ou supprime la cible si plus suivie.

Le consumer group `moddy-feeds` garantit qu'une commande n'est traitée qu'une fois
même avec plusieurs instances du service (scalabilité horizontale).
"""

from __future__ import annotations

import asyncio
import json
import socket
from typing import Any

import redis.exceptions

from app.config import POLL_BOUNDS, SUPPORTED_PLATFORMS
from app.connectors import available_platforms, get_connector
from app.connectors.base import ResolveError
from app.core import db, redis as r
from app.logging_config import get_logger

log = get_logger(__name__)

_CONSUMER_NAME = f"{socket.gethostname()}-{id(object())}"


async def ensure_group() -> None:
    """Crée le consumer group s'il n'existe pas (idempotent)."""
    client = r.get_redis()
    try:
        await client.xgroup_create(r.STREAM_COMMANDS, r.CONSUMER_GROUP, id="0", mkstream=True)
        log.info("created consumer group %s", r.CONSUMER_GROUP)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def run_commands_consumer() -> None:
    """Boucle bloquante : lit les commandes et les traite une par une."""
    await ensure_group()
    client = r.get_redis()
    log.info("commands consumer started as %s", _CONSUMER_NAME)

    while True:
        try:
            resp = await client.xreadgroup(
                r.CONSUMER_GROUP,
                _CONSUMER_NAME,
                {r.STREAM_COMMANDS: ">"},
                count=10,
                block=5000,
            )
        except redis.exceptions.ConnectionError as exc:
            log.warning("redis connection error: %s — retry in 2s", exc)
            await asyncio.sleep(2)
            continue

        if not resp:
            continue

        for _stream, messages in resp:
            for msg_id, fields in messages:
                await _process(msg_id, fields)
                await client.xack(r.STREAM_COMMANDS, r.CONSUMER_GROUP, msg_id)


async def _process(msg_id: str, fields: dict[str, str]) -> None:
    try:
        payload = json.loads(fields.get("data", "{}"))
    except json.JSONDecodeError:
        log.error("invalid command payload (id=%s): %r", msg_id, fields)
        return

    action = payload.get("action")
    request_id = payload.get("request_id")
    try:
        if action == "subscribe":
            await _handle_subscribe(payload)
        elif action == "unsubscribe":
            await _handle_unsubscribe(payload)
        else:
            await _reply(request_id, ok=False, error="unknown_action")
    except ResolveError as exc:
        log.info("resolve failed (%s): %s", request_id, exc.code)
        await _reply(request_id, ok=False, error=exc.code, platform=payload.get("platform"))
    except Exception:  # noqa: BLE001 — une commande ne doit jamais tuer le consumer
        log.exception("command processing crashed (id=%s)", msg_id)
        await _reply(request_id, ok=False, error="internal_error", platform=payload.get("platform"))


async def _handle_subscribe(payload: dict[str, Any]) -> None:
    request_id = payload.get("request_id")
    platform = payload.get("platform")
    identifier = payload.get("identifier")

    if platform not in SUPPORTED_PLATFORMS:
        return await _reply(request_id, ok=False, error="unknown_platform")
    if platform not in available_platforms():
        return await _reply(request_id, ok=False, error="platform_disabled", platform=platform)
    if not identifier:
        return await _reply(request_id, ok=False, error="missing_identifier", platform=platform)

    connector = get_connector(platform)
    resolved = await connector.resolve(identifier)

    poll_interval = POLL_BOUNDS[platform].clamp(payload.get("poll_interval"))

    created = await db.upsert_target(
        platform=platform,
        target_id=resolved.target_id,
        display_name=resolved.display_name,
        avatar_url=resolved.avatar_url,
        poll_interval=poll_interval,
        initial_state=resolved.initial_state,
    )

    target = await db.get_target(platform, resolved.target_id)
    if target is None:  # garde-fou ; ne devrait pas arriver
        return await _reply(request_id, ok=False, error="internal_error", platform=platform)

    # Premier poll d'une nouvelle cible : amorcer la dédup sans publier.
    if created and not connector.realtime:
        try:
            await connector.prime(target)
            await db.save_target_state(target, mark_polled=True)
        except Exception:  # noqa: BLE001
            log.exception("prime failed for %s:%s", platform, resolved.target_id)

    # Hook connecteur (Bluesky : ajouter le DID à la connexion).
    await connector.on_subscribe(target)

    log.info("subscribed %s:%s (poll=%s, new=%s)", platform, resolved.target_id, poll_interval, created)
    await _reply(
        request_id,
        ok=True,
        platform=platform,
        target_id=resolved.target_id,
        display_name=resolved.display_name,
        avatar_url=resolved.avatar_url,
        poll_interval=poll_interval,
    )


async def _handle_unsubscribe(payload: dict[str, Any]) -> None:
    request_id = payload.get("request_id")
    platform = payload.get("platform")
    identifier = payload.get("identifier")

    if platform not in SUPPORTED_PLATFORMS:
        return await _reply(request_id, ok=False, error="unknown_platform")

    connector = get_connector(platform)

    # Le bot peut envoyer soit la forme canonique, soit l'identifiant libre.
    target = await db.get_target(platform, identifier)
    if target is None:
        try:
            resolved = await connector.resolve(identifier)
            target = await db.get_target(platform, resolved.target_id)
        except ResolveError:
            target = None

    if target is None:
        # Déjà absente : succès idempotent.
        return await _reply(request_id, ok=True, platform=platform, removed=True)

    target_id = target.target_id

    # Le bot fournit l'intervalle le plus exigeant restant parmi ses guilds.
    remaining = payload.get("poll_interval")
    if remaining is not None:
        clamped = POLL_BOUNDS[platform].clamp(remaining)
        await db.set_poll_interval(platform, target_id, clamped)
        log.info("unsubscribe %s:%s — poll_interval → %s", platform, target_id, clamped)
        return await _reply(request_id, ok=True, platform=platform, target_id=target_id, poll_interval=clamped)

    # Plus aucune guild : suppression complète.
    await db.delete_target(platform, target_id)
    await connector.on_unsubscribe(target_id)
    log.info("unsubscribed %s:%s (removed)", platform, target_id)
    await _reply(request_id, ok=True, platform=platform, target_id=target_id, removed=True)


async def _reply(request_id: str | None, **fields: Any) -> None:
    """Publie une réponse corrélée sur `feeds:replies`."""
    if request_id is None:
        return
    body = {"request_id": request_id, **fields}
    client = r.get_redis()
    await client.xadd(
        r.STREAM_REPLIES,
        {"data": json.dumps(body, separators=(",", ":"))},
        maxlen=10_000,
        approximate=True,
    )
