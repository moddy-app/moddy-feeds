"""Accès Redis partagé (queue, commandes, dédup, heartbeat, cursor Bluesky).

Un seul pool `redis.asyncio.Redis` est partagé par tout le process. Les noms de
streams/clés sont centralisés ici pour éviter les chaînes magiques dispersées.
"""

from __future__ import annotations

import redis.asyncio as aioredis

from app.config import settings

# ─── Noms de streams / clés (contrat partagé avec le bot) ──────────────────
STREAM_COMMANDS = "feeds:commands"
STREAM_REPLIES = "feeds:replies"
STREAM_NOTIFICATIONS = "notifications:queue"

CONSUMER_GROUP = "moddy-feeds"

KEY_HEARTBEAT = "feeds:heartbeat"
KEY_BLUESKY_CURSOR = "feeds:bluesky:cursor"
KEY_TWITCH_TOKEN = "feeds:twitch:token"

# TTL de la dédup des événements (7 jours, cf. PROMPT §3).
DEDUP_TTL_SECONDS = 604_800

# Taille max de la queue notifications (anti-explosion mémoire Redis).
NOTIFICATIONS_MAXLEN = 10_000

_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    """Retourne le client Redis singleton (créé à la volée)."""
    global _client
    if _client is None:
        _client = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
        )
    return _client


async def close_redis() -> None:
    """Ferme proprement le pool (arrêt du service)."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def dedup_key(event_id: str) -> str:
    return f"notif:seen:{event_id}"
