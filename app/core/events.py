"""Pipeline d'événements : normalisation, déduplication et publication.

Tous les connecteurs produisent un dict via `make_event()` puis appellent
`publish_event()`. La dédup s'appuie sur Redis (SET NX EX) pour être partagée et
résistante aux redémarrages.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from app.core import redis as r
from app.logging_config import get_logger

log = get_logger(__name__)

EventType = Literal["video", "post", "live", "article"]

# Champs autorisés dans l'événement normalisé (contrat avec le bot, cf. PROMPT §4).
_EVENT_FIELDS = (
    "event_id",
    "platform",
    "type",
    "target_id",
    "author_name",
    "author_avatar",
    "title",
    "content",
    "url",
    "thumbnail",
    "published_at",
)


def make_event(
    *,
    event_id: str,
    platform: str,
    type: EventType,
    target_id: str,
    author_name: str | None = None,
    author_avatar: str | None = None,
    title: str | None = None,
    content: str | None = None,
    url: str | None = None,
    thumbnail: str | None = None,
    published_at: str | None = None,
) -> dict[str, Any]:
    """Construit un événement normalisé (clés stables, valeurs None omises)."""
    event = {
        "event_id": event_id,
        "platform": platform,
        "type": type,
        "target_id": target_id,
        "author_name": author_name,
        "author_avatar": author_avatar,
        "title": title,
        "content": content,
        "url": url,
        "thumbnail": thumbnail,
        "published_at": published_at,
    }
    return {k: v for k, v in event.items() if v is not None or k in ("event_id", "platform", "type", "target_id")}


async def is_duplicate(event_id: str) -> bool:
    """True si l'événement a déjà été vu (pose le marqueur sinon). Atomique via SET NX."""
    client = r.get_redis()
    created = await client.set(r.dedup_key(event_id), "1", nx=True, ex=r.DEDUP_TTL_SECONDS)
    return not created


async def mark_seen(event_id: str) -> None:
    """Marque un événement comme vu SANS le publier (premier poll d'une cible)."""
    client = r.get_redis()
    await client.set(r.dedup_key(event_id), "1", nx=True, ex=r.DEDUP_TTL_SECONDS)


async def publish_event(event: dict[str, Any]) -> bool:
    """Publie sur `notifications:queue` si non dupliqué. Retourne True si publié."""
    event_id = event["event_id"]
    if await is_duplicate(event_id):
        log.debug("dedup skip %s", event_id)
        return False

    client = r.get_redis()
    await client.xadd(
        r.STREAM_NOTIFICATIONS,
        {"data": json.dumps(event, separators=(",", ":"))},
        maxlen=r.NOTIFICATIONS_MAXLEN,
        approximate=True,
    )
    log.info("published %s (%s)", event_id, event.get("type"))
    return True
