"""Connecteurs plateformes : registre central résolu par nom de plateforme."""

from __future__ import annotations

from app.config import settings
from app.connectors.base import Connector, ResolveError
from app.connectors.bluesky import BlueskyConnector
from app.connectors.instagram import InstagramConnector
from app.connectors.rss import RSSConnector
from app.connectors.twitch import TwitchConnector
from app.connectors.youtube import YouTubeConnector

# Instances singletons (les connecteurs sont sans état partagé mutable).
_REGISTRY: dict[str, Connector] = {
    "youtube": YouTubeConnector(),
    "twitch": TwitchConnector(),
    "bluesky": BlueskyConnector(),
    "rss": RSSConnector(),
    "instagram": InstagramConnector(),
}


def get_connector(platform: str) -> Connector:
    """Retourne le connecteur d'une plateforme. Lève KeyError si inconnue."""
    return _REGISTRY[platform]


def available_platforms() -> list[str]:
    """Plateformes activées selon la config (Instagram off par défaut)."""
    platforms = ["youtube", "twitch", "rss"]
    if settings.bluesky_enabled:
        platforms.append("bluesky")
    if settings.instagram_enabled:
        platforms.append("instagram")
    return platforms


__all__ = [
    "Connector",
    "ResolveError",
    "get_connector",
    "available_platforms",
    "BlueskyConnector",
]
