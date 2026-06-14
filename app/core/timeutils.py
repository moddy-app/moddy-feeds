"""Utilitaires de dates — parsing tolérant et filtre anti-vieux-contenu."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


def parse_datetime(value: str | None) -> datetime | None:
    """Parse une date ISO 8601 ou RFC 822 (RSS) en datetime aware. None si échec."""
    if not value:
        return None
    raw = value.strip()
    # ISO 8601 (avec ou sans 'Z').
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        pass
    # RFC 822 (flux RSS classiques).
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


def too_old(published: str | None, *, hours: int = 24) -> bool:
    """True si la date de publication est antérieure à `hours` (re-publications, 1er poll).

    Une date absente ou illisible est considérée NON vieille (on ne filtre pas à l'aveugle).
    """
    dt = parse_datetime(published)
    if dt is None:
        return False
    age = datetime.now(timezone.utc) - dt
    return age.total_seconds() > hours * 3600


def is_future(published: str | None) -> bool:
    """True si la date est dans le futur (live programmé YouTube)."""
    dt = parse_datetime(published)
    if dt is None:
        return False
    return dt > datetime.now(timezone.utc)
