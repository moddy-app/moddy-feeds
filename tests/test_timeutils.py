"""Tests des utilitaires de dates."""

from datetime import datetime, timedelta, timezone

from app.core.timeutils import is_future, parse_datetime, too_old


def test_parse_iso_and_rfc822():
    assert parse_datetime("2026-06-12T14:30:00Z") is not None
    assert parse_datetime("Wed, 12 Jun 2026 14:30:00 GMT") is not None
    assert parse_datetime(None) is None
    assert parse_datetime("pas une date") is None


def test_too_old():
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    recent = datetime.now(timezone.utc).isoformat()
    assert too_old(old, hours=24) is True
    assert too_old(recent, hours=24) is False
    # Date illisible → non considérée vieille (on ne filtre pas à l'aveugle).
    assert too_old("bogus") is False


def test_is_future():
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    assert is_future(future) is True
    assert is_future(past) is False
