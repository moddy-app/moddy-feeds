"""Tests des bornes de poll_interval (clamp par plateforme)."""

from app.config import POLL_BOUNDS


def test_youtube_clamp_within_bounds():
    b = POLL_BOUNDS["youtube"]
    assert b.clamp(120) == 120
    assert b.clamp(10) == 60       # sous le min → min
    assert b.clamp(99999) == 3600  # au-dessus du max → max
    assert b.clamp(None) == 300    # défaut


def test_twitch_bounds():
    b = POLL_BOUNDS["twitch"]
    assert b.clamp(5) == 30
    assert b.clamp(9999) == 600
    assert b.clamp(None) == 60


def test_bluesky_realtime_ignores_interval():
    b = POLL_BOUNDS["bluesky"]
    assert b.realtime is True
    assert b.clamp(120) is None
    assert b.clamp(None) is None


def test_instagram_defaults():
    b = POLL_BOUNDS["instagram"]
    assert b.clamp(None) == 1800
    assert b.clamp(100) == 600
