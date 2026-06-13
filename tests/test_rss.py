"""Tests utilitaires RSS (strip_html, déterminisme de l'event_id)."""

from app.connectors.rss import _event_id, strip_html


def test_strip_html():
    assert strip_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert strip_html("") == ""


def test_event_id_is_deterministic_and_prefixed():
    a = _event_id("https://feed.xml", "guid-1")
    b = _event_id("https://feed.xml", "guid-1")
    c = _event_id("https://feed.xml", "guid-2")
    assert a == b
    assert a != c
    assert a.startswith("rss:")
