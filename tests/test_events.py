"""Tests de normalisation des événements (champs requis / None omis)."""

from app.core.events import make_event


def test_required_fields_always_present():
    ev = make_event(
        event_id="youtube:abc",
        platform="youtube",
        type="video",
        target_id="UC123",
    )
    assert ev["event_id"] == "youtube:abc"
    assert ev["platform"] == "youtube"
    assert ev["type"] == "video"
    assert ev["target_id"] == "UC123"
    # Champs optionnels None → omis.
    assert "title" not in ev
    assert "thumbnail" not in ev


def test_optional_fields_kept_when_present():
    ev = make_event(
        event_id="rss:x",
        platform="rss",
        type="article",
        target_id="https://feed",
        title="Hello",
        content="World",
    )
    assert ev["title"] == "Hello"
    assert ev["content"] == "World"
