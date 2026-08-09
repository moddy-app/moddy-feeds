"""Test de l'invariant qui fait cohabiter polling et EventSub sur Twitch.

Les deux transports peuvent détecter le même passage en live. Ce qui empêche la
double notification, c'est que tous deux produisent le **même `event_id`** —
la dédup Redis fait le reste. Si ce test casse, un stream notifié par EventSub
sera re-notifié par le poll quelques secondes plus tard.
"""

from app.connectors.twitch import make_live_event

STREAM = {
    "id": "42424242",
    "user_id": "1234",
    "user_name": "Streameuse",
    "user_login": "streameuse",
    "title": "on refait le monde",
    "game_name": "Just Chatting",
    "thumbnail_url": "https://static-cdn.jtvnw.net/previews/streameuse-{width}x{height}.jpg",
    "started_at": "2026-08-09T18:00:00Z",
}


def test_event_id_derives_from_stream_id():
    event = make_live_event(STREAM, "Streameuse", None)
    assert event["event_id"] == "twitch:42424242"
    assert event["target_id"] == "1234"
    assert event["type"] == "live"


def test_both_transports_produce_the_same_event_id():
    # Chemin polling : objet /streams complet.
    polled = make_live_event(STREAM, "Streameuse", None)
    # Chemin EventSub : payload minimal, enrichissement /streams indisponible.
    pushed = make_live_event(
        {
            "id": STREAM["id"],
            "user_id": STREAM["user_id"],
            "user_name": STREAM["user_name"],
            "user_login": STREAM["user_login"],
            "started_at": STREAM["started_at"],
        },
        "Streameuse",
        None,
    )
    assert polled["event_id"] == pushed["event_id"]
    assert polled["target_id"] == pushed["target_id"]


def test_thumbnail_placeholders_are_resolved():
    event = make_live_event(STREAM, "Streameuse", None)
    assert "{width}" not in event["thumbnail"]
    assert "1280x720" in event["thumbnail"]


def test_minimal_payload_omits_empty_fields():
    event = make_live_event(
        {"id": "1", "user_id": "2", "user_login": "x"}, None, None
    )
    # `make_event` retire les valeurs None : pas de clé title/content fantôme.
    assert "title" not in event
    assert "content" not in event


# ─── Transitions live/offline (logique pure, sans I/O) ─────────────────────
class _T:
    """Double minimal de `Target` pour la logique de transition."""

    def __init__(self, state=None, display_name="Streameuse", avatar_url=None):
        self.state = state if state is not None else {"live": False, "offline_cycles": 0}
        self.display_name = display_name
        self.avatar_url = avatar_url


def _transition(target, stream):
    from app.connectors.twitch import TwitchConnector

    return TwitchConnector._transition(target, stream)


def test_offline_to_live_emits_event():
    t = _T()
    event = _transition(t, STREAM)
    assert event is not None and event["event_id"] == "twitch:42424242"
    assert t.state["live"] is True


def test_still_live_emits_nothing():
    t = _T({"live": True, "offline_cycles": 2})
    assert _transition(t, STREAM) is None
    # Le compteur d'absence est remis à zéro tant que le stream est vu.
    assert t.state["offline_cycles"] == 0
    assert t.state["live"] is True


def test_offline_needs_three_cycles_before_reset():
    t = _T({"live": True, "offline_cycles": 0})
    for expected in (1, 2):
        assert _transition(t, None) is None
        assert t.state["live"] is True, "micro-coupure : ne pas clore le live trop tôt"
        assert t.state["offline_cycles"] == expected
    assert _transition(t, None) is None
    assert t.state["live"] is False
    assert t.state["offline_cycles"] == 0


def test_relive_after_confirmed_offline_emits_again():
    t = _T({"live": True, "offline_cycles": 2})
    _transition(t, None)                     # 3ᵉ cycle → offline confirmé
    assert t.state["live"] is False
    assert _transition(t, STREAM) is not None  # nouveau live → nouvelle notif


def test_display_name_refreshed_opportunistically():
    t = _T(display_name="Ancien Nom")
    _transition(t, STREAM)
    assert t.display_name == "Streameuse"


def test_offline_while_already_offline_is_noop():
    t = _T({"live": False, "offline_cycles": 0})
    assert _transition(t, None) is None
    assert t.state == {"live": False, "offline_cycles": 0}
