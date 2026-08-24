"""Tests de la décision de statut pour le heartbeat Moddy Health Monitor.

Logique pure, sans infra : `_status_from_checks` prend un dict de checks tout
fait (doublures) et décide `down`/`degraded`/`ok`.
"""

from app.schedulers import _status_from_checks


def test_down_when_vital_dependency_fails():
    checks = {
        "postgres": {"ok": False, "error": "connection refused"},
        "redis": {"ok": True},
        "scheduler": {"ok": True},
    }
    assert _status_from_checks(checks) == "down"


def test_down_when_redis_fails():
    checks = {
        "postgres": {"ok": True},
        "redis": {"ok": False, "error": "timeout"},
        "scheduler": {"ok": True},
    }
    assert _status_from_checks(checks) == "down"


def test_degraded_when_secondary_check_fails():
    checks = {
        "postgres": {"ok": True},
        "redis": {"ok": True},
        "scheduler": {"ok": False, "last_tick_age_s": 999},
    }
    assert _status_from_checks(checks) == "degraded"


def test_ok_when_all_checks_pass():
    checks = {
        "postgres": {"ok": True},
        "redis": {"ok": True},
        "scheduler": {"ok": True},
    }
    assert _status_from_checks(checks) == "ok"
