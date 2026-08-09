"""Tests de `publish_events` — la publication par lot ne doit pas trouer la dédup.

L'invariant du service est qu'un événement déjà vu n'est **jamais** re-poussé
sur `notifications:queue`. La version pipelinée regroupe les `SET NX` puis les
`XADD` : ce test vérifie que seuls les événements dont le `SET NX` a réussi sont
publiés. Redis est remplacé par un double en mémoire (test de logique pure).
"""

import pytest

from app.core import events as ev
from app.core import redis as r


class _FakePipeline:
    def __init__(self, client, kind):
        self._client = client
        self._kind = kind
        self._ops = []

    def set(self, key, _value, nx=False, ex=None):
        self._ops.append(key)

    def xadd(self, _stream, fields, **_kwargs):
        self._ops.append(fields)

    async def execute(self):
        if self._kind == "set":
            results = []
            for key in self._ops:
                created = key not in self._client.seen
                self._client.seen.add(key)
                results.append(True if created else None)
            return results
        self._client.published.extend(self._ops)
        return [b"1-1"] * len(self._ops)


class _FakeRedis:
    def __init__(self, seen=()):
        self.seen = set(seen)
        self.published = []
        self._next_kind = "set"

    def pipeline(self, transaction=False):
        kind, self._next_kind = self._next_kind, "xadd"
        return _FakePipeline(self, kind)

    async def set(self, key, _value, nx=False, ex=None):
        created = key not in self.seen
        self.seen.add(key)
        return True if created else None

    async def xadd(self, _stream, fields, **_kwargs):
        self.published.append(fields)
        return b"1-1"


@pytest.fixture
def fake_redis(monkeypatch):
    client = _FakeRedis()
    monkeypatch.setattr(r, "get_redis", lambda: client)
    monkeypatch.setattr(ev.r, "get_redis", lambda: client)
    return client


def _event(eid):
    return ev.make_event(event_id=eid, platform="youtube", type="video", target_id="UC1")


async def test_empty_batch_is_a_noop(fake_redis):
    assert await ev.publish_events([]) == 0
    assert fake_redis.published == []


async def test_all_fresh_events_are_published(fake_redis):
    count = await ev.publish_events([_event("a"), _event("b"), _event("c")])
    assert count == 3
    assert len(fake_redis.published) == 3


async def test_already_seen_events_are_filtered(fake_redis):
    fake_redis.seen.add(r.dedup_key("b"))
    count = await ev.publish_events([_event("a"), _event("b"), _event("c")])
    assert count == 2
    assert len(fake_redis.published) == 2


async def test_fully_duplicated_batch_publishes_nothing(fake_redis):
    fake_redis.seen.update({r.dedup_key("a"), r.dedup_key("b")})
    assert await ev.publish_events([_event("a"), _event("b")]) == 0
    assert fake_redis.published == []


async def test_single_event_batch_still_dedups(fake_redis):
    assert await ev.publish_events([_event("solo")]) == 1
    assert await ev.publish_events([_event("solo")]) == 0
    assert len(fake_redis.published) == 1
