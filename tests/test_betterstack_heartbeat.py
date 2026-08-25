"""Tests du heartbeat Better Stack : choix de l'URL pingée selon le statut."""

import asyncio

import httpx
import pytest

from app.core.betterstack_heartbeat import BetterStackHeartbeat


def _client_with_transport(url: str, status: str, calls: list[str]) -> BetterStackHeartbeat:
    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200)

    async def build_status() -> str:
        return status

    client = BetterStackHeartbeat(url=url, build_status=build_status)
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.asyncio
async def test_pings_base_url_when_status_ok():
    calls: list[str] = []
    client = _client_with_transport("https://uptime.betterstack.com/api/v1/heartbeat/tok", "ok", calls)
    task = asyncio.create_task(client._loop())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await client._http.aclose()
    assert calls == ["https://uptime.betterstack.com/api/v1/heartbeat/tok"]


@pytest.mark.asyncio
async def test_pings_fail_url_when_status_down():
    calls: list[str] = []
    client = _client_with_transport("https://uptime.betterstack.com/api/v1/heartbeat/tok", "down", calls)
    task = asyncio.create_task(client._loop())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await client._http.aclose()
    assert calls == ["https://uptime.betterstack.com/api/v1/heartbeat/tok/fail"]


def test_disabled_without_url():
    client = BetterStackHeartbeat(url=None)
    client.start()
    assert client._task is None
