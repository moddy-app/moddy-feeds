"""Client HTTP asynchrone partagé (httpx) — un seul pool pour tout le process."""

from __future__ import annotations

import httpx

_DEFAULT_HEADERS = {"User-Agent": "Moddy/1.0 (+https://moddy.app)"}

_client: httpx.AsyncClient | None = None


def get_http() -> httpx.AsyncClient:
    """Client httpx singleton avec timeouts et limites de connexions sains."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            headers=_DEFAULT_HEADERS,
            timeout=httpx.Timeout(15.0, connect=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            follow_redirects=False,  # redirections gérées explicitement (anti-SSRF)
        )
    return _client


async def close_http() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
