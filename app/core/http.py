"""Client HTTP asynchrone partagé (httpx) — un seul pool pour tout le process."""

from __future__ import annotations

import httpx

_DEFAULT_HEADERS = {"User-Agent": "Moddy/1.0 (+https://moddy.app)"}

_client: httpx.AsyncClient | None = None


def get_http() -> httpx.AsyncClient:
    """Client httpx singleton avec timeouts et limites de connexions sains.

    Deux réglages comptent pour la latence :

    - **HTTP/2** : le trafic se concentre sur une poignée d'hôtes
      (youtube.com, api.twitch.tv), donc le multiplexage évite d'ouvrir une
      connexion par poll concurrent.
    - **keepalive = max_connections** : à 20 keepalive pour 100 connexions, la
      majorité des polls repayait un handshake TLS complet (~100-200 ms) alors
      que le fetch conditionnel derrière ne coûte qu'un 304.

    Timeouts serrés volontairement : un poll qui traîne retarde la file entière,
    et il sera de toute façon retenté au tick suivant.
    """
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            headers=_DEFAULT_HEADERS,
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=100,
                keepalive_expiry=60.0,
            ),
            http2=True,
            follow_redirects=False,  # redirections gérées explicitement (anti-SSRF)
        )
    return _client


async def close_http() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
