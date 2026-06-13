"""Connecteur Twitch — polling Helix /streams (batch de 100), transitions live.

Pas d'EventSub (aucun endpoint exposé). Le token applicatif (client credentials)
est mis en cache Redis avec TTL = expires_in - 300 s. La détection des lives se
fait par batch de 100 user_id par requête.

Anti-faux-positif : un stream "disparu" doit l'être plusieurs cycles consécutifs
avant de reset `live=False` (micro-coupures Twitch).
"""

from __future__ import annotations

from typing import Any


from app.config import settings
from app.connectors.base import (
    Connector,
    ResolveError,
    ResolvedTarget,
    due_for_meta_refresh,
    stamp_meta_refresh,
)
from app.core import redis as r
from app.core.db import Target, save_target_state
from app.core.events import make_event, publish_event
from app.core.http import get_http
from app.logging_config import get_logger

log = get_logger(__name__)

_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
_USERS_URL = "https://api.twitch.tv/helix/users"
_STREAMS_URL = "https://api.twitch.tv/helix/streams"

# Nb de cycles offline consécutifs avant de considérer le live terminé.
_OFFLINE_CONFIRM_CYCLES = 3


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


class TwitchConnector(Connector):
    platform = "twitch"

    # ─── Token applicatif (cache Redis) ──────────────────────────────────────
    async def _get_token(self) -> str:
        if not settings.twitch_configured:
            raise ResolveError("twitch_not_configured", "client id/secret manquants")

        client = r.get_redis()
        if token := await client.get(r.KEY_TWITCH_TOKEN):
            return token

        http = get_http()
        resp = await http.post(
            _TOKEN_URL,
            data={
                "client_id": settings.twitch_client_id,
                "client_secret": settings.twitch_client_secret,
                "grant_type": "client_credentials",
            },
        )
        if resp.status_code != 200:
            raise ResolveError("twitch_auth_failed", f"HTTP {resp.status_code}")
        body = resp.json()
        token = body["access_token"]
        ttl = max(60, int(body.get("expires_in", 3600)) - 300)
        await client.set(r.KEY_TWITCH_TOKEN, token, ex=ttl)
        return token

    async def _headers(self) -> dict[str, str]:
        return {
            "Client-Id": settings.twitch_client_id or "",
            "Authorization": f"Bearer {await self._get_token()}",
        }

    # ─── Résolution login → user_id ──────────────────────────────────────────
    async def resolve(self, identifier: str) -> ResolvedTarget:
        login = identifier.strip().lstrip("@").lower()
        # Supporte une URL twitch.tv/login.
        if "twitch.tv/" in login:
            login = login.rsplit("twitch.tv/", 1)[-1].strip("/").split("?")[0]

        http = get_http()
        resp = await http.get(_USERS_URL, params={"login": login}, headers=await self._headers())
        if resp.status_code != 200:
            raise ResolveError("twitch_api_error", f"HTTP {resp.status_code}")
        data = resp.json().get("data") or []
        if not data:
            raise ResolveError("user_not_found")
        u = data[0]
        return ResolvedTarget(
            target_id=u["id"],
            display_name=u.get("display_name") or u.get("login"),
            avatar_url=u.get("profile_image_url"),
            initial_state={"live": False, "offline_cycles": 0},
        )

    # ─── Polling par batch (appelé par le scheduler) ─────────────────────────
    async def poll_batch(self, targets: list[Target]) -> None:
        """Détecte les transitions live pour un ensemble de cibles Twitch dues.

        Publie directement les événements et persiste l'état (la sémantique
        stateful live/offline ne rentre pas dans le contrat `poll → events`).
        """
        if not targets:
            return
        if not settings.twitch_configured:
            log.warning("twitch poll skipped: not configured")
            return

        headers = await self._headers()
        http = get_http()
        live_now: dict[str, dict[str, Any]] = {}

        for batch in _chunked([t.target_id for t in targets], 100):
            params = [("user_id", uid) for uid in batch] + [("first", "100")]
            resp = await http.get(_STREAMS_URL, params=params, headers=headers)
            if resp.status_code != 200:
                log.warning("twitch /streams HTTP %s", resp.status_code)
                continue
            for s in resp.json().get("data", []):
                live_now[s["user_id"]] = s

        # Rafraîchissement throttlé des avatars (absents de /streams → appel /users).
        await self._refresh_avatars(targets, headers)

        for t in targets:
            await self._reconcile(t, live_now.get(t.target_id))

    async def _refresh_avatars(self, targets: list[Target], headers: dict[str, str]) -> None:
        """Met à jour avatar + display_name au plus une fois/24 h par cible (batch 100)."""
        due = [t for t in targets if due_for_meta_refresh(t.state)]
        if not due:
            return
        http = get_http()
        by_id = {t.target_id: t for t in due}
        for batch in _chunked(list(by_id), 100):
            params = [("id", uid) for uid in batch]
            resp = await http.get(_USERS_URL, params=params, headers=headers)
            if resp.status_code != 200:
                continue
            for u in resp.json().get("data", []):
                t = by_id.get(u["id"])
                if not t:
                    continue
                t.display_name = u.get("display_name") or t.display_name
                t.avatar_url = u.get("profile_image_url") or t.avatar_url
                stamp_meta_refresh(t.state)

    async def _reconcile(self, t: Target, stream: dict[str, Any] | None) -> None:
        was_live = bool(t.state.get("live", False))
        had_event = False

        # Le streamer peut renommer son display_name : rafraîchir quand on l'a.
        if stream and (name := stream.get("user_name")) and name != t.display_name:
            t.display_name = name

        if stream and not was_live:
            # offline → live : nouvelle notif.
            t.state["live"] = True
            t.state["offline_cycles"] = 0
            had_event = await publish_event(
                make_event(
                    event_id=f"twitch:{stream['id']}",
                    platform="twitch",
                    type="live",
                    target_id=t.target_id,
                    author_name=stream.get("user_name") or t.display_name,
                    author_avatar=t.avatar_url,
                    title=stream.get("title"),
                    content=stream.get("game_name"),
                    url=f"https://twitch.tv/{stream.get('user_login', '')}",
                    thumbnail=(stream.get("thumbnail_url") or "")
                    .replace("{width}", "1280")
                    .replace("{height}", "720"),
                    published_at=stream.get("started_at"),
                )
            )
        elif stream and was_live:
            # Toujours live : reset le compteur d'absence.
            t.state["offline_cycles"] = 0
        elif not stream and was_live:
            # Absence : confirmer sur plusieurs cycles avant de reset (micro-coupures).
            cycles = int(t.state.get("offline_cycles", 0)) + 1
            t.state["offline_cycles"] = cycles
            if cycles >= _OFFLINE_CONFIRM_CYCLES:
                t.state["live"] = False
                t.state["offline_cycles"] = 0

        await save_target_state(t, mark_polled=True, had_event=had_event)

    async def poll(self, target: Target) -> list[dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError("Twitch utilise poll_batch (batching /streams)")
