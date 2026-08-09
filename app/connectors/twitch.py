"""Connecteur Twitch — polling Helix /streams (batch de 100), transitions live.

Pas d'EventSub (aucun endpoint exposé). Le token applicatif (client credentials)
est mis en cache Redis avec TTL = expires_in - 300 s. La détection des lives se
fait par batch de 100 user_id par requête.

Anti-faux-positif : un stream "disparu" doit l'être plusieurs cycles consécutifs
avant de reset `live=False` (micro-coupures Twitch).
"""

from __future__ import annotations

import asyncio
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


def make_live_event(
    stream: dict[str, Any], display_name: str | None, avatar_url: str | None
) -> dict[str, Any]:
    """Événement `live` normalisé depuis un objet stream Helix.

    Partagé avec le worker EventSub : les deux transports doivent produire
    **exactement le même `event_id`** (`twitch:{stream_id}`), c'est ce qui permet
    à la dédup Redis de les faire cohabiter sans double notification — le plus
    rapide des deux gagne, l'autre est ignoré.
    """
    return make_event(
        event_id=f"twitch:{stream['id']}",
        platform="twitch",
        type="live",
        target_id=stream["user_id"],
        author_name=stream.get("user_name") or display_name,
        author_avatar=avatar_url,
        title=stream.get("title"),
        content=stream.get("game_name"),
        url=f"https://twitch.tv/{stream.get('user_login', '')}",
        thumbnail=(stream.get("thumbnail_url") or "")
        .replace("{width}", "1280")
        .replace("{height}", "720"),
        published_at=stream.get("started_at"),
    )


class TwitchConnector(Connector):
    platform = "twitch"

    def __init__(self) -> None:
        # Renseigné par le worker EventSub à son démarrage (s'il est configuré).
        # Permet aux hooks abonnement/désabonnement de reconfigurer le websocket
        # à chaud, sans que ce module dépende du worker (import à sens unique).
        self.eventsub: Any | None = None

    async def on_subscribe(self, target: Target) -> None:
        if self.eventsub is not None:
            await self.eventsub.watch(target.target_id)

    async def on_unsubscribe(self, target_id: str) -> None:
        if self.eventsub is not None:
            await self.eventsub.unwatch(target_id)

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

    async def app_headers(self) -> dict[str, str]:
        """En-têtes Helix avec le token applicatif (réutilisé par le worker EventSub)."""
        return await self._headers()

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
        live_now = await self._fetch_live(targets, headers)

        # L'ordre des trois étapes suivantes est ce qui tient la latence :
        # calculer les transitions (pur, sans I/O), PUBLIER, puis seulement
        # persister. Publier en série avec un write DB entre chaque, sur un lot
        # de 500 cibles, faisait attendre 499 aller-retours à la dernière notif.
        pending: list[tuple[Target, dict[str, Any]]] = []
        for t in targets:
            if event := self._transition(t, live_now.get(t.target_id)):
                pending.append((t, event))

        published = (
            await asyncio.gather(*(publish_event(e) for _, e in pending)) if pending else []
        )
        with_event = {id(t) for (t, _), ok in zip(pending, published) if ok}

        # Hors du chemin critique : appel /users throttlé, puis une seule
        # écriture par cible (l'état d'avatar est déjà muté à ce stade).
        await self._refresh_avatars(targets, headers)
        await asyncio.gather(
            *(
                save_target_state(t, mark_polled=False, had_event=id(t) in with_event)
                for t in targets
            ),
            return_exceptions=True,
        )

    async def _fetch_live(
        self, targets: list[Target], headers: dict[str, str]
    ) -> dict[str, dict[str, Any]]:
        """Interroge /streams par lots de 100, les lots **en parallèle**.

        En séquentiel, 500 cibles = 5 aller-retours en série avant la première
        publication ; ce délai s'ajoute tel quel à la latence de notification.
        """
        http = get_http()
        chunks = _chunked([t.target_id for t in targets], 100)

        async def fetch(batch: list[str]) -> list[dict[str, Any]]:
            params = [("user_id", uid) for uid in batch] + [("first", "100")]
            resp = await http.get(_STREAMS_URL, params=params, headers=headers)
            if resp.status_code != 200:
                log.warning("twitch /streams HTTP %s", resp.status_code)
                return []
            return resp.json().get("data", [])

        live_now: dict[str, dict[str, Any]] = {}
        for result in await asyncio.gather(*(fetch(c) for c in chunks), return_exceptions=True):
            if isinstance(result, BaseException):
                log.warning("twitch /streams chunk failed: %s", result)
                continue
            for s in result:
                live_now[s["user_id"]] = s
        return live_now

    async def _refresh_avatars(self, targets: list[Target], headers: dict[str, str]) -> None:
        """Met à jour avatar + display_name au plus une fois/24 h par cible (batch 100)."""
        due = [t for t in targets if due_for_meta_refresh(t.state)]
        if not due:
            return
        http = get_http()
        by_id = {t.target_id: t for t in due}

        async def fetch(batch: list[str]) -> list[dict[str, Any]]:
            params = [("id", uid) for uid in batch]
            resp = await http.get(_USERS_URL, params=params, headers=headers)
            if resp.status_code != 200:
                return []
            return resp.json().get("data", [])

        chunks = _chunked(list(by_id), 100)
        for result in await asyncio.gather(*(fetch(c) for c in chunks), return_exceptions=True):
            if isinstance(result, BaseException):
                log.warning("twitch /users chunk failed: %s", result)
                continue
            for u in result:
                t = by_id.get(u["id"])
                if not t:
                    continue
                t.display_name = u.get("display_name") or t.display_name
                t.avatar_url = u.get("profile_image_url") or t.avatar_url
                stamp_meta_refresh(t.state)
                # Pas de write ici : l'appelant persiste tout en une passe.

    @staticmethod
    def _transition(t: Target, stream: dict[str, Any] | None) -> dict[str, Any] | None:
        """Applique la transition live/offline à `t.state`. Retourne l'event à publier.

        Fonction **pure d'I/O** (elle ne fait que muter l'état en mémoire), pour
        que l'appelant puisse publier tout un lot d'un coup puis persister.
        """
        was_live = bool(t.state.get("live", False))

        # Le streamer peut renommer son display_name : rafraîchir quand on l'a.
        if stream and (name := stream.get("user_name")) and name != t.display_name:
            t.display_name = name

        if stream and not was_live:
            # offline → live : nouvelle notif.
            t.state["live"] = True
            t.state["offline_cycles"] = 0
            return make_live_event(stream, t.display_name, t.avatar_url)
        if stream and was_live:
            # Toujours live : reset le compteur d'absence.
            t.state["offline_cycles"] = 0
        elif not stream and was_live:
            # Absence : confirmer sur plusieurs cycles avant de reset (micro-coupures).
            cycles = int(t.state.get("offline_cycles", 0)) + 1
            t.state["offline_cycles"] = cycles
            if cycles >= _OFFLINE_CONFIRM_CYCLES:
                t.state["live"] = False
                t.state["offline_cycles"] = 0
        return None

    async def poll(self, target: Target) -> list[dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError("Twitch utilise poll_batch (batching /streams)")
