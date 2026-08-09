"""Twitch EventSub en transport **WebSocket** — notifications live temps réel.

Pourquoi ça existe alors que `twitch.py` poll déjà : le polling `/streams` donne
au mieux une latence de `poll_interval / 2` (~15 s avec les bornes actuelles).
EventSub pousse `stream.online` en 1-3 s.

Pourquoi c'est possible sans exposer d'endpoint : EventSub existe en deux
transports. Le transport *webhook* exigerait que Twitch POSTe chez nous ; le
transport *websocket* est une connexion **sortante**, exactement le même modèle
que le worker Bluesky Jetstream.

Contraintes propres à ce transport :

- La création d'abonnements exige un **user access token** (le token applicatif
  de `twitch.py` ne convient pas), obtenu une fois via OAuth puis entretenu par
  refresh. Le refresh token **tourne** à chaque échange → persisté en Redis.
- **300 abonnements maximum par session.** Au-delà, les cibles excédentaires
  restent sur le polling — d'où la priorisation par activité récente.
- Twitch impose des reconnexions (`session_reconnect`) : à la reconnexion via
  `reconnect_url`, les abonnements sont conservés et ne doivent pas être recréés.

Cohabitation avec le polling : les deux chemins produisent le même `event_id`
(`twitch:{stream_id}`), donc la dédup Redis garantit une seule notification ;
le plus rapide gagne. Tant que le websocket est vivant, il pose un
`state.push_until` qui ralentit le poll des cibles couvertes — et si le
websocket meurt, ce lease expire et le polling rapide reprend tout seul.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets

from app.config import settings
from app.connectors.twitch import make_live_event
from app.core import db, redis as r
from app.core.events import publish_event
from app.core.http import get_http
from app.logging_config import get_logger

log = get_logger(__name__)

_WS_URL = "wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds=30"
_SUBSCRIPTIONS_URL = "https://api.twitch.tv/helix/eventsub/subscriptions"
_STREAMS_URL = "https://api.twitch.tv/helix/streams"
_TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# Plafond imposé par Twitch pour un transport websocket.
_MAX_SUBSCRIPTIONS = 300
_BACKOFF_MAX = 60
# Durée du lease posé sur les cibles couvertes, et fréquence de renouvellement.
# Le lease est volontairement court : si le worker meurt, le polling rapide
# reprend au bout d'une heure au pire, sans intervention.
_LEASE_SECONDS = 3600
_LEASE_REFRESH_EVERY = 900


class TwitchEventSub:
    """Worker long-vivant : une session websocket + N abonnements stream.online."""

    def __init__(self) -> None:
        self._session_id: str | None = None
        self._watched: set[str] = set()   # cibles réellement abonnées côté Twitch
        self._wanted: set[str] = set()    # cibles actives en DB
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()      # réveille le worker quand une cible arrive

    # ─── Token utilisateur (refresh rotatif, persisté) ───────────────────────
    async def _user_token(self) -> str:
        client = r.get_redis()
        if token := await client.get(r.KEY_TWITCH_USER_TOKEN):
            return token

        refresh = await client.get(r.KEY_TWITCH_USER_REFRESH)
        if not refresh:
            # Bootstrap : première utilisation, on part de la variable d'env.
            refresh = settings.twitch_user_refresh_token
        if not refresh:
            raise RuntimeError("aucun refresh token utilisateur disponible")

        http = get_http()
        resp = await http.post(
            _TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh,
                "client_id": settings.twitch_client_id,
                "client_secret": settings.twitch_client_secret,
            },
        )
        if resp.status_code != 200:
            raise RuntimeError(f"refresh du token utilisateur refusé (HTTP {resp.status_code})")

        body = resp.json()
        token = body["access_token"]
        ttl = max(60, int(body.get("expires_in", 3600)) - 300)
        await client.set(r.KEY_TWITCH_USER_TOKEN, token, ex=ttl)
        # Le refresh token tourne : persister le nouveau, sinon l'ancien devient
        # invalide et le service perd l'accès au prochain redémarrage.
        if new_refresh := body.get("refresh_token"):
            await client.set(r.KEY_TWITCH_USER_REFRESH, new_refresh)
        return token

    async def _headers(self) -> dict[str, str]:
        return {
            "Client-Id": settings.twitch_client_id or "",
            "Authorization": f"Bearer {await self._user_token()}",
            "Content-Type": "application/json",
        }

    # ─── Reconfiguration à chaud (hooks abonnement/désabonnement) ────────────
    async def watch(self, target_id: str) -> None:
        """Ajoute une cible à la session en cours (appelé après un `subscribe`)."""
        async with self._lock:
            self._wanted.add(target_id)
            session = self._session_id
            already = target_id in self._watched
        self._wake.set()  # débloque le worker s'il attendait une première cible
        if session and not already and len(self._watched) < _MAX_SUBSCRIPTIONS:
            await self._subscribe_one(target_id, session)

    async def unwatch(self, target_id: str) -> None:
        """Retire une cible (la souscription Twitch expirera d'elle-même)."""
        async with self._lock:
            self._wanted.discard(target_id)
            self._watched.discard(target_id)

    # ─── Worker ──────────────────────────────────────────────────────────────
    async def run(self) -> None:
        """Boucle de connexion avec backoff, reprise et reconnexion dirigée."""
        asyncio.create_task(self._lease_loop(), name="eventsub-lease")
        url = _WS_URL
        resume = False
        backoff = 1

        while True:
            # Twitch ferme toute session sans abonnement au bout de 10 s
            # (`4003 connection unused`). Sans cette garde, un service qui ne
            # suit aucune chaîne Twitch reconnecterait en boucle pour rien.
            if not await self._has_targets():
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=60)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                async with websockets.connect(url, max_size=2**20) as ws:
                    backoff = 1
                    reconnect_url = await self._consume(ws, resume=resume)
                # Reconnexion dirigée par Twitch : les abonnements sont conservés.
                if reconnect_url:
                    url, resume = reconnect_url, True
                    continue
                url, resume = _WS_URL, False
            except (websockets.WebSocketException, OSError) as exc:
                log.warning("eventsub disconnected: %s — backoff %ss", exc, backoff)
                url, resume = _WS_URL, False
            except Exception:  # noqa: BLE001 — le worker ne doit jamais mourir
                log.exception("eventsub crashed")
                url, resume = _WS_URL, False

            async with self._lock:
                self._session_id = None
                self._watched.clear()
            await asyncio.sleep(backoff)
            backoff = min(_BACKOFF_MAX, backoff * 2)

    async def _has_targets(self) -> bool:
        try:
            return await db.count_active_targets("twitch") > 0
        except Exception:  # noqa: BLE001 — DB indisponible : réessayer plus tard
            log.warning("eventsub: comptage des cibles impossible")
            return False

    async def _consume(self, ws: Any, *, resume: bool) -> str | None:
        """Traite les messages d'une session. Retourne une `reconnect_url` le cas échéant."""
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            meta = msg.get("metadata") or {}
            payload = msg.get("payload") or {}
            kind = meta.get("message_type")

            if kind == "session_welcome":
                session_id = (payload.get("session") or {}).get("id")
                async with self._lock:
                    self._session_id = session_id
                log.info("eventsub session %s (resume=%s)", session_id, resume)
                if not resume:
                    # Twitch ferme la session si aucun abonnement n'est créé
                    # dans les 10 s → ne pas bloquer la boucle de réception.
                    asyncio.create_task(self._subscribe_all(session_id), name="eventsub-subs")

            elif kind == "session_keepalive":
                continue

            elif kind == "notification":
                await self._handle_notification(payload)

            elif kind == "session_reconnect":
                new_url = (payload.get("session") or {}).get("reconnect_url")
                log.info("eventsub reconnect requested")
                return new_url

            elif kind == "revocation":
                sub = payload.get("subscription") or {}
                uid = (sub.get("condition") or {}).get("broadcaster_user_id")
                log.warning("eventsub revoked for %s (%s)", uid, sub.get("status"))
                if uid:
                    async with self._lock:
                        self._watched.discard(uid)
                    await db.clear_push_lease("twitch", uid)

    # ─── Abonnements ─────────────────────────────────────────────────────────
    async def _subscribe_all(self, session_id: str | None) -> None:
        """(Re)crée les abonnements pour les cibles actives, les plus chaudes d'abord."""
        if not session_id:
            return
        targets = await db.fetch_active_targets("twitch")
        # Priorité aux cibles qui ont publié récemment : si le plafond de 300 est
        # atteint, autant couvrir en temps réel celles qui streament vraiment.
        ordered = sorted(
            targets,
            key=lambda t: t.last_event_at.timestamp() if t.last_event_at else 0.0,
            reverse=True,
        )

        async with self._lock:
            self._wanted = {t.target_id for t in ordered}

        selected = ordered[:_MAX_SUBSCRIPTIONS]
        if len(ordered) > _MAX_SUBSCRIPTIONS:
            log.warning(
                "eventsub: %d cibles pour %d abonnements max — le reste reste en polling",
                len(ordered),
                _MAX_SUBSCRIPTIONS,
            )

        ok = 0
        for target in selected:
            if await self._subscribe_one(target.target_id, session_id):
                ok += 1
        log.info("eventsub subscribed %d/%d targets", ok, len(selected))

    async def _subscribe_one(self, target_id: str, session_id: str) -> bool:
        http = get_http()
        try:
            resp = await http.post(
                _SUBSCRIPTIONS_URL,
                headers=await self._headers(),
                json={
                    "type": "stream.online",
                    "version": "1",
                    "condition": {"broadcaster_user_id": target_id},
                    "transport": {"method": "websocket", "session_id": session_id},
                },
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("eventsub subscribe failed for %s: %s", target_id, exc)
            return False

        # 409 = déjà abonné sur cette session (reconnexion) → considéré couvert.
        if resp.status_code not in (200, 202, 409):
            log.warning("eventsub subscribe %s → HTTP %s: %s", target_id, resp.status_code, resp.text[:200])
            return False

        async with self._lock:
            self._watched.add(target_id)
        await db.set_push_lease("twitch", target_id, int(time.time()) + _LEASE_SECONDS)
        return True

    # ─── Notifications ───────────────────────────────────────────────────────
    async def _handle_notification(self, payload: dict[str, Any]) -> None:
        sub = payload.get("subscription") or {}
        if sub.get("type") != "stream.online":
            return
        event = payload.get("event") or {}
        user_id = event.get("broadcaster_user_id")
        stream_id = event.get("id")
        if not user_id or not stream_id:
            return

        target = await db.get_target("twitch", user_id)
        if target is None:  # désabonnée entre-temps
            return

        stream = await self._fetch_stream(user_id)
        if stream and stream.get("id") == stream_id:
            # Cas nominal : /streams a déjà le titre, le jeu et la miniature.
            payload_event = make_live_event(stream, target.display_name, target.avatar_url)
        else:
            # /streams pas encore à jour (course de quelques secondes) : publier
            # quand même — mieux vaut une notif immédiate un peu plus pauvre.
            payload_event = make_live_event(
                {
                    "id": stream_id,
                    "user_id": user_id,
                    "user_name": event.get("broadcaster_user_name"),
                    "user_login": event.get("broadcaster_user_login"),
                    "started_at": event.get("started_at"),
                },
                target.display_name,
                target.avatar_url,
            )

        if await publish_event(payload_event):
            # L'état `live` reste piloté par le poller (qui gère aussi la
            # transition inverse) ; on note juste l'événement pour la fenêtre chaude.
            target.state["live"] = True
            target.state["offline_cycles"] = 0
            await db.save_target_state(target, mark_polled=False, had_event=True)
            log.info("eventsub live %s (%s)", user_id, target.display_name)

    async def _fetch_stream(self, user_id: str) -> dict[str, Any] | None:
        """Enrichit la notification via /streams (token applicatif, 1 appel)."""
        from app.connectors import get_connector

        try:
            headers = await get_connector("twitch").app_headers()
            resp = await get_http().get(
                _STREAMS_URL, params={"user_id": user_id}, headers=headers
            )
            if resp.status_code != 200:
                return None
            data = resp.json().get("data") or []
            return data[0] if data else None
        except Exception:  # noqa: BLE001 — l'enrichissement est optionnel
            return None

    # ─── Lease de couverture push ────────────────────────────────────────────
    async def _lease_loop(self) -> None:
        """Prolonge `state.push_until` tant que la session est vivante.

        C'est ce qui dit au scheduler « inutile de poller vite ces cibles ». Le
        lease est court et renouvelé : arrêter de le renouveler suffit à faire
        repartir le polling rapide, sans code de bascule explicite.
        """
        while True:
            await asyncio.sleep(_LEASE_REFRESH_EVERY)
            async with self._lock:
                watched = list(self._watched) if self._session_id else []
            until = int(time.time()) + _LEASE_SECONDS
            for target_id in watched:
                try:
                    await db.set_push_lease("twitch", target_id, until)
                except Exception:  # noqa: BLE001
                    log.warning("eventsub lease refresh failed for %s", target_id)
            if watched:
                log.debug("eventsub lease refreshed for %d targets", len(watched))
