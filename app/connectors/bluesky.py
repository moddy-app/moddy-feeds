"""Connecteur Bluesky — websocket Jetstream (connexion sortante, temps réel).

Un worker long-vivant maintient une connexion Jetstream filtrée sur les DIDs
suivis (`wantedDids`). Les abonnements/désabonnements mettent à jour la liste à
chaud via `options_update` (sans reconnecter). Reprise sur coupure via `cursor`.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx
import websockets

from app.connectors.base import (
    META_REFRESH_TTL,
    Connector,
    ResolveError,
    ResolvedTarget,
)
from app.core import redis as r
from app.core.db import Target, fetch_active_targets, update_target_meta_stamped
from app.core.events import make_event, publish_event
from app.core.http import get_http
from app.logging_config import get_logger

log = get_logger(__name__)

_JETSTREAM_HOSTS = [
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-west.bsky.network",
    "jetstream1.us-west.bsky.network",
]
_COLLECTION = "app.bsky.feed.post"
_RESOLVE_URL = "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle"
_PROFILE_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"

_MAX_DIDS = 10_000
_BACKOFF_MAX = 60
_CURSOR_SAVE_EVERY = 10.0  # secondes

# Rafraîchissement intelligent des profils : borné et throttlé.
_META_REFRESH_EVERY = 3600.0   # cycle horaire
_META_BATCH = 50               # profils max rafraîchis par cycle (anti-burst API)


class BlueskyConnector(Connector):
    platform = "bluesky"
    realtime = True

    def __init__(self) -> None:
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._dids: set[str] = set()
        self._names: dict[str, str] = {}
        self._avatars: dict[str, str] = {}
        self._meta_at: dict[str, float] = {}  # DID → dernier refresh profil (epoch)
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()  # réveille le worker pour reconfigurer

    # ─── Résolution handle → DID + profil ────────────────────────────────────
    async def resolve(self, identifier: str) -> ResolvedTarget:
        handle = identifier.strip().lstrip("@")
        if handle.startswith("did:"):
            did = handle
        else:
            http = get_http()
            try:
                resp = await http.get(_RESOLVE_URL, params={"handle": handle}, follow_redirects=True)
            except httpx.HTTPError as exc:
                raise ResolveError("fetch_failed", str(exc)) from exc
            if resp.status_code != 200:
                raise ResolveError("handle_not_found", f"HTTP {resp.status_code}")
            did = resp.json().get("did")
            if not did:
                raise ResolveError("handle_not_found")

        name, avatar = await self._fetch_profile(did)
        return ResolvedTarget(target_id=did, display_name=name, avatar_url=avatar)

    async def _fetch_profile(self, did: str) -> tuple[str | None, str | None]:
        http = get_http()
        try:
            resp = await http.get(_PROFILE_URL, params={"actor": did}, follow_redirects=True)
            if resp.status_code == 200:
                p = resp.json()
                return (p.get("displayName") or p.get("handle"), p.get("avatar"))
        except httpx.HTTPError:
            pass
        return (None, None)

    # ─── Mise à jour à chaud des comptes suivis ──────────────────────────────
    async def on_subscribe(self, target: Target) -> None:
        async with self._lock:
            self._dids.add(target.target_id)
            if target.display_name:
                self._names[target.target_id] = target.display_name
            if target.avatar_url:
                self._avatars[target.target_id] = target.avatar_url
            self._meta_at[target.target_id] = time.time()  # profil frais à l'abonnement
        await self._push_options()

    async def on_unsubscribe(self, target_id: str) -> None:
        async with self._lock:
            self._dids.discard(target_id)
            self._names.pop(target_id, None)
            self._avatars.pop(target_id, None)
            self._meta_at.pop(target_id, None)
        await self._push_options()

    async def _push_options(self) -> None:
        """Envoie `options_update` si la connexion est ouverte ; réveille sinon."""
        self._wake.set()
        ws = self._ws
        if ws is None:
            return
        async with self._lock:
            dids = list(self._dids)[:_MAX_DIDS]
        try:
            await ws.send(
                json.dumps(
                    {
                        "type": "options_update",
                        "payload": {
                            "wantedCollections": [_COLLECTION],
                            "wantedDids": dids,
                        },
                    }
                )
            )
            log.info("bluesky options_update → %d DIDs", len(dids))
        except websockets.WebSocketException as exc:
            log.warning("bluesky options_update failed: %s", exc)

    # ─── Worker long-vivant ──────────────────────────────────────────────────
    async def run(self) -> None:
        """Boucle de connexion Jetstream avec backoff exponentiel et reprise cursor."""
        await self._load_targets()
        # Refresh profils en tâche de fond (throttlé, borné, timers persistés).
        asyncio.create_task(self._refresh_profiles_loop(), name="bluesky-meta")
        backoff = 1
        host_idx = 0

        while True:
            if not self._dids:
                # Rien à suivre : attendre un abonnement plutôt que spinner.
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
                continue

            host = _JETSTREAM_HOSTS[host_idx % len(_JETSTREAM_HOSTS)]
            url = await self._build_url(host)
            try:
                async with websockets.connect(url, max_size=2**20, ping_interval=20) as ws:
                    self._ws = ws
                    backoff = 1
                    log.info("bluesky connected to %s (%d DIDs)", host, len(self._dids))
                    await self._consume(ws)
            except (websockets.WebSocketException, OSError) as exc:
                log.warning("bluesky disconnected: %s — backoff %ss", exc, backoff)
            finally:
                self._ws = None

            host_idx += 1
            await asyncio.sleep(backoff)
            backoff = min(_BACKOFF_MAX, backoff * 2)

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        last_save = asyncio.get_event_loop().time()
        client = r.get_redis()

        while True:
            # Reconfiguration demandée → sortir pour reconstruire l'URL/options.
            if self._wake.is_set():
                self._wake.clear()
                await self._push_options()

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                continue

            msg = json.loads(raw)
            time_us = msg.get("time_us")
            now = asyncio.get_event_loop().time()
            if time_us and now - last_save >= _CURSOR_SAVE_EVERY:
                await client.set(r.KEY_BLUESKY_CURSOR, str(time_us))
                last_save = now

            await self._handle_message(msg)

    async def _handle_message(self, msg: dict[str, Any]) -> None:
        c = msg.get("commit") or {}
        if (
            msg.get("kind") != "commit"
            or c.get("operation") != "create"
            or c.get("collection") != _COLLECTION
        ):
            return
        rec = c.get("record") or {}
        if "reply" in rec:  # ignorer les réponses
            return

        did = msg.get("did")
        rkey = c.get("rkey")
        if not did or not rkey:
            return

        await publish_event(
            make_event(
                event_id=f"bluesky:{did}:{rkey}",
                platform="bluesky",
                type="post",
                target_id=did,
                author_name=self._names.get(did, did),
                author_avatar=self._avatars.get(did),
                content=(rec.get("text") or "")[:300],
                url=f"https://bsky.app/profile/{did}/post/{rkey}",
                thumbnail=self._extract_image(did, rec),
                published_at=rec.get("createdAt"),
            )
        )

    @staticmethod
    def _extract_image(did: str, rec: dict[str, Any]) -> str | None:
        embed = rec.get("embed") or {}
        if embed.get("$type") != "app.bsky.embed.images":
            return None
        images = embed.get("images") or []
        if not images:
            return None
        blob = (images[0].get("image") or {}).get("ref", {})
        cid = blob.get("$link") if isinstance(blob, dict) else None
        if not cid:
            return None
        return f"https://cdn.bsky.app/img/feed_fullsize/plain/{did}/{cid}@jpeg"

    async def _build_url(self, host: str) -> str:
        async with self._lock:
            dids = list(self._dids)[:_MAX_DIDS]
        params = [f"wantedCollections={_COLLECTION}"]
        params += [f"wantedDids={d}" for d in dids]
        cursor = await r.get_redis().get(r.KEY_BLUESKY_CURSOR)
        if cursor:
            params.append(f"cursor={cursor}")
        return f"wss://{host}/subscribe?" + "&".join(params)

    async def _load_targets(self) -> None:
        """Hydrate l'état mémoire depuis la DB au démarrage.

        Hydrate aussi le throttle de refresh (`state.meta_at`) pour que le service
        ne re-fetche pas tous les profils d'un coup après un redémarrage.
        """
        targets = await fetch_active_targets("bluesky")
        async with self._lock:
            for t in targets:
                self._dids.add(t.target_id)
                if t.display_name:
                    self._names[t.target_id] = t.display_name
                if t.avatar_url:
                    self._avatars[t.target_id] = t.avatar_url
                self._meta_at[t.target_id] = float(t.state.get("meta_at", 0))
        log.info("bluesky loaded %d DIDs from DB", len(self._dids))

    async def _refresh_profiles_loop(self) -> None:
        """Rafraîchit intelligemment les profils : horaire, borné, et persisté.

        Chaque cycle ne touche qu'au plus `_META_BATCH` profils périmés (> 24 h),
        les plus anciens d'abord. Le timestamp est écrit en DB → résistant au restart.
        """
        while True:
            await asyncio.sleep(_META_REFRESH_EVERY)
            now = time.time()
            async with self._lock:
                stale = [d for d in self._dids if now - self._meta_at.get(d, 0) > META_REFRESH_TTL]
            stale.sort(key=lambda d: self._meta_at.get(d, 0))
            for did in stale[:_META_BATCH]:
                name, avatar = await self._fetch_profile(did)
                ts = int(time.time())
                self._meta_at[did] = ts
                if name:
                    self._names[did] = name
                if avatar:
                    self._avatars[did] = avatar
                try:
                    await update_target_meta_stamped("bluesky", did, name, avatar, ts)
                except Exception:  # noqa: BLE001
                    log.warning("bluesky meta persist failed for %s", did)
            if stale:
                log.info("bluesky refreshed %d/%d stale profiles", min(len(stale), _META_BATCH), len(stale))
