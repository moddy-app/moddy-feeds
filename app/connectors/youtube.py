"""Connecteur YouTube — polling du feed Atom public (sans clé ni quota).

Résolution @handle → channel_id : API Data v3 si `YOUTUBE_API_KEY` présent
(fiable, 1 unité/abonnement), sinon fallback scraping HTML (gratuit, fragile).
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

import httpx

from app.config import settings
from app.connectors.base import Connector, ResolveError, ResolvedTarget
from app.core.db import Target
from app.core.events import make_event, mark_seen
from app.core.http import get_http
from app.core.timeutils import is_future, too_old
from app.logging_config import get_logger

log = get_logger(__name__)

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
}

_FEED_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
_CHANNEL_ID_RE = re.compile(r'"(?:channelId|externalId)":"(UC[\w-]{22})"')
_CANONICAL_RE = re.compile(r'<link rel="canonical" href="https://www\.youtube\.com/channel/(UC[\w-]{22})"')


class YouTubeConnector(Connector):
    platform = "youtube"

    # ─── Résolution ────────────────────────────────────────────────────────
    async def resolve(self, identifier: str) -> ResolvedTarget:
        ident = identifier.strip()
        channel_id = self._extract_channel_id(ident)
        if channel_id:
            return await self._resolve_known_id(channel_id)
        if settings.youtube_api_key:
            return await self._resolve_via_api(ident)
        return await self._resolve_via_scrape(ident)

    @staticmethod
    def _extract_channel_id(ident: str) -> str | None:
        """Extrait un UC… si l'identifiant est déjà un channel_id ou une URL /channel/."""
        m = re.search(r"(UC[\w-]{22})", ident)
        return m.group(1) if m else None

    async def _resolve_known_id(self, channel_id: str) -> ResolvedTarget:
        """Cas channel_id direct : on récupère le nom via le feed Atom (gratuit)."""
        http = get_http()
        try:
            resp = await http.get(_FEED_URL.format(cid=channel_id), follow_redirects=True)
        except httpx.HTTPError as exc:
            raise ResolveError("fetch_failed", str(exc)) from exc
        if resp.status_code != 200:
            raise ResolveError("channel_not_found", f"HTTP {resp.status_code}")
        root = ET.fromstring(resp.content)
        name = root.findtext("atom:title", default=channel_id, namespaces=NS)
        return ResolvedTarget(target_id=channel_id, display_name=name)

    async def _resolve_via_api(self, ident: str) -> ResolvedTarget:
        handle = ident.lstrip("@")
        http = get_http()
        try:
            resp = await http.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params={
                    "part": "id,snippet",
                    "forHandle": f"@{handle}",
                    "key": settings.youtube_api_key,
                },
                follow_redirects=True,
            )
        except httpx.HTTPError as exc:
            raise ResolveError("fetch_failed", str(exc)) from exc
        data = resp.json()
        items = data.get("items") or []
        if not items:
            raise ResolveError("channel_not_found")
        item = items[0]
        snippet = item.get("snippet", {})
        thumbs = snippet.get("thumbnails", {})
        avatar = (thumbs.get("high") or thumbs.get("default") or {}).get("url")
        return ResolvedTarget(
            target_id=item["id"],
            display_name=snippet.get("title"),
            avatar_url=avatar,
        )

    async def _resolve_via_scrape(self, ident: str) -> ResolvedTarget:
        handle = ident if ident.startswith("@") else f"@{ident}"
        http = get_http()
        try:
            resp = await http.get(f"https://www.youtube.com/{handle}", follow_redirects=True)
        except httpx.HTTPError as exc:
            raise ResolveError("fetch_failed", str(exc)) from exc
        html = resp.text
        m = _CANONICAL_RE.search(html) or _CHANNEL_ID_RE.search(html)
        if not m:
            raise ResolveError("channel_not_found", "channelId introuvable dans le HTML")
        return await self._resolve_known_id(m.group(1))

    # ─── Polling ─────────────────────────────────────────────────────────────
    async def prime(self, target: Target) -> None:
        await self._run(target, prime=True)

    async def poll(self, target: Target) -> list[dict[str, Any]]:
        return await self._run(target, prime=False)

    async def _run(self, target: Target, *, prime: bool) -> list[dict[str, Any]]:
        http = get_http()
        headers: dict[str, str] = {}
        if etag := target.state.get("etag"):
            headers["If-None-Match"] = etag

        resp = await http.get(
            _FEED_URL.format(cid=target.target_id), headers=headers, follow_redirects=True
        )
        if resp.status_code == 304:
            return []
        if resp.status_code != 200:
            raise ResolveError("bad_status", f"HTTP {resp.status_code}")

        target.state["etag"] = resp.headers.get("ETag")

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as exc:
            raise ResolveError("parse_error", str(exc)) from exc

        # La chaîne peut être renommée : rafraîchir le nom depuis le feed.
        feed_author = root.findtext("atom:author/atom:name", namespaces=NS)
        if feed_author and feed_author != target.display_name:
            target.display_name = feed_author

        events: list[dict[str, Any]] = []
        for entry in root.findall("atom:entry", NS):
            video_id = entry.findtext("yt:videoId", namespaces=NS)
            if not video_id:
                continue
            title = entry.findtext("atom:title", namespaces=NS)
            published = entry.findtext("atom:published", namespaces=NS)
            eid = f"youtube:{video_id}"

            if prime or not target.state.get("initialized"):
                await mark_seen(eid)
                continue
            # Filtre re-publications et lives programmés (date future).
            if too_old(published, hours=24) or is_future(published):
                continue

            events.append(
                make_event(
                    event_id=eid,
                    platform="youtube",
                    type="video",
                    target_id=target.target_id,
                    author_name=target.display_name,
                    author_avatar=target.avatar_url,
                    title=title,
                    url=f"https://youtube.com/watch?v={video_id}",
                    thumbnail=f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
                    published_at=published,
                )
            )

        target.state["initialized"] = True
        return events
