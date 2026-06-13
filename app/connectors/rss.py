"""Connecteur RSS générique — polling conditionnel (ETag / Last-Modified).

Sécurité : toute URL est validée anti-SSRF à l'abonnement ET avant chaque fetch.
Les redirections sont suivies manuellement (max 3), chacune revalidée, et la
réponse est bornée à ~2 Mo.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import feedparser
import httpx

from app.connectors.base import Connector, ResolveError, ResolvedTarget
from app.core import security
from app.core.db import Target
from app.core.events import make_event, mark_seen
from app.core.http import get_http
from app.logging_config import get_logger

log = get_logger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    return _TAG_RE.sub("", text or "").strip()


def _event_id(feed_url: str, guid: str) -> str:
    digest = hashlib.sha256(f"{feed_url}{guid}".encode()).hexdigest()[:24]
    return f"rss:{digest}"


async def _safe_get(url: str, headers: dict[str, str]) -> httpx.Response:
    """GET avec validation anti-SSRF, redirections manuelles bornées et taille max."""
    http = get_http()
    current = url
    for _ in range(security.MAX_REDIRECTS + 1):
        await security.assert_url_is_safe(current)
        resp = await http.get(current, headers=headers)
        if resp.is_redirect and (loc := resp.headers.get("location")):
            current = str(httpx.URL(current).join(loc))
            continue
        # Garde-fou taille (en plus du streaming idéal : httpx a déjà lu le corps).
        if len(resp.content) > security.MAX_RESPONSE_BYTES:
            raise ResolveError("feed_too_large", "réponse > 2 Mo")
        return resp
    raise ResolveError("too_many_redirects")


class RSSConnector(Connector):
    platform = "rss"

    async def resolve(self, identifier: str) -> ResolvedTarget:
        """Valide qu'un flux est fetchable et parsable avec ≥ 1 entrée."""
        url = identifier.strip()
        try:
            await security.assert_url_is_safe(url)
        except security.SSRFError as exc:
            raise ResolveError("unsafe_url", str(exc)) from exc

        try:
            resp = await _safe_get(url, headers={})
        except (httpx.HTTPError, security.SSRFError) as exc:
            raise ResolveError("fetch_failed", str(exc)) from exc

        if resp.status_code != 200:
            raise ResolveError("bad_status", f"HTTP {resp.status_code}")

        parsed = feedparser.parse(resp.content)
        if not parsed.entries:
            raise ResolveError("no_entries", "flux vide ou non parsable")

        title = parsed.feed.get("title") or url
        return ResolvedTarget(
            target_id=url,
            display_name=title,
            avatar_url=parsed.feed.get("image", {}).get("href")
            if isinstance(parsed.feed.get("image"), dict)
            else None,
            initial_state={
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
            },
        )

    async def prime(self, target: Target) -> None:
        """Premier passage : marquer toutes les entrées vues sans publier."""
        await self._run(target, prime=True)

    async def poll(self, target: Target) -> list[dict[str, Any]]:
        return await self._run(target, prime=False)

    async def _run(self, target: Target, *, prime: bool) -> list[dict[str, Any]]:
        headers: dict[str, str] = {}
        if etag := target.state.get("etag"):
            headers["If-None-Match"] = etag
        if lm := target.state.get("last_modified"):
            headers["If-Modified-Since"] = lm

        try:
            resp = await _safe_get(target.target_id, headers)
        except (httpx.HTTPError, security.SSRFError, ResolveError) as exc:
            log.warning("rss fetch failed %s: %s", target.target_id, exc)
            raise

        if resp.status_code == 304:
            return []
        if resp.status_code != 200:
            raise ResolveError("bad_status", f"HTTP {resp.status_code}")

        target.state["etag"] = resp.headers.get("ETag")
        target.state["last_modified"] = resp.headers.get("Last-Modified")

        parsed = feedparser.parse(resp.content)
        feed_title = parsed.feed.get("title", target.display_name or target.target_id)

        events: list[dict[str, Any]] = []
        for entry in parsed.entries[:20]:
            guid = entry.get("id") or entry.get("link") or ""
            if not guid:
                continue
            eid = _event_id(target.target_id, guid)
            if prime or not target.state.get("initialized"):
                await mark_seen(eid)
                continue
            events.append(
                make_event(
                    event_id=eid,
                    platform="rss",
                    type="article",
                    target_id=target.target_id,
                    author_name=feed_title,
                    title=entry.get("title"),
                    content=strip_html(entry.get("summary", ""))[:300],
                    url=entry.get("link"),
                    published_at=entry.get("published") or entry.get("updated"),
                )
            )

        target.state["initialized"] = True
        return events
