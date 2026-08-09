"""WebSub (PubSubHubbub) — réception push des nouvelles vidéos YouTube.

**Ce module est le seul endroit du service qui expose du HTTP entrant**, et il
ne démarre que si `WEBSUB_CALLBACK_URL` et `WEBSUB_SECRET` sont fournis. Sans
eux, YouTube reste intégralement en polling et le service tourne exactement
comme avant (aucun port ouvert).

Pourquoi accepter cette exception au design « worker pur » : le feed Atom
YouTube est le seul chemin de polling dont la latence se compte en minutes
(le feed lui-même met 1 à 5 min à refléter une publication, avant même de
compter l'intervalle de poll). WebSub la ramène à quelques secondes — c'est le
plus gros gain de latence disponible sur ce service, et il n'y a pas
d'équivalent sortant côté Google.

Protocole, côté sécurité (le point qui compte, l'URL étant publique) :

1. `POST` au hub pour s'abonner, en fournissant `hub.secret`.
2. Le hub rappelle en `GET` avec `hub.challenge` → on ne le renvoie que si le
   `hub.topic` correspond à une cible **connue en base**.
3. Les notifications arrivent en `POST`, signées `X-Hub-Signature`. Toute
   requête non signée ou mal signée est rejetée : c'est ce qui empêche
   n'importe qui de fabriquer de fausses notifications sur une URL publique.

Le lease accordé par le hub expire (5 jours max en pratique) : il est stocké
dans `state.push_until` et renouvelé par `run_renewal_worker`. Tant qu'il est
valide, le scheduler ralentit le poll de la cible ; s'il expire sans
renouvellement, le polling rapide reprend automatiquement.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time

from aiohttp import web

from app.config import settings
from app.core import db
from app.core.events import publish_events
from app.core.http import get_http
from app.logging_config import get_logger

log = get_logger(__name__)

_HUB_URL = "https://pubsubhubbub.appspot.com/subscribe"
_TOPIC_URL = "https://www.youtube.com/xml/feeds/videos.xml?channel_id={cid}"

# Taille max acceptée pour une notification (un feed Atom d'une entrée est minuscule).
_MAX_BODY_BYTES = 1_048_576

# Renouvellement : on repasse bien avant l'expiration pour absorber un hub lent.
_RENEW_MARGIN = 43_200        # 12 h
_RENEW_EVERY = 1800           # cycle de vérification
_RENEW_BATCH = 100


def _topic(channel_id: str) -> str:
    return _TOPIC_URL.format(cid=channel_id)


def _channel_id_from_topic(topic: str) -> str | None:
    """Extrait le channel_id d'une URL de topic (forme imposée par YouTube)."""
    marker = "channel_id="
    if marker not in topic:
        return None
    return topic.split(marker, 1)[1].split("&", 1)[0] or None


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """Valide l'en-tête `X-Hub-Signature: <algo>=<hexdigest>` en temps constant.

    Le hub Google signe en SHA-1 ; les autres algorithmes sont acceptés au cas où
    il évoluerait. Une signature absente ou d'algorithme inconnu est un rejet.
    """
    if not header or "=" not in header:
        return False
    algo, _, digest = header.partition("=")
    try:
        hasher = getattr(hashlib, algo.lower())
    except AttributeError:
        return False
    expected = hmac.new(secret.encode(), body, hasher).hexdigest()
    return hmac.compare_digest(expected, digest.strip())


# ─── Demandes d'abonnement au hub ──────────────────────────────────────────
async def request_subscription(channel_id: str, *, mode: str = "subscribe") -> bool:
    """Envoie une demande (dés)abonnement au hub. La confirmation arrive en GET."""
    if not settings.websub_configured:
        return False
    try:
        resp = await get_http().post(
            _HUB_URL,
            data={
                "hub.callback": settings.websub_callback_url,
                "hub.topic": _topic(channel_id),
                "hub.mode": mode,
                "hub.verify": "async",
                "hub.secret": settings.websub_secret,
                "hub.lease_seconds": str(settings.websub_lease_seconds),
            },
        )
    except Exception as exc:  # noqa: BLE001 — le polling reste le filet
        log.warning("websub %s failed for %s: %s", mode, channel_id, exc)
        return False

    if resp.status_code not in (202, 204):
        log.warning("websub %s %s → HTTP %s", mode, channel_id, resp.status_code)
        return False
    log.info("websub %s requested for %s", mode, channel_id)
    return True


async def request_unsubscription(channel_id: str) -> bool:
    await db.clear_push_lease("youtube", channel_id)
    return await request_subscription(channel_id, mode="unsubscribe")


# ─── Serveur HTTP (callback du hub) ────────────────────────────────────────
async def _handle_verification(request: web.Request) -> web.Response:
    """GET : confirmation d'abonnement — ne renvoyer le challenge que si la cible existe."""
    params = request.query
    mode = params.get("hub.mode")
    topic = params.get("hub.topic", "")
    challenge = params.get("hub.challenge")
    if not challenge:
        return web.Response(status=400, text="missing challenge")

    channel_id = _channel_id_from_topic(topic)
    if not channel_id:
        return web.Response(status=404, text="unknown topic")

    target = await db.get_target("youtube", channel_id)
    if target is None:
        # Cible inconnue (ou déjà désabonnée) : refuser l'abonnement.
        log.info("websub verification refused for unknown channel %s", channel_id)
        return web.Response(status=404, text="unknown topic")

    if mode == "subscribe":
        lease = int(params.get("hub.lease_seconds") or settings.websub_lease_seconds)
        await db.set_push_lease("youtube", channel_id, int(time.time()) + lease)
        log.info("websub subscription confirmed for %s (lease %ss)", channel_id, lease)
    elif mode == "unsubscribe":
        await db.clear_push_lease("youtube", channel_id)
        log.info("websub unsubscription confirmed for %s", channel_id)

    return web.Response(status=200, text=challenge)


async def _handle_notification(request: web.Request) -> web.Response:
    """POST : nouveau contenu poussé par le hub (signé HMAC)."""
    body = await request.content.read(_MAX_BODY_BYTES + 1)
    if len(body) > _MAX_BODY_BYTES:
        return web.Response(status=413, text="payload too large")

    secret = settings.websub_secret or ""
    if not verify_signature(secret, body, request.headers.get("X-Hub-Signature")):
        log.warning("websub notification rejected: bad signature")
        return web.Response(status=403, text="bad signature")

    # Répondre vite au hub : le traitement se fait hors du cycle requête/réponse.
    asyncio.create_task(_process_push(body), name="websub-push")
    return web.Response(status=204)


async def _process_push(body: bytes) -> None:
    """Transforme le feed poussé en événements (même chemin que le poll)."""
    from app.connectors.youtube import parse_feed

    try:
        channel_id = _extract_channel_id(body)
        if not channel_id:
            return
        target = await db.get_target("youtube", channel_id)
        if target is None:
            return
        # Une cible non amorcée n'a pas encore de dédup : laisser le poll faire
        # le premier passage plutôt que de notifier tout l'historique poussé.
        if not target.state.get("initialized"):
            return

        events = await parse_feed(body, target)
        published = await publish_events(events)
        await db.save_target_state(target, mark_polled=False, had_event=published > 0)
        if published:
            log.info("websub push → %d event(s) for %s", published, channel_id)
    except Exception:  # noqa: BLE001 — une notif malformée ne tue pas le serveur
        log.exception("websub push processing failed")


def _extract_channel_id(body: bytes) -> str | None:
    """Lit le `yt:channelId` du feed poussé (sans faire confiance au topic reçu)."""
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }
    for path in ("atom:entry/yt:channelId", "yt:channelId"):
        if value := root.findtext(path, namespaces=ns):
            return value
    return None


async def _handle_health(_: web.Request) -> web.Response:
    return web.Response(status=200, text="ok")


def build_app() -> web.Application:
    app = web.Application(client_max_size=_MAX_BODY_BYTES)
    app.router.add_get("/websub/youtube", _handle_verification)
    app.router.add_post("/websub/youtube", _handle_notification)
    app.router.add_get("/health", _handle_health)
    return app


async def run_server() -> None:
    """Sert le callback WebSub jusqu'à annulation de la tâche."""
    runner = web.AppRunner(build_app(), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.websub_port)
    await site.start()
    log.info("websub server listening on :%d (callback=%s)", settings.websub_port, settings.websub_callback_url)
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


# ─── Renouvellement des leases ─────────────────────────────────────────────
async def run_renewal_worker() -> None:
    """(Ré)abonne les chaînes dont le lease est absent ou proche de l'expiration.

    Couvre aussi le démarrage à froid : au premier passage, toutes les chaînes
    existantes n'ont pas de `push_until` et sont donc (ré)abonnées.
    """
    while True:
        try:
            deadline = int(time.time()) + _RENEW_MARGIN
            targets = await db.fetch_push_renewals("youtube", deadline, _RENEW_BATCH)
            for target in targets:
                await request_subscription(target.target_id)
                await asyncio.sleep(0.1)  # ne pas marteler le hub
            if targets:
                log.info("websub renewal: %d channel(s) (re)subscribed", len(targets))
        except Exception:  # noqa: BLE001 — le worker ne doit jamais mourir
            log.exception("websub renewal cycle failed")
        await asyncio.sleep(_RENEW_EVERY)
