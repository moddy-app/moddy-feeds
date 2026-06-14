"""Garde anti-SSRF — LE point de sécurité du service (cf. PROMPT §9).

Les URLs RSS proviennent des utilisateurs. Avant tout fetch, on résout le DNS et
on refuse toute IP privée / loopback / link-local / réservée / metadata cloud.
On borne aussi les redirections et la taille de réponse côté connecteur.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

MAX_REDIRECTS = 3
MAX_RESPONSE_BYTES = 2 * 1024 * 1024  # ~2 Mo

# Adresses notoirement sensibles (metadata cloud) en plus des plages privées.
_BLOCKED_HOSTS = {"metadata.google.internal"}


class SSRFError(ValueError):
    """Levée quand une URL cible une ressource interdite."""


def _is_blocked_ip(ip: ipaddress._BaseAddress) -> bool:
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
        # 169.254.169.254 (AWS/GCP/Azure metadata) est link_local → déjà couvert,
        # mais on garde une ceinture de sécurité explicite.
        or str(ip) == "169.254.169.254"
    )


async def assert_url_is_safe(url: str) -> None:
    """Valide qu'une URL est publique et fetchable. Lève SSRFError sinon.

    À appeler à l'abonnement ET avant chaque fetch (DNS rebinding) côté connecteur.
    """
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise SSRFError(f"scheme interdit: {parsed.scheme!r}")

    host = parsed.hostname
    if not host:
        raise SSRFError("hôte manquant")

    if host.lower() in _BLOCKED_HOSTS:
        raise SSRFError(f"hôte bloqué: {host}")

    # Résolution DNS (toutes les adresses) hors boucle pour ne pas bloquer l'event loop.
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, parsed.port or 0, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise SSRFError(f"résolution DNS impossible: {host}") from exc

    for *_, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if _is_blocked_ip(ip):
            raise SSRFError(f"IP non publique refusée: {ip} ({host})")
