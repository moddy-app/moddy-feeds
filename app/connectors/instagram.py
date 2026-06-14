"""Connecteur Instagram — désactivé par défaut (cf. PROMPT §8).

Il n'existe aucune API officielle pour suivre un compte arbitraire. Ce connecteur
est un squelette branchable sur un scraper tiers payant (Option A : Apify, etc.)
le jour où la demande le justifie. Tant que `INSTAGRAM_ENABLED=false`, il n'est pas
enregistré dans `available_platforms()` et toute commande renvoie `not_supported`.

Structure identique à un connecteur poller (resolve + poll) — il suffira de câbler
l'appel HTTP au scraper choisi et la transformation JSON → événement normalisé.
"""

from __future__ import annotations

from typing import Any

from app.connectors.base import Connector, ResolveError, ResolvedTarget
from app.core.db import Target


class InstagramConnector(Connector):
    platform = "instagram"

    async def resolve(self, identifier: str) -> ResolvedTarget:
        raise ResolveError(
            "not_supported",
            "Instagram n'est pas activé (aucune API officielle, voir docs/connectors.md).",
        )

    async def poll(self, target: Target) -> list[dict[str, Any]]:  # pragma: no cover
        # TODO(option-A): appeler le scraper (Apify run-sync), mapper vers make_event.
        return []
