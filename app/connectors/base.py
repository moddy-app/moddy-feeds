"""Interface commune des connecteurs plateformes.

Cycle de vie d'un connecteur :
  resolve(identifier) → ResolvedTarget   (à l'abonnement, forme canonique + métas)
  prime(target)                          (premier poll : marquer le contenu existant vu)
  poll(target) → list[event]             (pollers : YouTube, RSS, Instagram)

Twitch et Bluesky ont des cycles spécifiques (batch / websocket) et surchargent
ce qu'il faut. La classe de base fournit des no-ops sûrs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from app.core.db import Target

# Durée de vie d'une métadonnée coûteuse (avatar, profil) avant rafraîchissement.
META_REFRESH_TTL = 86_400  # 24 h


def due_for_meta_refresh(state: dict[str, Any], ttl: int = META_REFRESH_TTL) -> bool:
    """True si les métadonnées coûteuses méritent un rafraîchissement (throttle)."""
    return (time.time() - float(state.get("meta_at", 0))) > ttl


def stamp_meta_refresh(state: dict[str, Any]) -> None:
    """Marque l'instant du dernier rafraîchissement de métadonnées."""
    state["meta_at"] = int(time.time())


class ResolveError(Exception):
    """Échec de résolution d'un identifiant (code = error renvoyé au bot)."""

    def __init__(self, code: str, message: str | None = None):
        self.code = code
        super().__init__(message or code)


@dataclass
class ResolvedTarget:
    """Résultat de la résolution d'un identifiant vers sa forme canonique."""

    target_id: str
    display_name: str | None = None
    avatar_url: str | None = None
    initial_state: dict[str, Any] | None = None


class Connector:
    """Classe de base — un connecteur par plateforme."""

    platform: str = "base"
    #: La plateforme est-elle temps réel (websocket) plutôt que pollée ?
    realtime: bool = False

    async def resolve(self, identifier: str) -> ResolvedTarget:
        """Transforme un identifiant libre en cible canonique. Lève ResolveError."""
        raise NotImplementedError

    async def prime(self, target: Target) -> None:
        """Premier poll : marquer le contenu existant comme vu (pas de notif initiale).

        Par défaut, délègue à `poll` en mode amorçage si le connecteur le gère ;
        sinon no-op. Surcharger pour un amorçage spécifique.
        """
        return None

    async def poll(self, target: Target) -> list[dict[str, Any]]:
        """Poll une cible et retourne les événements normalisés à publier."""
        raise NotImplementedError

    async def on_subscribe(self, target: Target) -> None:
        """Hook post-abonnement (ex: Bluesky met à jour wantedDids)."""
        return None

    async def on_unsubscribe(self, target_id: str) -> None:
        """Hook post-désabonnement (ex: Bluesky retire le DID)."""
        return None
