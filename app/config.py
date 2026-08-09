"""Configuration centralisée (12-factor) chargée depuis l'environnement.

Tout le réglage du service passe par des variables d'environnement, validées au
démarrage par pydantic-settings. Importer `settings` (singleton) partout ailleurs.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class PollBounds:
    """Bornes de l'intervalle de polling pour une plateforme (en secondes).

    `min`/`max` permettent de clamp silencieusement la valeur demandée par le bot.
    `default` est utilisé quand aucun intervalle n'est fourni.
    `realtime` = la plateforme ne poll pas (websocket) → le paramètre est ignoré.
    """

    min: int
    max: int
    default: int
    realtime: bool = False

    def clamp(self, value: int | None) -> int | None:
        """Ramène `value` dans [min, max]. None → default. Realtime → None."""
        if self.realtime:
            return None
        if value is None:
            return self.default
        return max(self.min, min(self.max, value))


# Bornes par plateforme — source de vérité unique (cf. PROMPT §2).
#
# Les valeurs sont agressives parce que tous les pollers font du *fetch
# conditionnel* (ETag / Last-Modified) : un poll sans nouveauté coûte un 304 de
# quelques centaines d'octets. Les cibles couvertes par un transport push
# (WebSub YouTube, EventSub Twitch) sont automatiquement ralenties par le
# scheduler — cf. `PUSH_FALLBACK_INTERVAL` : le poll n'est plus qu'un filet.
POLL_BOUNDS: dict[str, PollBounds] = {
    "youtube": PollBounds(min=30, max=3600, default=120),
    "twitch": PollBounds(min=10, max=600, default=30),
    "rss": PollBounds(min=60, max=3600, default=180),
    "instagram": PollBounds(min=600, max=86_400, default=1800),
    "bluesky": PollBounds(min=0, max=0, default=0, realtime=True),
}

# Intervalle plancher appliqué à une cible dont le push est actif (`state.push_until`
# dans le futur) : inutile de poller vite ce qui arrive déjà en temps réel.
PUSH_FALLBACK_INTERVAL = 900

# Fenêtre « chaude » : après un événement, une cible est pollée `HOT_FACTOR` fois
# plus vite pendant `HOT_WINDOW_SECONDS`. Les publications sont corrélées (série
# de shorts, redémarrage de stream après une coupure), donc c'est là que la
# réactivité paie le plus. Borné par le `min` de la plateforme.
HOT_WINDOW_SECONDS = 7200
HOT_FACTOR = 3

# Plateformes pollées par le scheduler à chaque tick (Bluesky est temps réel).
POLLED_PLATFORMS: tuple[str, ...] = ("youtube", "rss", "twitch", "instagram")

SUPPORTED_PLATFORMS: frozenset[str] = frozenset(POLL_BOUNDS.keys())


class Settings(BaseSettings):
    """Variables d'environnement du service (cf. PROMPT §11)."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Infrastructure (requis).
    database_url: str = Field(..., alias="DATABASE_URL")
    redis_url: str = Field(..., alias="REDIS_URL")

    # Résolution des handles / API plateformes (optionnels).
    youtube_api_key: str | None = Field(default=None, alias="YOUTUBE_API_KEY")
    twitch_client_id: str | None = Field(default=None, alias="TWITCH_CLIENT_ID")
    twitch_client_secret: str | None = Field(default=None, alias="TWITCH_CLIENT_SECRET")

    # Twitch EventSub WebSocket (temps réel) — nécessite un *user* access token.
    # Absent → le service reste en polling /streams, sans dégradation.
    twitch_user_refresh_token: str | None = Field(
        default=None, alias="TWITCH_USER_REFRESH_TOKEN"
    )
    eventsub_enabled: bool = Field(default=True, alias="EVENTSUB_ENABLED")

    # WebSub YouTube (temps réel) — nécessite une URL publique joignable par Google.
    # Absente → pas de serveur HTTP démarré, YouTube reste en polling.
    websub_callback_url: str | None = Field(default=None, alias="WEBSUB_CALLBACK_URL")
    websub_secret: str | None = Field(default=None, alias="WEBSUB_SECRET")
    websub_port: int = Field(default=8080, alias="PORT")
    websub_lease_seconds: int = Field(default=432_000, alias="WEBSUB_LEASE_SECONDS")

    # Réglages runtime (valeurs par défaut sûres).
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    # "console" (lisible), "json" (Railway), ou None → auto selon TTY.
    log_format: str | None = Field(default=None, alias="LOG_FORMAT")
    scheduler_tick_seconds: int = Field(default=2, alias="SCHEDULER_TICK_SECONDS")
    heartbeat_seconds: int = Field(default=30, alias="HEARTBEAT_SECONDS")
    bluesky_enabled: bool = Field(default=True, alias="BLUESKY_ENABLED")
    instagram_enabled: bool = Field(default=False, alias="INSTAGRAM_ENABLED")

    # Tailles de lot / limites (scalabilité).
    scheduler_batch_limit: int = Field(default=500, alias="SCHEDULER_BATCH_LIMIT")
    # Nb max de lots drainés dans un même tick (garde-fou anti-boucle infinie).
    scheduler_max_batches: int = Field(default=20, alias="SCHEDULER_MAX_BATCHES")
    poll_concurrency: int = Field(default=50, alias="POLL_CONCURRENCY")
    db_pool_max: int = Field(default=20, alias="DB_POOL_MAX")

    @property
    def twitch_configured(self) -> bool:
        return bool(self.twitch_client_id and self.twitch_client_secret)

    @property
    def eventsub_configured(self) -> bool:
        """EventSub WS exige l'app Twitch *et* un refresh token utilisateur."""
        return bool(
            self.eventsub_enabled and self.twitch_configured and self.twitch_user_refresh_token
        )

    @property
    def websub_configured(self) -> bool:
        """WebSub exige une URL de callback publique *et* un secret HMAC."""
        return bool(self.websub_callback_url and self.websub_secret)


# Singleton importable : `from app.config import settings`.
settings = Settings()  # type: ignore[call-arg]
