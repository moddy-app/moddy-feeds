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
POLL_BOUNDS: dict[str, PollBounds] = {
    "youtube": PollBounds(min=60, max=3600, default=300),
    "twitch": PollBounds(min=30, max=600, default=60),
    "rss": PollBounds(min=120, max=3600, default=300),
    "instagram": PollBounds(min=600, max=86_400, default=1800),
    "bluesky": PollBounds(min=0, max=0, default=0, realtime=True),
}

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

    # Réglages runtime (valeurs par défaut sûres).
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    # "console" (lisible), "json" (Railway), ou None → auto selon TTY.
    log_format: str | None = Field(default=None, alias="LOG_FORMAT")
    scheduler_tick_seconds: int = Field(default=10, alias="SCHEDULER_TICK_SECONDS")
    heartbeat_seconds: int = Field(default=30, alias="HEARTBEAT_SECONDS")
    bluesky_enabled: bool = Field(default=True, alias="BLUESKY_ENABLED")
    instagram_enabled: bool = Field(default=False, alias="INSTAGRAM_ENABLED")

    # Tailles de lot / limites (scalabilité).
    scheduler_batch_limit: int = Field(default=200, alias="SCHEDULER_BATCH_LIMIT")
    db_pool_max: int = Field(default=10, alias="DB_POOL_MAX")

    @property
    def twitch_configured(self) -> bool:
        return bool(self.twitch_client_id and self.twitch_client_secret)


# Singleton importable : `from app.config import settings`.
settings = Settings()  # type: ignore[call-arg]
