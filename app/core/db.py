"""Accès PostgreSQL dédié (pool asyncpg) + couche d'accès aux `targets`.

La DB stocke les cibles canoniques et leur état interne. Le pool est partagé par
tout le process. Les helpers ici encapsulent les requêtes pour que les
connecteurs/schedulers n'écrivent jamais de SQL en dur.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import asyncpg

from app.config import settings
from app.logging_config import get_logger

log = get_logger(__name__)

_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"

_pool: asyncpg.Pool | None = None


async def init_db() -> asyncpg.Pool:
    """Crée le pool et applique les migrations idempotentes."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=settings.db_pool_max,
            command_timeout=30,
        )
        await _run_migrations(_pool)
    return _pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_db() first")
    return _pool


async def close_db() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def _run_migrations(pool: asyncpg.Pool) -> None:
    """Applique les fichiers .sql de migrations/ par ordre alphabétique.

    Chaque fichier doit être idempotent (CREATE TABLE IF NOT EXISTS …). Le suivi
    se fait via une table `schema_migrations`.
    """
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())"
        )
        applied = {
            r["name"]
            for r in await conn.fetch("SELECT name FROM schema_migrations")
        }
        for sql_file in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            if sql_file.name in applied:
                continue
            log.info("Applying migration %s", sql_file.name)
            async with conn.transaction():
                await conn.execute(sql_file.read_text(encoding="utf-8"))
                await conn.execute(
                    "INSERT INTO schema_migrations (name) VALUES ($1)",
                    sql_file.name,
                )


# ─── Représentation d'une cible ────────────────────────────────────────────
class Target:
    """Vue typée d'une ligne de `targets` (état mutable en mémoire pendant un poll)."""

    __slots__ = (
        "platform",
        "target_id",
        "display_name",
        "avatar_url",
        "status",
        "fail_count",
        "poll_interval",
        "last_poll_at",
        "state",
        "last_event_at",
    )

    def __init__(self, record: asyncpg.Record):
        self.platform: str = record["platform"]
        self.target_id: str = record["target_id"]
        self.display_name: str | None = record["display_name"]
        self.avatar_url: str | None = record["avatar_url"]
        self.status: str = record["status"]
        self.fail_count: int = record["fail_count"]
        self.poll_interval: int | None = record["poll_interval"]
        self.last_poll_at = record["last_poll_at"]
        self.state: dict[str, Any] = (
            json.loads(record["state"]) if isinstance(record["state"], str) else dict(record["state"])
        )
        self.last_event_at = record["last_event_at"]


# ─── Opérations CRUD sur les cibles ────────────────────────────────────────
async def upsert_target(
    *,
    platform: str,
    target_id: str,
    display_name: str | None,
    avatar_url: str | None,
    poll_interval: int | None,
    initial_state: dict[str, Any] | None = None,
) -> bool:
    """Crée la cible si absente, sinon abaisse `poll_interval` au minimum.

    Retourne True si la cible vient d'être créée (→ premier poll à marquer vu).
    Une cible partagée garde l'intervalle le plus exigeant (le minimum non-NULL).
    """
    state_json = json.dumps(initial_state or {})
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO targets (platform, target_id, display_name, avatar_url,
                                 poll_interval, state)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            ON CONFLICT (platform, target_id) DO UPDATE SET
                display_name  = COALESCE(EXCLUDED.display_name, targets.display_name),
                avatar_url    = COALESCE(EXCLUDED.avatar_url, targets.avatar_url),
                poll_interval = LEAST(
                    COALESCE(EXCLUDED.poll_interval, targets.poll_interval),
                    COALESCE(targets.poll_interval, EXCLUDED.poll_interval)
                ),
                status        = CASE WHEN targets.status = 'disabled'
                                     THEN 'active' ELSE targets.status END
            RETURNING (xmax = 0) AS inserted
            """,
            platform,
            target_id,
            display_name,
            avatar_url,
            poll_interval,
            state_json,
        )
    return bool(row["inserted"])


async def set_poll_interval(platform: str, target_id: str, poll_interval: int | None) -> None:
    """Recalcule l'intervalle après un unsubscribe (valeur restante la plus exigeante)."""
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE targets SET poll_interval = $3 WHERE platform = $1 AND target_id = $2",
            platform,
            target_id,
            poll_interval,
        )


async def update_target_meta(
    platform: str, target_id: str, display_name: str | None, avatar_url: str | None
) -> None:
    """Met à jour les métadonnées (nom/avatar) sans écraser avec des NULL."""
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE targets SET
                display_name = COALESCE($3, display_name),
                avatar_url   = COALESCE($4, avatar_url)
            WHERE platform = $1 AND target_id = $2
            """,
            platform,
            target_id,
            display_name,
            avatar_url,
        )


async def update_target_meta_stamped(
    platform: str,
    target_id: str,
    display_name: str | None,
    avatar_url: str | None,
    meta_at: int,
) -> None:
    """Met à jour nom/avatar ET horodate le refresh dans `state.meta_at`.

    Le timestamp est persisté pour que le throttle de rafraîchissement survive à
    un redémarrage (sinon tous les profils seraient re-fetchés au boot).
    """
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE targets SET
                display_name = COALESCE($3, display_name),
                avatar_url   = COALESCE($4, avatar_url),
                state = jsonb_set(state, '{meta_at}', to_jsonb($5::bigint), true)
            WHERE platform = $1 AND target_id = $2
            """,
            platform,
            target_id,
            display_name,
            avatar_url,
            meta_at,
        )


async def delete_target(platform: str, target_id: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            "DELETE FROM targets WHERE platform = $1 AND target_id = $2",
            platform,
            target_id,
        )


async def get_target(platform: str, target_id: str) -> Target | None:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM targets WHERE platform = $1 AND target_id = $2",
            platform,
            target_id,
        )
    return Target(row) if row else None


async def fetch_due_targets(platforms: list[str], default_intervals: dict[str, int], limit: int) -> list[Target]:
    """Cibles actives dont l'intervalle est écoulé (boucle par tick, cf. PROMPT §2).

    `default_intervals` fournit le défaut plateforme quand `poll_interval` est NULL.
    """
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM targets
            WHERE status = 'active'
              AND platform = ANY($1::text[])
              AND (
                last_poll_at IS NULL
                OR last_poll_at
                   + (COALESCE(poll_interval, ($2::jsonb ->> platform)::int) || ' seconds')::interval
                   <= now()
              )
            ORDER BY last_poll_at ASC NULLS FIRST
            LIMIT $3
            """,
            platforms,
            json.dumps(default_intervals),
            limit,
        )
    return [Target(r) for r in rows]


async def fetch_active_targets(platform: str) -> list[Target]:
    """Toutes les cibles actives d'une plateforme (utilisé par Bluesky/Twitch)."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM targets WHERE status = 'active' AND platform = $1",
            platform,
        )
    return [Target(r) for r in rows]


async def save_target_state(target: Target, *, mark_polled: bool = True, had_event: bool = False) -> None:
    """Persiste l'état muté d'une cible après un poll.

    Persiste aussi `display_name`/`avatar_url` depuis l'objet en mémoire : un
    connecteur peut les mettre à jour pendant un poll (les comptes/chaînes
    renomment et changent d'avatar). COALESCE évite d'écraser avec un NULL.
    """
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE targets SET
                state = $3::jsonb,
                display_name = COALESCE($6, display_name),
                avatar_url   = COALESCE($7, avatar_url),
                last_poll_at  = CASE WHEN $4 THEN now() ELSE last_poll_at END,
                last_event_at = CASE WHEN $5 THEN now() ELSE last_event_at END,
                fail_count = 0,
                status = CASE WHEN status = 'failing' THEN 'active' ELSE status END
            WHERE platform = $1 AND target_id = $2
            """,
            target.platform,
            target.target_id,
            json.dumps(target.state),
            mark_polled,
            had_event,
            target.display_name,
            target.avatar_url,
        )


async def register_failure(target: Target, *, disable_after: int = 50) -> None:
    """Incrémente le compteur d'échecs ; passe en `disabled` au-delà du seuil."""
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE targets SET
                fail_count = fail_count + 1,
                last_poll_at = now(),
                status = CASE
                    WHEN fail_count + 1 >= $3 THEN 'disabled'
                    ELSE 'failing' END
            WHERE platform = $1 AND target_id = $2
            """,
            target.platform,
            target.target_id,
            disable_after,
        )
