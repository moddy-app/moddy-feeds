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


# ─── Intervalle effectif d'une cible (expression SQL partagée) ─────────────
#
# L'intervalle demandé par le bot n'est pas appliqué tel quel : il est modulé
# par trois régimes, puis borné par le minimum de la plateforme.
#
#   $2 = défauts plateforme (jsonb)    $3 = minimums plateforme (jsonb)
#   $4 = plancher si push actif        $5 = fenêtre chaude (s)   $6 = facteur chaud
#
#  • push actif (`state.push_until` dans le futur) → on RALENTIT au plancher
#    `$4` : WebSub/EventSub apportent déjà l'événement en quelques secondes, le
#    poll n'est plus qu'un filet de rattrapage.
#  • cible chaude (événement récent) → on ACCÉLÈRE d'un facteur `$6`.
#  • cible en échec → on ESPACE proportionnellement à `fail_count` (×2, ×3… ×10),
#    ce qui remplace l'ancienne exclusion pure des cibles `failing`.
#
# Le régime push l'emporte sur le régime chaud (GREATEST appliqué en premier),
# sinon une cible push-ée récemment active serait pollée inutilement vite.
_EFFECTIVE_INTERVAL_SQL = """
GREATEST(
    (($3::jsonb) ->> t.platform)::int,
    (CASE
        WHEN COALESCE((t.state ->> 'push_until')::bigint, 0) > EXTRACT(EPOCH FROM now())
            THEN GREATEST(COALESCE(t.poll_interval, (($2::jsonb) ->> t.platform)::int), $4::int)
        ELSE COALESCE(t.poll_interval, (($2::jsonb) ->> t.platform)::int)
     END)
    / (CASE
        WHEN t.last_event_at > now() - make_interval(secs => $5::int) THEN $6::int
        ELSE 1
       END)
) * (1 + LEAST(t.fail_count, 9))
"""

# Borne indexable ($7 = plus petit minimum parmi les plateformes demandées).
#
# `_EFFECTIVE_INTERVAL_SQL` étant encadré par un GREATEST sur le minimum
# plateforme, une cible pollée il y a moins que ce minimum ne peut PAS être due :
# la condition est donc *nécessaire*, et elle porte sur une colonne nue, ce qui
# permet à `idx_targets_active_due` de borner le parcours au lieu de laisser
# évaluer l'expression sur toute la table (cf. migration 002).
# Invariant à préserver si l'expression change : l'intervalle effectif doit
# rester ≥ au minimum de la plateforme, sinon des cibles seraient ignorées.
#
# `status <> 'disabled'` et non `= 'active'` : `register_failure` bascule une
# cible en `failing` dès le premier échec, et seul un poll réussi la ramène à
# `active`. Filtrer sur `active` seul la rendait donc **définitivement muette**
# après une simple erreur réseau. Elle reste sélectionnée, simplement espacée
# par le facteur `fail_count` ci-dessus, jusqu'à `disabled` (échecs répétés).
# Le prédicat doit rester identique à celui de `idx_targets_active_due`, sinon
# Postgres ne peut plus utiliser l'index partiel.
_IS_DUE_SQL = f"""
t.status <> 'disabled'
AND t.platform = ANY($1::text[])
AND (t.last_poll_at IS NULL OR t.last_poll_at <= now() - make_interval(secs => $7::int))
AND (
    t.last_poll_at IS NULL
    OR t.last_poll_at + make_interval(secs => {_EFFECTIVE_INTERVAL_SQL}) <= now()
)
"""


async def claim_due_targets(
    platforms: list[str],
    default_intervals: dict[str, int],
    min_intervals: dict[str, int],
    *,
    push_fallback: int,
    hot_window: int,
    hot_factor: int,
    limit: int,
) -> list[Target]:
    """Réserve et retourne les cibles dues (cf. PROMPT §2, régime adaptatif).

    « Réserve » : `last_poll_at` est estampillé **au moment de la sélection**, dans
    la même requête, avec `FOR UPDATE SKIP LOCKED`. Deux conséquences :

    - plusieurs instances du service peuvent drainer la même file sans se marcher
      dessus (chaque ligne n'est servie qu'une fois) ;
    - l'intervalle se mesure de *début* de poll à début de poll, et non de fin à
      début — la durée du poll ne s'ajoute plus à la latence perçue.

    Corollaire : les appelants persistent ensuite l'état avec `mark_polled=False`,
    la réservation ayant déjà fait le travail.
    """
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            f"""
            WITH due AS (
                SELECT t.platform, t.target_id
                FROM targets t
                WHERE {_IS_DUE_SQL}
                ORDER BY t.last_poll_at ASC NULLS FIRST
                LIMIT $8
                FOR UPDATE SKIP LOCKED
            )
            UPDATE targets tg
               SET last_poll_at = now()
              FROM due
             WHERE tg.platform = due.platform AND tg.target_id = due.target_id
            RETURNING tg.*
            """,
            platforms,
            json.dumps(default_intervals),
            json.dumps(min_intervals),
            push_fallback,
            hot_window,
            hot_factor,
            min(min_intervals.values(), default=1),
            limit,
        )
    return [Target(r) for r in rows]


async def scheduler_backlog(
    platforms: list[str],
    default_intervals: dict[str, int],
    min_intervals: dict[str, int],
    *,
    push_fallback: int,
    hot_window: int,
    hot_factor: int,
) -> tuple[int, float]:
    """Retard du scheduler : (nb de cibles dues, plus vieux retard en secondes).

    Sans cette mesure, un service saturé continue de tourner « normalement » : la
    file de cibles dues ne se vide plus, l'intervalle effectif dérive et la
    latence augmente en silence. C'est la métrique à surveiller en prod.
    """
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT count(*) AS due_count,
                   COALESCE(EXTRACT(EPOCH FROM (
                       now() - min(t.last_poll_at
                                   + make_interval(secs => {_EFFECTIVE_INTERVAL_SQL}))
                   )), 0) AS max_overdue
            FROM targets t
            WHERE {_IS_DUE_SQL}
            """,
            platforms,
            json.dumps(default_intervals),
            json.dumps(min_intervals),
            push_fallback,
            hot_window,
            hot_factor,
            min(min_intervals.values(), default=1),
        )
    return int(row["due_count"]), float(row["max_overdue"] or 0.0)


async def count_active_targets(platform: str) -> int:
    """Nombre de cibles actives d'une plateforme (sans charger les lignes)."""
    async with get_pool().acquire() as conn:
        return int(
            await conn.fetchval(
                "SELECT count(*) FROM targets WHERE status = 'active' AND platform = $1",
                platform,
            )
        )


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

    `push_until` fait exception et est **repris depuis la base**, jamais depuis
    l'objet en mémoire : il appartient aux workers push (WebSub/EventSub), qui
    peuvent l'écrire pendant qu'un poll est en vol. Sans ça, un poll lent
    écraserait un lease tout juste obtenu et la cible repasserait en poll rapide.
    """
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE targets SET
                state = $3::jsonb || (
                    CASE WHEN state ? 'push_until'
                         THEN jsonb_build_object('push_until', state -> 'push_until')
                         ELSE '{}'::jsonb END
                ),
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


# ─── Transports push (WebSub / EventSub) ───────────────────────────────────
async def set_push_lease(platform: str, target_id: str, until_epoch: int) -> None:
    """Note jusqu'à quand une cible est couverte par un transport temps réel.

    Persisté dans `state.push_until` (invariant : un timer ne vit jamais
    seulement en mémoire). Le scheduler s'en sert pour ralentir le poll de cette
    cible, et la garantie est *auto-cicatrisante* : si le websocket meurt ou que
    le lease WebSub n'est pas renouvelé, la valeur expire et le polling rapide
    reprend tout seul.
    """
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE targets
               SET state = jsonb_set(state, '{push_until}', to_jsonb($3::bigint), true)
             WHERE platform = $1 AND target_id = $2
            """,
            platform,
            target_id,
            until_epoch,
        )


async def clear_push_lease(platform: str, target_id: str) -> None:
    """Retire la couverture push d'une cible (révocation, désabonnement du hub)."""
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE targets SET state = state - 'push_until' WHERE platform = $1 AND target_id = $2",
            platform,
            target_id,
        )


async def fetch_push_renewals(platform: str, before_epoch: int, limit: int) -> list[Target]:
    """Cibles actives dont le lease push expire avant `before_epoch` (à renouveler)."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM targets
            WHERE status = 'active'
              AND platform = $1
              AND COALESCE((state ->> 'push_until')::bigint, 0) < $2
            ORDER BY COALESCE((state ->> 'push_until')::bigint, 0) ASC
            LIMIT $3
            """,
            platform,
            before_epoch,
            limit,
        )
    return [Target(r) for r in rows]


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
