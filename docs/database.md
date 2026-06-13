# Base de données

`moddy-feeds` possède sa **propre** base PostgreSQL (variable `DATABASE_URL`),
distincte de la DB principale de Moddy. Elle ne stocke que les **cibles** et leur
**état interne** — jamais de données de guilds (qui restent côté bot).

## Schéma

```sql
CREATE TABLE targets (
    platform      TEXT NOT NULL,
    target_id     TEXT NOT NULL,          -- forme canonique (channel_id, did, user_id, URL…)
    display_name  TEXT,
    avatar_url    TEXT,
    status        TEXT NOT NULL DEFAULT 'active',  -- active | failing | disabled
    fail_count    INT  NOT NULL DEFAULT 0,
    poll_interval INT,                     -- secondes ; NULL = défaut plateforme
    last_poll_at  TIMESTAMPTZ,
    state         JSONB NOT NULL DEFAULT '{}',     -- etag RSS, dernier item, live en cours…
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_at TIMESTAMPTZ,
    PRIMARY KEY (platform, target_id)
);

CREATE INDEX idx_targets_due ON targets (platform, status, last_poll_at);
```

## Sémantique des champs

| Champ | Rôle |
|---|---|
| `(platform, target_id)` | clé primaire — identité canonique de la cible |
| `display_name` / `avatar_url` | métadonnées résolues à l'abonnement |
| `status` | `active` (pollée), `failing` (échecs transitoires), `disabled` (abandonnée après ~50 échecs) |
| `fail_count` | compteur d'échecs consécutifs ; remis à 0 au premier succès |
| `poll_interval` | intervalle retenu (le **minimum** demandé par les guilds) ; `NULL` = défaut plateforme |
| `last_poll_at` | horodatage du dernier poll — pilote la sélection « cible due » |
| `state` | JSONB libre par connecteur : `etag`, `last_modified`, `initialized`, `live`, `offline_cycles`… |
| `last_event_at` | dernier événement publié — utile pour ralentir les flux inactifs |

## État `state` par plateforme

| Plateforme | Clés `state` |
|---|---|
| youtube | `etag`, `initialized` |
| rss | `etag`, `last_modified`, `initialized` |
| twitch | `live` (bool), `offline_cycles` (int) |
| bluesky | (état en mémoire + cursor Redis ; `state` peu utilisé) |

## Migrations

Les migrations sont des fichiers SQL **idempotents** dans `migrations/`,
appliqués automatiquement au démarrage (`app/core/db.py::_run_migrations`). Le
suivi se fait via la table `schema_migrations`.

- Convention de nommage : `NNN_description.sql` (ordre alphabétique).
- Chaque fichier doit utiliser `CREATE … IF NOT EXISTS` / `ALTER … IF NOT EXISTS`
  pour rester ré-exécutable.
- Pour ajouter une migration : créer `migrations/002_xxx.sql` ; elle sera
  appliquée au prochain boot et enregistrée dans `schema_migrations`.

## Déduplication (hors PostgreSQL)

La dédup des événements **n'est pas en base** : elle vit dans le Redis partagé
(`SET notif:seen:{event_id} NX EX 604800`, fenêtre 7 jours). Cela la rend
partagée entre instances et résistante aux redémarrages.

## Accès

Toutes les requêtes passent par `app/core/db.py` (pool asyncpg). Les connecteurs
et schedulers n'écrivent jamais de SQL en dur — ils utilisent les helpers
(`upsert_target`, `fetch_due_targets`, `save_target_state`, `register_failure`…).
