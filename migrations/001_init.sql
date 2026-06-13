-- Migration 001 — schéma initial du service moddy-feeds.
-- Idempotente : exécutable plusieurs fois sans effet de bord (CREATE … IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS targets (
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

-- Index pour la boucle scheduler "cibles dues" (filtre status + platform, tri last_poll_at).
CREATE INDEX IF NOT EXISTS idx_targets_due
    ON targets (platform, status, last_poll_at);
