-- Migration 002 — index de sélection des cibles dues (latence du scheduler).
-- Idempotente.
--
-- L'intervalle effectif d'une cible est une *expression* (défaut plateforme,
-- régime push, régime chaud), donc il n'est pas indexable directement :
-- `idx_targets_due` ne servait qu'au filtre platform/status et laissait un
-- Seq Scan + tri sur toute la table à chaque tick (~48 ms pour 50 000 cibles,
-- toutes les 2 s).
--
-- Cet index partiel donne l'ordre `last_poll_at` directement, sur les seules
-- lignes actives. Combiné à la borne « last_poll_at <= now() - min_plateforme »
-- ajoutée dans la requête (condition *nécessaire* pour être due, puisque
-- l'intervalle effectif est toujours ≥ ce minimum), le plan devient un Index
-- Scan qui s'arrête dès la limite de lot atteinte : ~1,3 ms pour le même volume.
-- Le prédicat doit rester aligné sur celui de la requête (`status <> 'disabled'`),
-- sinon Postgres refuse d'utiliser l'index partiel.
CREATE INDEX IF NOT EXISTS idx_targets_active_due
    ON targets (last_poll_at NULLS FIRST)
    WHERE status <> 'disabled';

-- Renouvellement des leases push (WebSub / EventSub) : petite table balayée
-- toutes les 30 min, mais autant éviter le scan complet.
CREATE INDEX IF NOT EXISTS idx_targets_push_until
    ON targets (platform, ((state ->> 'push_until')::bigint))
    WHERE status = 'active';
