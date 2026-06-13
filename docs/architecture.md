# Architecture

## Vue d'ensemble

```
   Bot / Backend Moddy
         │
         │  XADD feeds:commands        (Redis partagé)
         │  { action, platform, identifier, request_id, poll_interval? }
         ▼
┌──────────────── moddy-feeds (worker Python, pas de HTTP) ────────────────┐
│                                                                           │
│  Consumer feeds:commands          Connecteurs                             │
│  (subscribe / unsubscribe)   ┌────────────────────────────────┐           │
│         │                    │ YouTube   → polling feed Atom   │           │
│         ▼                    │ Twitch    → polling Helix       │           │
│  PostgreSQL dédiée           │ Bluesky   → websocket Jetstream │           │
│  (targets + state)           │ Instagram → scraper tiers (off) │           │
│         ▲                    │ RSS       → polling conditionnel│           │
│         │                    └───────────────┬────────────────┘           │
│  Scheduler (tick 10 s)                       │                            │
│         └────────────────────► normalize + dédup (Redis)                  │
└──────────────────────────────────────────────┼────────────────────────────┘
                                               ▼
                            XADD notifications:queue    (Redis partagé)
                                               │
                                               ▼
                                   Bot Discord (consumer group)
```

## Pourquoi « rien d'exposé »

Pas d'endpoint public ⇒ pas de webhooks entrants possibles ⇒ ni WebSub YouTube
ni EventSub Twitch (qui exigent que Google/Twitch POSTent chez nous). **Tout passe
par du polling ou des websockets sortants.**

| Service | Méthode | Latence typique |
|---|---|---|
| YouTube | Polling feed Atom | ≤ 5 min |
| Twitch | Polling Helix `/streams` (batch 100) | ≤ 1 min |
| Bluesky | Websocket Jetstream (sortant) | temps réel |
| Instagram | Aucune méthode propre — connecteur off | — |
| RSS | Polling conditionnel (ETag/Last-Modified) | ≤ 5 min |

Passer un jour au temps réel YouTube/Twitch = ajouter un mini serveur HTTP avec 2
routes de callback. L'architecture ne change pas, seul le connecteur change.

## Séparation des responsabilités

- `moddy-feeds` gère des **cibles** : une chaîne YouTube suivie par 200 serveurs
  = **1 cible**. Il ne connaît pas les guilds.
- Le **bot** garde la table `social_subscriptions` (guild ↔ cible) dans la DB
  principale de Moddy et dispatche les événements reçus aux bons serveurs.

## Processus interne (workers asyncio)

`app/main.py` lance en parallèle, sans serveur HTTP :

| Worker | Rôle | Fichier |
|---|---|---|
| `commands` | Consume `feeds:commands`, répond sur `feeds:replies` | `app/commands.py` |
| `scheduler` | Tick 10 s : poll les cibles dues (YouTube/RSS/Twitch/IG) | `app/schedulers.py` |
| `bluesky` | Websocket Jetstream temps réel | `app/connectors/bluesky.py` |
| `heartbeat` | `SET feeds:heartbeat` toutes les 30 s | `app/schedulers.py` |

Chaque worker est résilient : une commande ou un tick qui plante est loggé sans
tuer la boucle. Si une tâche meurt complètement, le process s'arrête et Railway
le redémarre (`restartPolicyType: ON_FAILURE`).

## Boucle de scheduling par tick

Plutôt qu'une boucle globale par plateforme, un **tick unique** (10 s) sélectionne
les cibles dues :

```sql
SELECT * FROM targets
WHERE status = 'active'
  AND platform = ANY($1)
  AND (last_poll_at IS NULL
       OR last_poll_at + (COALESCE(poll_interval, default) || ' seconds')::interval <= now())
ORDER BY last_poll_at ASC NULLS FIRST
LIMIT 200
```

- YouTube/RSS/Instagram → un poll par cible (concurrence bornée à 20).
- Twitch → regroupé en paquets de 100 pour `/streams`.

Cela permet un **`poll_interval` configurable par cible** (cf. `integration.md`).

## Couches du code

```
app/
├── main.py            # orchestrateur asyncio
├── config.py          # settings + bornes poll par plateforme
├── logging_config.py  # logs console (dev) / JSON Railway (prod)
├── commands.py        # consumer feeds:commands → feeds:replies
├── schedulers.py      # tick scheduler + heartbeat
├── connectors/        # un module par plateforme (interface commune)
└── core/              # infra partagée
    ├── redis.py       # client + noms de streams/clés
    ├── db.py          # pool asyncpg + accès targets
    ├── events.py      # normalize + dédup + publish
    ├── http.py        # client httpx partagé
    ├── security.py    # garde anti-SSRF
    └── timeutils.py   # parsing dates / filtres anti-vieux
```

## Garanties

- **Déduplication** : `SET notif:seen:{event_id} NX EX 604800` (Redis partagé) →
  aucun doublon même après redémarrage ou avec plusieurs instances.
- **At-least-once** : la queue `notifications:queue` est un stream Redis ; le bot
  l'acquitte via son consumer group. La dédup côté service borne les doublons.
- **Reprise Bluesky** : `cursor` (time_us) sauvegardé toutes les ~10 s → zéro
  événement perdu sur coupure (Jetstream rejoue plusieurs heures).
- **Scalabilité horizontale** : le consumer group `moddy-feeds` permet de lancer
  plusieurs instances du worker commandes sans double traitement (voir
  `operations.md` pour les nuances Bluesky/scheduler).
