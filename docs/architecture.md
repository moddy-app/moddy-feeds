# Architecture

## Vue d'ensemble

```
   Bot / Backend Moddy
         │
         │  XADD feeds:commands        (Redis partagé)
         │  { action, platform, identifier, request_id, poll_interval? }
         ▼
┌──────────────────────── moddy-feeds (worker Python) ─────────────────────┐
│                                                                           │
│  Consumer feeds:commands          Connecteurs                             │
│  (subscribe / unsubscribe)   ┌─────────────────────────────────┐          │
│         │                    │ YouTube   → WebSub + polling     │          │
│         ▼                    │ Twitch    → EventSub WS + polling│          │
│  PostgreSQL dédiée           │ Bluesky   → websocket Jetstream  │          │
│  (targets + state)           │ Instagram → scraper tiers (off)  │          │
│         ▲                    │ RSS       → polling conditionnel │          │
│         │                    └───────────────┬─────────────────┘          │
│  Scheduler (tick 2 s)                        │                            │
│         └────────────────────► normalize + dédup (Redis)                  │
└──────────────────────────────────────────────┼────────────────────────────┘
                                               ▼
                            XADD notifications:queue    (Redis partagé)
                                               │
                                               ▼
                                   Bot Discord (consumer group)
```

## Transports et latence

Chaque plateforme a un **chemin rapide** (push) et un **chemin de repli**
(polling). Le polling n'est jamais désactivé : il sert de filet quand le push
n'est pas configuré, tombe, ou rate un événement.

| Service | Chemin rapide | Repli | Latence médiane |
|---|---|---|---|
| YouTube | WebSub (HTTP entrant) | polling feed Atom | ~10 s (push) / ~1-2 min |
| Twitch | EventSub WebSocket (sortant) | polling Helix `/streams` | ~2 s (push) / ~15 s |
| Bluesky | Websocket Jetstream (sortant) | — | ~1 s |
| RSS | — | polling conditionnel (ETag) | ~1,5 min |
| Instagram | — | aucune méthode propre — connecteur off | — |

Les deux chemins produisent le **même `event_id`**, donc la dédup Redis garantit
une notification unique : le transport le plus rapide gagne, l'autre est ignoré.

### Le cas particulier du HTTP entrant

Le service était initialement un worker pur, sans aucun port ouvert. WebSub
YouTube fait exception : c'est le seul gain de latence majeur qui n'a pas
d'équivalent en connexion sortante côté Google, et le feed Atom est le chemin le
plus lent du service (le feed lui-même met 1 à 5 min à refléter une publication).

L'exception est donc **contenue** :

- un seul module (`app/websub.py`), une seule route utile, démarré uniquement si
  `WEBSUB_CALLBACK_URL` **et** `WEBSUB_SECRET` sont fournis ;
- toute notification est vérifiée en HMAC (`X-Hub-Signature`), et un challenge
  d'abonnement n'est confirmé que pour une cible déjà connue en base ;
- sans ces variables, aucun port n'est ouvert et le service se comporte
  exactement comme avant.

Twitch n'a pas eu besoin de cette exception : EventSub existe en transport
websocket **sortant**, le même modèle que Bluesky.

### Auto-cicatrisation du régime push

Une cible couverte par un transport push porte un `state.push_until` (un bail
daté, persisté en base). Tant qu'il est valide, le scheduler ralentit son
polling au plancher `PUSH_FALLBACK_INTERVAL` (900 s). Le bail est court et
renouvelé par le worker push ; s'il cesse de l'être — websocket mort, lease
WebSub non renouvelé, révocation — il expire et **le polling rapide reprend
sans intervention**. Aucun code de bascule explicite, donc aucun état incohérent
possible entre « je crois être en temps réel » et « je le suis ».

## Séparation des responsabilités

- `moddy-feeds` gère des **cibles** : une chaîne YouTube suivie par 200 serveurs
  = **1 cible**. Il ne connaît pas les guilds.
- Le **bot** garde la table `social_subscriptions` (guild ↔ cible) dans la DB
  principale de Moddy et dispatche les événements reçus aux bons serveurs.

## Processus interne (workers asyncio)

`app/main.py` lance en parallèle :

| Worker | Rôle | Fichier |
|---|---|---|
| `commands` | Consume `feeds:commands`, répond sur `feeds:replies` | `app/commands.py` |
| `scheduler` | Tick 2 s : poll les cibles dues (YouTube/RSS/Twitch/IG) | `app/schedulers.py` |
| `bluesky` | Websocket Jetstream temps réel | `app/connectors/bluesky.py` |
| `heartbeat` | `SET feeds:heartbeat` toutes les 30 s | `app/schedulers.py` |

Workers conditionnels (accélérateurs, jamais des dépendances) :

| Worker | Condition | Fichier |
|---|---|---|
| `twitch-eventsub` | `TWITCH_USER_REFRESH_TOKEN` défini | `app/connectors/twitch_eventsub.py` |
| `websub-server` | `WEBSUB_CALLBACK_URL` + `WEBSUB_SECRET` | `app/websub.py` |
| `websub-renewal` | idem | `app/websub.py` |

Chaque worker est résilient : une commande ou un tick qui plante est loggé sans
tuer la boucle. Si une tâche meurt complètement, le process s'arrête et Railway
le redémarre (`restartPolicyType: ON_FAILURE`).

## Boucle de scheduling par tick

Un **tick unique** (2 s) réserve et poll les cibles dues. Trois propriétés
tiennent la latence quand le volume monte.

**1. Réservation atomique.** La sélection et l'estampillage de `last_poll_at`
sont la même requête (`FOR UPDATE SKIP LOCKED`) : plusieurs instances peuvent
drainer la file sans se marcher dessus, et l'intervalle se mesure de *début* à
début de poll — la durée du poll ne s'ajoute plus à la latence.

**2. Drain complet.** Un tick redemande des lots tant qu'il en trouve
(`SCHEDULER_MAX_BATCHES` au maximum). Avec un lot fixe, la capacité serait
plafonnée à `batch_limit / tick` cibles/s ; au-delà, la file ne se vide plus et
l'intervalle réel dérive **en silence**. Le cas est mesuré, pas supposé : le
retard est échantillonné toutes les 60 s et loggé en `warn` au-delà de 30 s.

**3. Intervalle effectif adaptatif** (cf. `db.claim_due_targets`) — l'intervalle
demandé par le bot n'est pas appliqué tel quel :

| Régime | Condition | Effet |
|---|---|---|
| push | `state.push_until` dans le futur | ralenti au plancher 900 s |
| chaud | `last_event_at` < 2 h | accéléré ×3 |
| normal | — | intervalle demandé |

Le résultat est toujours borné par le minimum de la plateforme. Le régime chaud
paie parce que les publications sont corrélées (série de vidéos, redémarrage de
stream après une coupure).

- YouTube/RSS/Instagram → un poll par cible (concurrence `POLL_CONCURRENCY`).
- Twitch → paquets de 100 pour `/streams`, les paquets **en parallèle**.

### Indexation

L'intervalle effectif est une expression, donc non indexable : une sélection
naïve dégénère en Seq Scan + tri sur toute la table (~48 ms pour 50 000 cibles,
toutes les 2 s). La requête ajoute donc une borne indexable — `last_poll_at <=
now() - min_plateforme`, condition *nécessaire* pour être due — servie par
`idx_targets_active_due` (migration 002). Mesuré sur 50 000 cibles : **1,3 ms**.

⚠️ Si l'expression d'intervalle change, l'intervalle effectif doit **rester ≥ au
minimum de la plateforme**, sinon cette borne masquerait des cibles dues.

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
