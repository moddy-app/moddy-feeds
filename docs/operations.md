# Exploitation (Railway)

## Déploiement

Le service tourne sur **Railway** comme worker. Il n'écoute sur aucun port, sauf
si le callback WebSub YouTube est configuré (cf. § « Temps réel » ci-dessous).

- `Dockerfile` : image `python:3.11-slim`, utilisateur non-root.
- `railway.json` : builder Dockerfile, `startCommand: python -m app.main`,
  `restartPolicyType: ON_FAILURE` (10 retries).

Provisionner sur Railway :
1. Un service **PostgreSQL** dédié → fournit `DATABASE_URL`.
2. Le **Redis partagé** avec le bot → `REDIS_URL`.
3. Déployer ce repo ; les migrations s'appliquent automatiquement au boot.

## Variables d'environnement

| Variable | Requis | Rôle |
|---|---|---|
| `DATABASE_URL` | ✅ | PostgreSQL dédiée (targets + state) |
| `REDIS_URL` | ✅ | Redis **partagé** avec le bot (commandes + queue) |
| `YOUTUBE_API_KEY` | ⬜ | Résolution @handle fiable (sinon scraping HTML) |
| `TWITCH_CLIENT_ID` | ⬜* | App Twitch (*requis pour Twitch) |
| `TWITCH_CLIENT_SECRET` | ⬜* | App Twitch (*requis pour Twitch) |
| `LOG_LEVEL` | ⬜ | `INFO` par défaut |
| `LOG_FORMAT` | ⬜ | `console` \| `json` (auto : `json` hors TTY / Railway) |
| `TWITCH_USER_REFRESH_TOKEN` | ⬜ | active EventSub WebSocket (live en ~2 s) |
| `EVENTSUB_ENABLED` | ⬜ | `true` par défaut (coupe-circuit) |
| `WEBSUB_CALLBACK_URL` | ⬜ | active le push YouTube — **ouvre un port HTTP** |
| `WEBSUB_SECRET` | ⬜ | secret HMAC du callback (requis avec l'URL) |
| `WEBSUB_LEASE_SECONDS` | ⬜ | durée demandée au hub (déf. 432000) |
| `PORT` | ⬜ | port d'écoute du callback (déf. 8080, fourni par Railway) |
| `SCHEDULER_TICK_SECONDS` | ⬜ | période du tick (déf. 2) |
| `HEARTBEAT_SECONDS` | ⬜ | période heartbeat (déf. 30) |
| `BLUESKY_ENABLED` | ⬜ | `true` par défaut |
| `INSTAGRAM_ENABLED` | ⬜ | `false` par défaut |
| `SCHEDULER_BATCH_LIMIT` | ⬜ | taille d'un lot réservé (déf. 500) |
| `SCHEDULER_MAX_BATCHES` | ⬜ | lots max drainés par tick (déf. 20) |
| `POLL_CONCURRENCY` | ⬜ | polls HTTP simultanés (déf. 50) |
| `DB_POOL_MAX` | ⬜ | taille max du pool asyncpg (déf. 20) |

## Temps réel (optionnel)

Les deux transports push sont des **accélérateurs** : sans eux le service
fonctionne, en polling. Les activer ne change ni le contrat Redis ni le format
des événements.

**Twitch EventSub** — poser `TWITCH_USER_REFRESH_TOKEN` (procédure OAuth dans
`.env.example`). Vérifier au boot : `twitch eventsub enabled (websocket
transport)` puis `eventsub subscribed N/M targets`. Le refresh token tourne : le
nouveau est stocké dans Redis (`feeds:twitch:user_refresh`) et prime sur la
variable d'environnement.

**WebSub YouTube** — activer « Generate Domain » sur le service Railway, puis
poser `WEBSUB_CALLBACK_URL=https://<domaine>/websub/youtube` et
`WEBSUB_SECRET=$(openssl rand -hex 32)`. C'est la seule fonctionnalité qui ouvre
un port. Vérifier : `websub server listening on :8080` puis, au premier cycle de
renouvellement, `websub renewal: N channel(s) (re)subscribed`. Une route
`/health` répond `ok` (utilisable en `healthcheckPath` Railway **uniquement** si
WebSub est activé — sinon rien n'écoute et le healthcheck échouerait).

Pour revenir en arrière : retirer les variables et redéployer. Les baux
`state.push_until` expirent d'eux-mêmes et le polling rapide reprend.

## Surveiller la latence

La métrique à regarder est le **retard du scheduler**, échantillonné toutes les
60 s. Au-delà de 30 s de retard, un `warn` est émis :

```
scheduler behind: 1240 targets due, oldest overdue by 95s
```

Ça veut dire que la file de cibles dues ne se vide plus : l'intervalle réel n'est
plus celui annoncé aux utilisateurs. Dans l'ordre : augmenter `POLL_CONCURRENCY`,
puis `SCHEDULER_BATCH_LIMIT`, puis `DB_POOL_MAX` ; si le retard persiste, lancer
une seconde instance (la réservation `FOR UPDATE SKIP LOCKED` permet à plusieurs
instances de drainer la même file sans doublon).

## Logs

Les logs sont conçus pour être **lisibles à l'œil en local** et **exploitables par
Railway en prod** :

- **`LOG_FORMAT=console`** (auto en terminal) : ligne colorée et alignée
  `HH:MM:SS  LEVEL  module  message`.
- **`LOG_FORMAT=json`** (auto hors TTY, donc sur Railway) : une ligne JSON par log
  avec un champ **`level`** que Railway reconnaît pour **colorer et filtrer par
  sévérité**. Mapping :

  | Python | `level` Railway |
  |---|---|
  | DEBUG | `debug` |
  | INFO | `info` |
  | WARNING | `warn` |
  | ERROR / CRITICAL | `error` |

  Exemple : `{"level":"info","logger":"app.commands","message":"subscribed youtube:UC… (poll=120, new=True)","time":"…"}`

`PYTHONUNBUFFERED=1` (Dockerfile) garantit que les logs sortent immédiatement.
Pour filtrer dans Railway : utiliser le sélecteur de niveau (`error`, `warn`…).

### Conventions de niveaux

- `INFO` : cycle de vie normal (abonnements, événements publiés, connexions).
- `WARNING` (`warn`) : dégradation récupérable (échec réseau ponctuel, reconnexion
  Bluesky, `/streams` non-200).
- `ERROR` : exception inattendue (crash d'une commande, d'un tick) — toujours
  accompagnée de la stacktrace dans le champ `error`.

## Healthcheck / monitoring

- Le worker `heartbeat` écrit `feeds:heartbeat` toutes les 30 s (TTL ~90 s). Le
  backend surveille la présence de cette clé (`EXISTS feeds:heartbeat`).
- Railway redémarre automatiquement le process sur crash (`ON_FAILURE`).
- Indicateurs utiles à grapher côté backend :
  - longueur de `notifications:queue` (`XLEN`) — alerte si le bot ne consomme pas.
  - longueur du backlog `feeds:commands` du group `moddy-feeds` (`XPENDING`).

## Scalabilité

| Aspect | Capacité |
|---|---|
| Twitch | `/streams` accepte 100 user_id/req, rate limit ≈ 800 req/min → ~80 000 streamers/min |
| YouTube/RSS | poll conditionnel (304) très léger ; concurrence `POLL_CONCURRENCY` (déf. 50) |
| Bluesky | 10 000 DIDs/connexion ; au-delà, ouvrir plusieurs connexions (évolution) |
| Twitch EventSub | **300 abonnements/session** ; au-delà, priorisation par activité + polling |
| DB | `idx_targets_active_due` → sélection des cibles dues en ~1,3 ms pour 50 000 cibles |

### Lancer plusieurs instances

- **Worker commandes** : le consumer group `moddy-feeds` garantit qu'une commande
  n'est traitée qu'une fois → scaling horizontal sûr.
- **Scheduler** : la réservation des cibles se fait en `FOR UPDATE SKIP LOCKED`
  dans la requête de sélection → deux instances ne peuvent pas servir la même
  cible. Scaling horizontal sûr, et c'est le levier à utiliser si le retard du
  scheduler ne se résorbe pas.
- **Bluesky** : le worker maintient un état websocket en mémoire ; garder **une
  seule instance** Bluesky (ou sharder les DIDs par instance — évolution).
- **EventSub / WebSub** : garder **une seule instance** de ces workers. Plusieurs
  instances EventSub ouvriraient des sessions concurrentes (abonnements
  dupliqués côté Twitch), et plusieurs callbacks WebSub se disputeraient le même
  lease. La dédup empêcherait les doublons d'événements, mais c'est du gaspillage.

> Recommandation v1 : **une seule instance** du service suffit largement aux
> besoins (des dizaines de milliers de cibles). Scaler les commandes en premier
> si nécessaire.

## Robustesse intégrée

- Cibles en échec : `failing` après le 1ᵉʳ échec, `disabled` après ~50 échecs
  consécutifs (RSS injoignable, chaîne supprimée…). Une cible `failing` continue
  d'être pollée, simplement **espacée** proportionnellement à `fail_count`
  (×2, ×3… plafonné à ×10) : une erreur réseau isolée ne doit pas rendre une
  cible muette.
- Anti-faux-positif Twitch : 3 cycles offline avant de clore un live.
- Reprise Bluesky sans perte via `cursor`.
- Backoffs exponentiels sur reconnexion Bluesky et erreurs Redis.
