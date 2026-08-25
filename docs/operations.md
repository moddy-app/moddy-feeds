# Exploitation (Railway)

## Déploiement

Le service tourne sur **Railway** comme worker (aucun port exposé).

- `Dockerfile` : image `python:3.11-slim`, utilisateur non-root, pas d'`EXPOSE`.
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
| `SCHEDULER_TICK_SECONDS` | ⬜ | période du tick (déf. 10) |
| `HEARTBEAT_SECONDS` | ⬜ | période heartbeat (déf. 30) |
| `BLUESKY_ENABLED` | ⬜ | `true` par défaut |
| `INSTAGRAM_ENABLED` | ⬜ | `false` par défaut |
| `SCHEDULER_BATCH_LIMIT` | ⬜ | nb max de cibles par tick (déf. 200) |
| `DB_POOL_MAX` | ⬜ | taille max du pool asyncpg (déf. 10) |
| `HM_URL` | ⬜ | Moddy Health Monitor (sans slash final) — absent ⇒ heartbeat désactivé |
| `HM_INGEST_TOKEN` | ⬜* | token d'ingestion (*requis avec `HM_URL`) |
| `BETTERSTACK_HEARTBEAT_URL` | ⬜ | URL secrète du heartbeat Better Stack — absente ⇒ heartbeat désactivé |
| `BETTERSTACK_HEARTBEAT_INTERVAL_SECONDS` | ⬜ | période du ping (déf. 180 = 3 min, doit matcher la config Better Stack) |

Il n'y a **aucun secret HTTP** (domaine, HMAC, token interne) : rien n'est exposé.

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
- Si `HM_URL`/`HM_INGEST_TOKEN` sont renseignés, un heartbeat sortant est aussi
  poussé toutes les 20 s vers le **Moddy Health Monitor**
  (`POST $HM_URL/ingest/heartbeat`, cf. `app/core/health_monitor.py`). Fire-and-
  forget : un échec ne fait que logger un warning, jamais planter le service.
  Statut calculé par `build_health_checks()` (`app/schedulers.py`) :
  - `postgres`/`redis` injoignables → `down`.
  - scheduler figé (aucun tick réussi depuis 3×`SCHEDULER_TICK_SECONDS`) → `degraded`.
  - sinon → `ok`.
- Si `BETTERSTACK_HEARTBEAT_URL` est renseignée, un heartbeat Better Stack
  (cron/heartbeat monitor) est aussi pingé toutes les
  `BETTERSTACK_HEARTBEAT_INTERVAL_SECONDS` (déf. 180 s = 3 min, cf.
  `app/core/betterstack_heartbeat.py`) :
  - statut `ok` (même calcul que ci-dessus) → `GET $BETTERSTACK_HEARTBEAT_URL`.
  - sinon (`degraded`/`down`) → `GET $BETTERSTACK_HEARTBEAT_URL/fail`, pour
    déclencher l'incident sans attendre le délai de grâce.
  - Fire-and-forget : un échec ne fait que logger un warning.
  - La fréquence et la grace period doivent être configurées côté Better Stack
    pour matcher `BETTERSTACK_HEARTBEAT_INTERVAL_SECONDS`.
- Railway redémarre automatiquement le process sur crash (`ON_FAILURE`).
- Indicateurs utiles à grapher côté backend :
  - longueur de `notifications:queue` (`XLEN`) — alerte si le bot ne consomme pas.
  - longueur du backlog `feeds:commands` du group `moddy-feeds` (`XPENDING`).

## Scalabilité

| Aspect | Capacité |
|---|---|
| Twitch | `/streams` accepte 100 user_id/req, rate limit ≈ 800 req/min → ~80 000 streamers/min |
| YouTube/RSS | poll conditionnel (304) très léger ; concurrence bornée à 20/tick |
| Bluesky | 10 000 DIDs/connexion ; au-delà, ouvrir plusieurs connexions (évolution) |
| DB | index `idx_targets_due` ; `LIMIT 200` par tick → étaler la charge |

### Lancer plusieurs instances

- **Worker commandes** : le consumer group `moddy-feeds` garantit qu'une commande
  n'est traitée qu'une fois → scaling horizontal sûr.
- **Scheduler** : avec plusieurs instances, deux ticks peuvent sélectionner la
  même cible. La **dédup Redis** empêche les doublons d'événements, mais pour
  éviter le polling redondant, préférer **une seule instance scheduler** (ou
  ajouter un `SELECT … FOR UPDATE SKIP LOCKED` — évolution simple).
- **Bluesky** : le worker maintient un état websocket en mémoire ; garder **une
  seule instance** Bluesky (ou sharder les DIDs par instance — évolution).

> Recommandation v1 : **une seule instance** du service suffit largement aux
> besoins (des dizaines de milliers de cibles). Scaler les commandes en premier
> si nécessaire.

## Robustesse intégrée

- Cibles en échec : `failing` après le 1ᵉʳ échec, `disabled` après ~50 échecs
  consécutifs (RSS injoignable, chaîne supprimée…).
- Anti-faux-positif Twitch : 3 cycles offline avant de clore un live.
- Reprise Bluesky sans perte via `cursor`.
- Backoffs exponentiels sur reconnexion Bluesky et erreurs Redis.
