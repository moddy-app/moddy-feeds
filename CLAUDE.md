# CLAUDE.md

Guide pour travailler efficacement sur ce dépôt (lu en priorité par les agents IA).

## Ce qu'est ce projet

`moddy-feeds` est un **microservice worker Python** qui surveille YouTube,
Twitch, Bluesky et des flux RSS, et pousse des **événements normalisés** vers le
bot Discord de Moddy. Toute la communication passe par **Redis partagé** ; le
service a sa **propre base PostgreSQL** (cibles + état).

Le service est un worker : il n'expose **aucune API**. Seule exception, le
callback WebSub YouTube (`app/websub.py`), qui n'ouvre un port que si
`WEBSUB_CALLBACK_URL` et `WEBSUB_SECRET` sont fournis — sans eux, aucun serveur
HTTP ne démarre. Toute autre fonctionnalité doit rester en sortant (polling ou
websocket) ; voir `docs/architecture.md` § « Le cas particulier du HTTP entrant ».

La spécification d'origine est dans [`PROMPT.md`](PROMPT.md). La documentation
complète est dans [`docs/`](docs/) — **commencer par [`docs/README.md`](docs/README.md)**.

## Commandes essentielles

```bash
pip install -r requirements-dev.txt   # dépendances (dev inclut pytest/ruff)
python -m app.main                     # lance tous les workers
pytest -q                              # tests (logique pure, sans infra)
ruff check app/ tests/                 # lint
```

Variables requises pour démarrer : `DATABASE_URL`, `REDIS_URL` (cf. `.env.example`).

## Architecture en bref

`app/main.py` lance 4 workers asyncio, plus 3 optionnels :

- **commands** (`app/commands.py`) — consume `feeds:commands`, répond sur `feeds:replies`.
- **scheduler** (`app/schedulers.py`) — tick 2 s : réserve et poll les cibles dues.
- **bluesky** (`app/connectors/bluesky.py`) — websocket Jetstream temps réel.
- **heartbeat** — `SET feeds:heartbeat` pour le healthcheck Railway.
- *(opt.)* **twitch-eventsub** (`app/connectors/twitch_eventsub.py`) — websocket
  sortant, live en ~2 s ; actif si `TWITCH_USER_REFRESH_TOKEN`.
- *(opt.)* **websub-server / websub-renewal** (`app/websub.py`) — push YouTube en
  ~10 s ; actif si `WEBSUB_CALLBACK_URL` + `WEBSUB_SECRET`.

Les transports push sont des **accélérateurs, jamais des dépendances** : sans
config, tout fonctionne en polling.

```
app/
├── main.py            # orchestrateur asyncio
├── config.py          # settings + POLL_BOUNDS (bornes poll par plateforme)
├── logging_config.py  # logs console (dev) / JSON niveau Railway (prod)
├── commands.py        # feeds:commands → feeds:replies
├── schedulers.py      # tick scheduler + heartbeat
├── websub.py          # callback WebSub YouTube (seul HTTP entrant, optionnel)
├── connectors/        # base.py + youtube/twitch/twitch_eventsub/bluesky/rss/instagram
└── core/              # redis, db (asyncpg), events (dédup), http, security (SSRF), timeutils
migrations/            # SQL idempotent, appliqué au boot
tests/                 # tests de logique pure
docs/                  # documentation (voir docs/README.md)
```

## Contrat Redis (à ne jamais casser sans coordination avec le bot)

| Stream | Sens | Format |
|---|---|---|
| `feeds:commands` | bot → service | `{request_id, action, platform, identifier, poll_interval?}` |
| `feeds:replies` | service → bot | `{request_id, ok, target_id, display_name, avatar_url, poll_interval}` |
| `notifications:queue` | service → bot | événement normalisé (cf. `docs/integration.md`) |

Détails complets : [`docs/integration.md`](docs/integration.md).

## Invariants à respecter (importants)

1. **Une cible = une ligne** `(platform, target_id)` canonique. Un connecteur
   manipule des **cibles**, jamais des guilds. Le fan-out vers les serveurs est
   la responsabilité du bot. → 1 poll, 1 événement, quel que soit le nb de serveurs.
2. **Dédup obligatoire** : tout événement passe par `publish_event()` (Redis
   `SET NX EX`). Ne jamais `xadd` un événement en contournant la dédup.
3. **Timers persistés** : tout état pilotant un timer va en DB (`state`) ou Redis,
   jamais seulement en mémoire — le service doit survivre à un redémarrage sans
   re-notifier ni re-fetcher en masse (cf. `docs/design-notes.md`).
4. **Métadonnées rafraîchies intelligemment** : nom (gratuit) en opportuniste,
   avatar/profil (coûteux) throttlés ≤ 1×/24 h via `state.meta_at`.
5. **Un transport push ne remplace jamais le polling**, il le ralentit via un
   bail daté (`state.push_until`) que le worker push renouvelle. Un bail qui
   expire fait repartir le polling rapide tout seul : c'est la garantie qu'une
   panne de push ne se traduit jamais par des notifications perdues. Corollaire :
   les deux chemins doivent produire le **même `event_id`** (la dédup fait le tri).
6. **Anti-SSRF** sur toute URL utilisateur (RSS) : `core/security.py`, à
   l'abonnement ET avant chaque fetch. C'est LE point de sécurité.
7. **Toute URL de callback publique est authentifiée** : la notification WebSub
   est vérifiée en HMAC, et un challenge d'abonnement n'est confirmé que pour une
   cible connue en base. Une requête non signée ou mal signée est rejetée.
8. **Pas de SQL hors `core/db.py`**, pas de noms de streams/clés hors `core/redis.py`.
9. **Une commande/tick qui plante ne tue jamais la boucle** : try/except + log.
10. **Async pur** : aucune I/O bloquante dans l'event loop.
11. **Le scheduler doit rester indexable** : l'intervalle effectif est une
   expression SQL, bornée par une condition indexable (`last_poll_at <= now() -
   min_plateforme`). Si l'expression change, l'intervalle effectif doit rester
   ≥ au minimum de la plateforme, sinon des cibles dues seraient ignorées.

## Logs

Lisibles en dev (`LOG_FORMAT=console`, coloré) et exploitables par Railway en
prod (`LOG_FORMAT=json` avec champ `level` ∈ `debug|info|warn|error`). Auto-détecté
selon TTY. Conventions : `INFO` = cycle normal, `warn` = dégradation récupérable,
`error` = exception inattendue (+ stacktrace).

## Ajouter une plateforme

1. `app/connectors/<nom>.py` héritant de `Connector` (`resolve` + `poll`).
2. Bornes dans `POLL_BOUNDS` (`config.py`).
3. Enregistrer dans `connectors/__init__.py` (`_REGISTRY` + `available_platforms`).
4. Documenter dans `docs/connectors.md` + test.

## Conventions

- Commits clairs et descriptifs ; développement sur la branche dédiée.
- Code commenté en français (cohérence avec l'existant), docstrings sur les
  modules/fonctions publiques.
- Lancer `pytest -q` avant de pousser.
