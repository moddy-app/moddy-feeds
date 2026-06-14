# CLAUDE.md

Guide pour travailler efficacement sur ce dépôt (lu en priorité par les agents IA).

## Ce qu'est ce projet

`moddy-feeds` est un **microservice worker Python** (pas de serveur HTTP) qui
surveille YouTube, Twitch, Bluesky et des flux RSS, et pousse des **événements
normalisés** vers le bot Discord de Moddy. Toute la communication passe par
**Redis partagé** ; le service a sa **propre base PostgreSQL** (cibles + état).

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

`app/main.py` lance 4 workers asyncio en parallèle, sans HTTP :

- **commands** (`app/commands.py`) — consume `feeds:commands`, répond sur `feeds:replies`.
- **scheduler** (`app/schedulers.py`) — tick 10 s : poll les cibles dues (YouTube/RSS/Twitch/IG).
- **bluesky** (`app/connectors/bluesky.py`) — websocket Jetstream temps réel.
- **heartbeat** — `SET feeds:heartbeat` pour le healthcheck Railway.

```
app/
├── main.py            # orchestrateur asyncio
├── config.py          # settings + POLL_BOUNDS (bornes poll par plateforme)
├── logging_config.py  # logs console (dev) / JSON niveau Railway (prod)
├── commands.py        # feeds:commands → feeds:replies
├── schedulers.py      # tick scheduler + heartbeat
├── connectors/        # base.py + youtube/twitch/bluesky/rss/instagram
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
5. **Anti-SSRF** sur toute URL utilisateur (RSS) : `core/security.py`, à
   l'abonnement ET avant chaque fetch. C'est LE point de sécurité.
6. **Pas de SQL hors `core/db.py`**, pas de noms de streams/clés hors `core/redis.py`.
7. **Une commande/tick qui plante ne tue jamais la boucle** : try/except + log.
8. **Async pur** : aucune I/O bloquante dans l'event loop.

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
