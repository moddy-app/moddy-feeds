# Développement

## Prérequis

- Python 3.11+
- Un PostgreSQL et un Redis accessibles (local : Docker conseillé).

```bash
docker run -d --name moddy-pg   -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
docker run -d --name moddy-redis -p 6379:6379 redis:7
```

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env            # renseigner DATABASE_URL / REDIS_URL
```

> Note : `feedparser` dépend de `sgmllib3k`, qui nécessite `setuptools` pour se
> construire. En cas d'échec de build, `pip install setuptools` d'abord.

## Lancer le service

```bash
python -m app.main
```

Les migrations s'appliquent automatiquement au démarrage. En terminal, les logs
sont en format `console` coloré ; définir `LOG_FORMAT=json` pour reproduire la
sortie Railway.

## Tests

```bash
pytest -q
```

Les tests couvrent la logique pure sans infra (clamp des intervalles,
normalisation d'événements, anti-SSRF, dates, RSS). Les connecteurs réseau et
l'accès DB/Redis sont conçus pour être testés en intégration (non requis pour le
CI de base).

## Tester le pipeline de bout en bout (manuel)

Avec un Redis local et le service lancé :

```python
import asyncio, json
from uuid import uuid4
import redis.asyncio as aioredis

async def main():
    r = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    rid = str(uuid4())
    await r.xadd("feeds:commands", {"data": json.dumps({
        "request_id": rid, "action": "subscribe",
        "platform": "rss", "identifier": "https://www.lemonde.fr/rss/une.xml",
    })})
    # lire la réponse
    resp = await r.xread({"feeds:replies": "0"}, block=10000, count=10)
    print(resp)

asyncio.run(main())
```

## Conventions de code

- **Async partout** ; aucune I/O bloquante dans l'event loop.
- **Pas de SQL en dur hors `core/db.py`**, pas de chaînes de streams/clés hors
  `core/redis.py`.
- Un connecteur ne connaît jamais les guilds — il manipule des **cibles**.
- Les exceptions d'un poll/commande sont **loggées sans tuer la boucle**.
- Lint : `ruff check app/ tests/`.

## Ajouter une plateforme

1. Créer `app/connectors/maplateforme.py` héritant de `Connector`.
2. Implémenter `resolve()` et `poll()` (ou un cycle dédié type Twitch/Bluesky).
3. Ajouter ses bornes dans `POLL_BOUNDS` (`app/config.py`).
4. L'enregistrer dans `app/connectors/__init__.py` (`_REGISTRY` + `available_platforms`).
5. Documenter dans `docs/connectors.md` et tester.
