# Documentation — moddy-feeds

`moddy-feeds` est le microservice de notifications multi-plateformes de Moddy.
C'est un **worker Python autonome** (aucun endpoint HTTP exposé) qui :

1. reçoit des commandes d'abonnement via un **stream Redis** (`feeds:commands`),
2. surveille YouTube, Twitch, Bluesky et les flux RSS (Instagram en option),
3. pousse des **événements normalisés** dans une queue Redis (`notifications:queue`)
   que le bot Discord consomme.

Il possède sa **propre base PostgreSQL** (cibles + état interne) et communique
avec le reste de Moddy **uniquement via Redis partagé**.

## Sommaire

| Document | Contenu |
|---|---|
| [`architecture.md`](architecture.md) | Vue d'ensemble, flux de données, choix de conception |
| [`integration.md`](integration.md) | **Intégration bot / backend / autres services** (contrat Redis) |
| [`design-notes.md`](design-notes.md) | Garanties : cibles partagées, refresh métadonnées, restart |
| [`connectors.md`](connectors.md) | Détail de chaque connecteur (YouTube, Twitch, Bluesky, RSS, Instagram) |
| [`database.md`](database.md) | Schéma PostgreSQL, migrations, sémantique des champs |
| [`operations.md`](operations.md) | Déploiement Railway, variables d'env, logs, scalabilité, monitoring |
| [`development.md`](development.md) | Lancer en local, tests, conventions de code |

## Démarrage rapide

```bash
cp .env.example .env          # renseigner DATABASE_URL et REDIS_URL
pip install -r requirements.txt
python -m app.main            # lance tous les workers
```

Voir [`development.md`](development.md) pour le détail.
