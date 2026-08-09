# Connecteurs

Chaque plateforme est un module dans `app/connectors/` qui implémente l'interface
`Connector` (`app/connectors/base.py`) :

```python
async def resolve(identifier) -> ResolvedTarget   # identifiant libre → forme canonique
async def prime(target)                            # 1er poll : marquer le contenu vu
async def poll(target) -> list[event]              # pollers : retourne les events à publier
async def on_subscribe(target)                     # hook (Bluesky : ajoute le DID)
async def on_unsubscribe(target_id)                # hook (Bluesky : retire le DID)
```

Le registre `get_connector(platform)` résout l'instance. `available_platforms()`
filtre selon la config (`BLUESKY_ENABLED`, `INSTAGRAM_ENABLED`).

---

## YouTube (`youtube.py` + `../app/websub.py`)

- **Détection rapide (optionnelle) :** **WebSub**. À l'abonnement, le service
  demande au hub `pubsubhubbub.appspot.com` de pousser les futures vidéos ; la
  notification arrive en ~10 s au lieu de 1-2 min. Actif seulement si
  `WEBSUB_CALLBACK_URL` + `WEBSUB_SECRET` sont configurés.
- **Détection de repli (toujours active) :** polling du feed Atom public, sans
  clé ni quota : `https://www.youtube.com/feeds/videos.xml?channel_id=UC…`
- **Conditionnel :** envoie `If-None-Match` (ETag) → `304` = rien de neuf.
- **Cohabitation :** les deux chemins passent par `parse_feed()`, donc produisent
  le même `event_id` (`youtube:{video_id}`) → une seule notification. Tant que le
  lease WebSub est valide (`state.push_until`), le poll de la chaîne est ralenti
  à 900 s ; si le lease expire sans renouvellement, le poll rapide reprend seul.
- **Sécurité du callback :** notification vérifiée en HMAC (`X-Hub-Signature`),
  challenge d'abonnement confirmé uniquement pour une chaîne connue en base,
  corps borné à 1 Mo, `channel_id` relu **dans le feed** et non dans le topic.
- **Forme canonique :** `channel_id` (`UC…`).
- **Résolution @handle :**
  - Avec `YOUTUBE_API_KEY` (recommandé) : Data API v3 `channels?forHandle=` →
    `id` + `title` + `thumbnails` (1 unité de quota/abonnement).
  - Sans clé : scraping du HTML de `youtube.com/@handle` (extraction `channelId`).
  - Si l'identifiant est déjà un `UC…` ou une URL `/channel/`, on récupère juste
    le nom via le feed Atom.
- **Filtres :** re-publications/contenus de plus de 24 h (`too_old`) et lives
  programmés (date `published` future) sont ignorés.
- **Premier poll :** toutes les vidéos existantes sont marquées vues (pas de
  rafale de 15 notifs à l'abonnement).

---

## Twitch (`twitch.py` + `twitch_eventsub.py`)

- **Détection rapide (optionnelle) :** **EventSub en transport WebSocket** —
  connexion *sortante*, comme Bluesky. `stream.online` arrive en ~2 s.
  Nécessite un **user access token** (`TWITCH_USER_REFRESH_TOKEN`, cf.
  `.env.example`) ; le refresh token tourne et est persisté dans Redis.
  Plafond Twitch : **300 abonnements par session** — au-delà, les cibles sont
  priorisées par activité récente et le reste reste en polling.
- **Détection de repli (toujours active) :** polling Helix `GET /streams`,
  **jusqu'à 100 user_id par requête**, les paquets en parallèle. Réponse riche :
  titre, jeu, viewers, thumbnail — pas de 2ᵉ appel.
- **Cohabitation :** les deux chemins passent par `make_live_event()`, donc
  produisent le même `event_id` (`twitch:{stream_id}`). Sur notification
  EventSub, un appel `/streams` enrichit le payload (titre/jeu/miniature) ; s'il
  n'est pas encore à jour, l'événement est publié quand même, en version courte.
- **Token applicatif :** client credentials, **caché dans Redis**
  (`feeds:twitch:token`, TTL = `expires_in - 300 s`).
- **Forme canonique :** `user_id` Twitch.
- **Résolution login :** Helix `GET /users?login=` → `id`, `display_name`,
  `profile_image_url`. Supporte aussi une URL `twitch.tv/login`.
- **Transitions :** offline→live publie un event `type=live` (id = `stream.id`,
  unique). live→offline est confirmé sur **3 cycles consécutifs** avant de reset
  `live=False` (anti micro-coupure → pas de fausse re-notif au retour).
- **Batching :** le scheduler regroupe les cibles Twitch dues en paquets de 100,
  émis en parallèle (en série, 500 cibles = 5 aller-retours avant la 1ʳᵉ notif).
- **Prérequis :** `TWITCH_CLIENT_ID` + `TWITCH_CLIENT_SECRET`. Sans eux, les
  abonnements Twitch répondent `twitch_not_configured`.

---

## Bluesky (`bluesky.py`)

- **Détection :** websocket **Jetstream** (connexion sortante, publique, sans
  auth, temps réel) filtré sur `app.bsky.feed.post` + `wantedDids`.
- **Forme canonique :** `did:plc:…`.
- **Résolution :**
  - Handle → DID : `com.atproto.identity.resolveHandle`.
  - Nom + avatar : `app.bsky.actor.getProfile`.
- **Mise à jour à chaud :** chaque subscribe/unsubscribe envoie un
  `options_update` sur le websocket (pas de reconnexion). Max **10 000 DIDs** par
  connexion.
- **Filtres :** seuls les `commit`/`create` sur `app.bsky.feed.post` non-`reply`
  sont publiés. Les images sont extraites de `record.embed` (URL CDN reconstruite).
- **Reprise :** `cursor` (`time_us`) sauvegardé toutes les ~10 s dans
  `feeds:bluesky:cursor`. Reconnexion en backoff exponentiel (1 s → 60 s) avec
  rotation des hôtes Jetstream (`jetstream1/2.us-east/west`).
- Le paramètre `poll_interval` est **ignoré** (temps réel).

---

## RSS (`rss.py`)

- **Détection :** polling conditionnel (`If-None-Match` / `If-Modified-Since`).
- **Forme canonique :** l'URL du flux elle-même.
- **`event_id` :** `rss:` + `sha256(url + guid)[:24]` (déterministe).
- **Sécurité — LE point critique :** les URLs viennent des utilisateurs.
  `app/core/security.py` impose **anti-SSRF** :
  - schéma `http(s)` uniquement,
  - résolution DNS et **refus des IP** privées / loopback / link-local /
    réservées / metadata cloud (`169.254.169.254`),
  - **3 redirections max**, chacune revalidée,
  - réponse bornée à **~2 Mo**.
  Validation à l'abonnement **et** avant chaque fetch (anti DNS-rebinding).
- **Validation à l'abonnement :** un fetch test doit renvoyer un flux parsable
  avec ≥ 1 entrée, sinon `ok: false` (`no_entries`).
- **Premier poll :** entrées existantes marquées vues sans publier.

---

## Instagram (`instagram.py`) — désactivé par défaut

Il **n'existe aucune API officielle** pour suivre un compte arbitraire (la Graph
API ne couvre que les comptes qu'on possède). Le connecteur est un **squelette**
prêt à brancher sur un scraper tiers payant (Option A : Apify, RapidAPI…) le jour
où la demande le justifie — sa structure est identique à un poller RSS.

Tant que `INSTAGRAM_ENABLED=false`, la plateforme n'apparaît pas dans
`available_platforms()` et toute commande répond `platform_disabled` /
`not_supported`. Voir la section 8 du `PROMPT.md` pour l'analyse complète des
options (et pourquoi lancer sans Instagram).

### Brancher l'Option A plus tard

1. Mettre `INSTAGRAM_ENABLED=true`.
2. Implémenter `resolve()` (username → id stable) et `poll()` (appel scraper →
   `make_event(type="post", …)`) dans `instagram.py`.
3. Ajouter un quota dur (nombre max de comptes IG) pour borner la facture.
