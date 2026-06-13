# Moddy Feeds — Spécification du microservice de notifications

Service autonome (nouveau repo), hébergé sur Railway, **sans aucun endpoint HTTP exposé**. Tout passe par Redis :
- il reçoit les commandes d'abonnement via un stream Redis,
- il surveille les plateformes,
- il pousse les événements normalisés dans la queue Redis que le bot consomme.

Il a sa **propre base PostgreSQL** pour stocker ses cibles et son état interne.

---

## 1. Architecture

```
   Bot / Backend Moddy
         │
         │  XADD feeds:commands        (Redis partagé)
         │  { action: subscribe, platform, identifier, request_id }
         ▼
┌──────────────── moddy-feeds (worker Python, pas de HTTP) ────────────────┐
│                                                                           │
│  Consumer feeds:commands          Connecteurs                             │
│  (subscribe / unsubscribe)   ┌────────────────────────────────┐           │
│                              │ YouTube   → polling feed Atom   │           │
│         PostgreSQL dédiée    │ Twitch    → polling Helix       │           │
│         (targets + state)    │ Bluesky   → websocket Jetstream │           │
│                              │ Instagram → voir section 8      │           │
│                              │ RSS       → polling             │           │
│                              └───────────────┬────────────────┘           │
│                                              │                            │
│                                   normalize + dédup                       │
└──────────────────────────────────────────────┼────────────────────────────┘
                                               ▼
                            XADD notifications:queue    (Redis partagé)
                                               │
                                               ▼
                                   Bot Discord (consumer group)
```

**Conséquence importante du choix "rien d'exposé"** : pas d'endpoint public = pas de webhooks entrants possibles. Donc pas de WebSub YouTube ni d'EventSub Twitch (les deux exigent que Google/Twitch puissent POSTer chez toi). Tout fonctionne en **polling ou websocket sortant**, ce qui est très bien pour une v1 :

| Service | Méthode | Latence typique |
|---|---|---|
| YouTube | Polling feed Atom | ≤ 5 min |
| Twitch | Polling Helix `/streams` | ≤ 1 min |
| Bluesky | Websocket Jetstream (connexion sortante) | temps réel |
| Instagram | Pas de méthode propre — voir section 8 | — |
| RSS | Polling conditionnel | ≤ 5 min |

Si un jour tu veux le temps réel YouTube/Twitch, il suffira d'ajouter un mini serveur HTTP avec 2 routes de callback — l'architecture ne change pas, seul le connecteur change.

**Séparation des responsabilités** (inchangée) :
- `moddy-feeds` gère des **cibles** : une chaîne YouTube suivie par 200 serveurs = 1 cible.
- Le bot garde la table `social_subscriptions` (guild ↔ cible) dans la DB principale de Moddy et dispatch à la réception des événements.

---

## 2. Canal de commande : stream Redis `feeds:commands`

### Côté bot — envoyer une commande

```python
request_id = str(uuid4())
await redis.xadd("feeds:commands", {"data": json.dumps({
    "request_id": request_id,
    "action": "subscribe",            # subscribe | unsubscribe
    "platform": "youtube",            # youtube | twitch | bluesky | instagram | rss
    "identifier": "@mrbeast",         # handle / login / URL, format libre
    "poll_interval": 120,             # optionnel, en secondes — voir règles ci-dessous
})})
```

### Côté service — consommer et répondre

Le service consomme `feeds:commands` (consumer group `moddy-feeds`), résout l'identifiant vers sa forme canonique, fait le setup du connecteur, puis publie la réponse sur `feeds:replies` :

```python
await redis.xadd("feeds:replies", {"data": json.dumps({
    "request_id": request_id,          # le bot corrèle avec sa demande
    "ok": True,
    "platform": "youtube",
    "target_id": "UCX6OQ3DkcsbYNE6H8uQQuVA",   # forme canonique résolue
    "display_name": "MrBeast",
    "avatar_url": "https://...",
})})
# ou en cas d'échec :
# {"request_id": ..., "ok": false, "error": "channel_not_found"}
```

Le bot attend sa réponse (consumer sur `feeds:replies`, timeout 10 s) avant de confirmer à l'utilisateur "✅ Notifications activées pour **MrBeast**" et d'insérer sa ligne `social_subscriptions` avec le `target_id` canonique reçu.

Règle de gestion : le bot n'envoie `unsubscribe` que quand **plus aucune guild** ne suit la cible.

### Latence de polling configurable (`poll_interval`)

Chaque abonnement peut spécifier son intervalle de polling en secondes. Règles :

- **Bornes par plateforme** (le service clamp silencieusement et renvoie la valeur retenue dans la réponse) :

| Plateforme | Min | Max | Défaut |
|---|---|---|---|
| youtube | 60 s | 3600 s | 300 s |
| twitch | 30 s | 600 s | 60 s |
| rss | 120 s | 3600 s | 300 s |
| instagram (futur) | 600 s | 86400 s | 1800 s |
| bluesky | — | — | — (temps réel, paramètre ignoré) |

- **Une cible étant partagée entre plusieurs guilds**, si deux abonnements demandent des intervalles différents pour la même cible, le service retient le **minimum** des intervalles actifs (stocké dans `targets.poll_interval`). À l'`unsubscribe`, le bot renvoie l'intervalle le plus exigeant restant parmi ses guilds (champ `poll_interval` sur la commande unsubscribe aussi), ou le service retombe sur le défaut si rien n'est précisé.
- Le scheduler ne fait plus une boucle globale par plateforme mais une boucle par tick (10 s) qui poll les cibles dont `now() >= last_poll_at + poll_interval` :

```python
async def scheduler_tick():
    due = await db.fetch("""
        SELECT * FROM targets
        WHERE status = 'active'
          AND platform = ANY($1)
          AND (last_poll_at IS NULL
               OR last_poll_at + (poll_interval || ' seconds')::interval <= now())
        LIMIT 200
    """, ["youtube", "rss"])
    ...
```

Pour Twitch, le batching par 100 reste : à chaque tick, regroupe les cibles Twitch "dues" en paquets de 100 pour `/streams`.

---

## 3. Base PostgreSQL du service

```sql
CREATE TABLE targets (
    platform      TEXT NOT NULL,
    target_id     TEXT NOT NULL,          -- forme canonique
    display_name  TEXT,
    avatar_url    TEXT,
    status        TEXT DEFAULT 'active',  -- active | failing | disabled
    fail_count    INT  DEFAULT 0,
    poll_interval INT,                     -- secondes ; NULL = défaut plateforme
    last_poll_at  TIMESTAMPTZ,
    state         JSONB DEFAULT '{}',     -- etag RSS, dernier item vu, live en cours...
    created_at    TIMESTAMPTZ DEFAULT now(),
    last_event_at TIMESTAMPTZ,
    PRIMARY KEY (platform, target_id)
);
```

La dédup des événements reste dans le Redis partagé :

```python
async def is_duplicate(event_id: str) -> bool:
    return not await redis.set(f"notif:seen:{event_id}", "1", nx=True, ex=604_800)
```

---

## 4. Publication des événements

```python
async def publish_event(event: dict) -> None:
    if await is_duplicate(event["event_id"]):
        return
    await redis.xadd("notifications:queue",
                     {"data": json.dumps(event)},
                     maxlen=10_000, approximate=True)
```

Format normalisé, identique quelle que soit la plateforme :

```json
{
  "event_id": "youtube:dQw4w9WgXcQ",
  "platform": "youtube",
  "type": "video",                   // video | post | live | article
  "target_id": "UCxxxxxxxx",
  "author_name": "MrBeast",
  "author_avatar": "https://...",
  "title": "...",
  "content": "...",
  "url": "https://...",
  "thumbnail": "https://...",
  "published_at": "2026-06-12T14:30:00Z"
}
```

---

## 5. YouTube — polling du feed Atom

Chaque chaîne expose gratuitement ses ~15 dernières vidéos, sans clé ni quota :

```
https://www.youtube.com/feeds/videos.xml?channel_id=UC...
```

### Boucle de polling (toutes les 5 min)

```python
import xml.etree.ElementTree as ET

NS = {"atom": "http://www.w3.org/2005/Atom",
      "yt": "http://www.youtube.com/xml/schemas/2015"}

async def poll_youtube(target):
    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={target.target_id}"
    headers = {}
    if etag := target.state.get("etag"):
        headers["If-None-Match"] = etag

    r = await http.get(url, headers=headers, timeout=15)
    if r.status_code == 304:
        return                                  # rien de nouveau
    target.state["etag"] = r.headers.get("ETag")

    root = ET.fromstring(r.content)
    for entry in root.findall("atom:entry", NS):
        video_id  = entry.find("yt:videoId", NS).text
        title     = entry.find("atom:title", NS).text
        published = entry.find("atom:published", NS).text

        event_id = f"youtube:{video_id}"
        # Filtre anti-vieux contenu (re-publications, premier poll)
        if too_old(published, hours=24):
            continue
        await publish_event({
            "event_id": event_id,
            "platform": "youtube",
            "type": "video",
            "target_id": target.target_id,
            "author_name": target.display_name,
            "title": title,
            "url": f"https://youtube.com/watch?v={video_id}",
            "thumbnail": f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
            "published_at": published,
        })
```

⚠️ **Premier poll d'une cible** : marquer les vidéos existantes comme vues (dédup) sans publier, sinon 15 notifs d'un coup à l'abonnement.

⚠️ Le feed liste aussi les **lives programmés et Shorts** comme des entrées normales — filtre si besoin (un Short a une URL `/shorts/` accessible en HEAD, un live programmé a une `published` future).

### Résolution @handle → channel_id (à l'abonnement)

- **Avec clé API** (recommandé, fiable) : `GET https://www.googleapis.com/youtube/v3/channels?part=id,snippet&forHandle=@mrbeast&key=...` → `items[0].id` + `snippet.title` + `snippet.thumbnails`. 1 unité de quota sur 10 000/jour, uniquement à l'abonnement.
- **Sans clé** : fetch `https://youtube.com/@mrbeast`, extraire `"channelId":"UC..."` du HTML. Gratuit mais fragile.

---

## 6. Twitch — polling Helix `/streams`

Sans endpoint exposé, EventSub est impossible → on interroge l'API officielle Helix. Elle accepte **jusqu'à 100 streamers par requête**, donc même avec des milliers de cibles ça reste léger.

### Token applicatif (prérequis)

App créée sur dev.twitch.tv/console, puis client credentials :

```python
r = await http.post("https://id.twitch.tv/oauth2/token", data={
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials",
})
token = r.json()["access_token"]     # ~60 jours ; cache Redis avec TTL expires_in - 300
```

### Boucle de détection des lives (toutes les 60 s)

```python
HEADERS = {"Client-Id": CLIENT_ID, "Authorization": f"Bearer {token}"}

async def poll_twitch(targets: list):
    live_now: dict[str, dict] = {}

    for batch in chunked([t.target_id for t in targets], 100):
        params = [("user_id", uid) for uid in batch] + [("first", "100")]
        r = await http.get("https://api.twitch.tv/helix/streams",
                           params=params, headers=HEADERS)
        for s in r.json()["data"]:
            live_now[s["user_id"]] = s

    for t in targets:
        was_live = t.state.get("live", False)
        s = live_now.get(t.target_id)

        if s and not was_live:                       # passage offline → live
            t.state["live"] = True
            await publish_event({
                "event_id": f"twitch:{s['id']}",      # id unique du stream
                "platform": "twitch",
                "type": "live",
                "target_id": t.target_id,
                "author_name": s["user_name"],
                "title": s["title"],
                "content": s["game_name"],
                "url": f"https://twitch.tv/{s['user_login']}",
                "thumbnail": s["thumbnail_url"]
                              .replace("{width}", "1280").replace("{height}", "720"),
                "published_at": s["started_at"],
            })
        elif not s and was_live:                      # fin du live
            t.state["live"] = False                   # (event "offline" optionnel)
```

Gros avantage sur EventSub au passage : la réponse `/streams` contient **déjà** le titre, le jeu, les viewers et la thumbnail — pas de second appel d'enrichissement.

Limites : rate limit Helix ≈ 800 requêtes/min par client_id → 80 000 streamers surveillés à la minute, très loin de tes besoins. Anti-faux-positifs : si un stream "disparaît" un seul cycle (micro-coupure), attends 2-3 cycles offline avant de reset `live=False`, sinon tu renverras une notif au retour. La dédup par `stream.id` protège déjà du cas inverse.

### Résolution login → user_id (à l'abonnement)

```
GET https://api.twitch.tv/helix/users?login=pseudo
→ data[0].id, display_name, profile_image_url
```

---

## 7. Bluesky — websocket Jetstream

Connexion **sortante** (aucune exposition nécessaire), publique, sans auth, temps réel.

### Connexion

```
wss://jetstream2.us-east.bsky.network/subscribe
    ?wantedCollections=app.bsky.feed.post
    &wantedDids=did:plc:xxx
    &wantedDids=did:plc:yyy
    &cursor=<time_us>            # optionnel : rejoue après une coupure
```

`wantedDids` : max 10 000 DIDs par connexion. Instances : `jetstream1/2.us-east` et `jetstream1/2.us-west` (`.bsky.network`).

### Messages reçus

```json
{
  "did": "did:plc:xxx",
  "time_us": 1765519626134432,
  "kind": "commit",
  "commit": {
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3l3qo2vutsw2b",
    "record": {
      "$type": "app.bsky.feed.post",
      "text": "Contenu du post",
      "createdAt": "2026-06-12T14:30:00.000Z",
      "reply": { },          // présent si c'est une réponse → à ignorer
      "embed": { }           // images / lien / quote
    }
  }
}
```

Worker :

```python
async for raw in ws:
    msg = json.loads(raw)
    save_cursor(msg.get("time_us"))            # Redis, toutes les ~10 s
    c = msg.get("commit", {})
    if (msg.get("kind") != "commit"
            or c.get("operation") != "create"
            or c.get("collection") != "app.bsky.feed.post"
            or "reply" in c.get("record", {})):
        continue

    did, rkey = msg["did"], c["rkey"]
    rec = c["record"]
    await publish_event({
        "event_id": f"bluesky:{did}:{rkey}",
        "platform": "bluesky",
        "type": "post",
        "target_id": did,
        "author_name": display_name_cache[did],
        "content": rec.get("text", "")[:300],
        "url": f"https://bsky.app/profile/{did}/post/{rkey}",
        "published_at": rec.get("createdAt"),
    })
```

### Mettre à jour les comptes suivis sans reconnecter

À chaque subscribe/unsubscribe Bluesky, envoie sur le websocket :

```json
{"type": "options_update",
 "payload": {"wantedCollections": ["app.bsky.feed.post"],
             "wantedDids": ["did:plc:xxx", "did:plc:nouveau"]}}
```

### Reconnexion

Backoff exponentiel (1 s → 60 s max) + reprise avec `cursor` = dernier `time_us` sauvegardé → zéro événement perdu (Jetstream garde plusieurs heures de replay).

### Résolutions (API publique, sans auth, à l'abonnement)

- Handle → DID : `GET https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=moddy.app`
- Nom + avatar : `GET https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor={did}` (cache 1 h)

Les images d'un post sont dans `record.embed` (type `app.bsky.embed.images`) ; l'URL CDN se construit ainsi : `https://cdn.bsky.app/img/feed_fullsize/plain/{did}/{blob_cid}@jpeg`.

---

## 8. Instagram — soyons honnêtes : il n'y a pas de bonne solution

C'est la plateforme que je te déconseille d'inclure au lancement. État des lieux factuel :

**Il n'existe aucune API officielle** pour suivre les posts d'un compte arbitraire. L'API Instagram (Meta Graph) ne donne accès qu'aux comptes **que tu possèdes** (business/creator liés à ton app, avec validation Meta). Impossible donc de proposer "suis @cristiano" proprement.

Les options qui existent réellement, avec leurs défauts :

### Option A — Scrapers tiers payants (la moins pire si tu y tiens)
Des services comme **Apify** (acteurs "Instagram Scraper"), **ScrapingBot**, ou des APIs sur RapidAPI (ex. "instagram-scraper-api") exposent une API REST : tu donnes un username, ils renvoient les derniers posts en JSON (id, caption, image, date). Ton connecteur devient un simple poller HTTP.
- ✅ Intégration en une heure, données structurées propres.
- ❌ Payant à la requête (~quelques $/1000 requêtes) → coût qui scale avec le nombre de comptes × fréquence de poll. À 500 comptes pollés toutes les 15 min, ça chiffre vite.
- ❌ Contre les CGU d'Instagram (le risque juridique est porté par le scraper, mais la fiabilité fluctue quand Meta durcit ses défenses).

### Option B — Scraping maison
Endpoint web non documenté (`https://www.instagram.com/api/v1/users/web_profile_info/?username=...` avec les bons headers) ou Playwright.
- ❌ Bans d'IP rapides depuis un datacenter (Railway sera grillé en quelques heures), nécessite des proxies résidentiels payants, casse à chaque changement de Meta. Je te le déconseille fermement : c'est un puits de maintenance sans fond pour un service que tu veux justement "set and forget".

### Option C — RSSHub route Instagram
Existe, mais exige des cookies de session Instagram en config, qui expirent et font bannir le compte utilisé. Même fragilité que l'option B, avec une couche en plus.

### Ma recommandation
Lance sans Instagram. Architecture-wise, ton service est prêt : le jour où tu veux l'ajouter, c'est un connecteur poller de plus (Option A), identique au connecteur RSS dans sa structure. Et tu pourras décider à ce moment-là si la demande des serveurs justifie le coût du scraper. Si tu veux quand même tester dès maintenant, prends Apify avec un quota dur (nombre max de comptes Instagram global) pour borner la facture.

---

## 9. RSS générique — polling conditionnel

```python
import feedparser

async def poll_rss(target):
    headers = {"User-Agent": "Moddy/1.0 (+https://moddy.app)"}
    if etag := target.state.get("etag"):
        headers["If-None-Match"] = etag
    if lm := target.state.get("last_modified"):
        headers["If-Modified-Since"] = lm

    r = await http.get(target.target_id, headers=headers,
                       timeout=15, follow_redirects=True)
    if r.status_code == 304:
        return
    if r.status_code != 200:
        await register_failure(target)        # disabled après ~50 échecs
        return

    target.state |= {"etag": r.headers.get("ETag"),
                     "last_modified": r.headers.get("Last-Modified")}

    parsed = feedparser.parse(r.content)
    first_run = not target.state.get("initialized")
    target.state["initialized"] = True

    for entry in parsed.entries[:20]:
        guid = entry.get("id") or entry.get("link")
        event_id = "rss:" + hashlib.sha256(
            (target.target_id + guid).encode()).hexdigest()[:24]
        if first_run:
            await mark_seen(event_id)          # vu, mais pas publié
            continue
        await publish_event({
            "event_id": event_id,
            "platform": "rss",
            "type": "article",
            "target_id": target.target_id,
            "author_name": parsed.feed.get("title", target.target_id),
            "title": entry.get("title"),
            "content": strip_html(entry.get("summary", ""))[:300],
            "url": entry.get("link"),
            "published_at": entry.get("published"),
        })
```

Règles :
- Intervalle 5 min (30 min pour les flux inactifs depuis > 7 jours).
- **Anti-SSRF à l'abonnement** (les URLs viennent des utilisateurs) : résoudre le DNS et refuser IP privées / localhost / link-local / metadata cloud, limiter à 3 redirections et ~2 Mo de réponse. C'est LE point de sécurité du service.
- Validation à l'abonnement : un fetch test doit renvoyer un flux parsable avec ≥ 1 entrée, sinon réponse `ok: false` au bot.

---

## 10. Structure du repo

```
moddy-feeds/
├── app/
│   ├── main.py                  # asyncio.run: lance tous les workers
│   ├── config.py                # pydantic-settings
│   ├── commands.py              # consumer feeds:commands + réponses feeds:replies
│   ├── connectors/
│   │   ├── base.py              # interface: resolve(identifier), setup, teardown, poll/run
│   │   ├── youtube.py
│   │   ├── twitch.py
│   │   ├── bluesky.py
│   │   ├── instagram.py         # plus tard (option A)
│   │   └── rss.py
│   ├── core/
│   │   ├── events.py            # normalize + publish + dédup
│   │   ├── db.py                # asyncpg
│   │   └── redis.py
│   └── schedulers.py            # boucles: yt 5min, twitch 60s, rss 5min
├── Dockerfile
└── railway.json
```

Pas de FastAPI, pas de serveur HTTP : `main.py` lance simplement les tâches asyncio (consumer de commandes, Jetstream, boucles de polling). Le "healthcheck" Railway peut être un simple `SET feeds:heartbeat` toutes les 30 s que ton backend surveille — ou tu actives le restart-on-crash et basta.

---

## 11. Variables d'environnement

```
DATABASE_URL=...                  # PostgreSQL dédiée au service
REDIS_URL=...                     # Redis PARTAGÉ avec le bot (queue + commandes)
YOUTUBE_API_KEY=...               # uniquement résolution @handle (1 unité/abonnement)
TWITCH_CLIENT_ID=...
TWITCH_CLIENT_SECRET=...
```

C'est tout. Pas de domaine, pas de secret HMAC, pas de token d'API interne — il n'y a rien à protéger puisque rien n'est exposé.

---

## 12. Ordre de développement

1. **Squelette** : consumer `feeds:commands` + réponses + DB + un connecteur factice qui publie un faux événement → valide la boucle bot → service → queue → bot.
2. **RSS** : valide le vrai pipeline, et sert de base au connecteur YouTube.
3. **YouTube** : même logique que RSS avec le feed Atom + résolution de handle.
4. **Bluesky** : Jetstream + options_update + cursor.
5. **Twitch** : token app + polling /streams + transitions live/offline.
6. **Robustesse** : statuts failing/disabled, heartbeat, backoffs.
7. **(Plus tard) Instagram** : connecteur Apify si la demande le justifie.
