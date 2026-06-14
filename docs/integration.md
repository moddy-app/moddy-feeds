# Intégration bot / backend / autres services Moddy

Ce document est le **contrat d'intégration** entre `moddy-feeds` et le reste de
l'écosystème Moddy (bot Discord, backend, futurs services). Toute la
communication passe par **Redis partagé** — aucune API HTTP, aucun accès direct à
la base PostgreSQL du service.

> **Règle d'or :** le bot/backend n'écrit JAMAIS dans la base de `moddy-feeds` et
> ne lit JAMAIS `targets` directement. Le seul contrat est Redis (3 streams).

## 1. Streams Redis utilisés

| Stream | Sens | Producteur | Consommateur |
|---|---|---|---|
| `feeds:commands` | bot → service | bot/backend | `moddy-feeds` (group `moddy-feeds`) |
| `feeds:replies` | service → bot | `moddy-feeds` | bot/backend |
| `notifications:queue` | service → bot | `moddy-feeds` | bot Discord (group au choix) |

Clés annexes (lecture seule pour le backend) :

| Clé | Contenu |
|---|---|
| `feeds:heartbeat` | timestamp du dernier tick (healthcheck) — TTL ~90 s |

> ⚠️ La **dédup** (`notif:seen:*`) et le **cursor Bluesky** (`feeds:bluesky:cursor`)
> sont internes au service : ne pas y toucher.

## 2. S'abonner à une cible (`subscribe`)

### Côté bot — envoyer la commande

```python
import json
from uuid import uuid4

request_id = str(uuid4())
await redis.xadd("feeds:commands", {"data": json.dumps({
    "request_id": request_id,
    "action": "subscribe",            # subscribe | unsubscribe
    "platform": "youtube",            # youtube | twitch | bluesky | rss | instagram
    "identifier": "@mrbeast",         # handle / login / URL — format libre
    "poll_interval": 120,             # optionnel (secondes) — voir §5
})})
```

### Côté bot — attendre la réponse (`feeds:replies`)

Le service résout l'identifiant vers sa **forme canonique**, configure le
connecteur, puis publie une réponse corrélée par `request_id`. Le bot lit
`feeds:replies` (timeout conseillé : **10 s**) avant de confirmer à l'utilisateur.

```python
# Réponse succès
{
  "request_id": "…",
  "ok": True,
  "platform": "youtube",
  "target_id": "UCX6OQ3DkcsbYNE6H8uQQuVA",   # forme canonique résolue
  "display_name": "MrBeast",
  "avatar_url": "https://…",
  "poll_interval": 120                        # valeur effectivement retenue (clampée)
}

# Réponse échec
{ "request_id": "…", "ok": False, "platform": "youtube", "error": "channel_not_found" }
```

Le bot **stocke `target_id`** (forme canonique) dans sa table `social_subscriptions`
et l'utilise ensuite pour corréler les événements reçus.

### Exemple de helper côté bot

```python
async def subscribe(platform, identifier, poll_interval=None, timeout=10):
    request_id = str(uuid4())
    payload = {"request_id": request_id, "action": "subscribe",
               "platform": platform, "identifier": identifier}
    if poll_interval is not None:
        payload["poll_interval"] = poll_interval
    await redis.xadd("feeds:commands", {"data": json.dumps(payload)})
    return await wait_reply(request_id, timeout)   # lit feeds:replies, filtre request_id
```

## 3. Se désabonner (`unsubscribe`)

**Règle de gestion (côté bot) :** envoyer `unsubscribe` **uniquement quand plus
aucune guild** ne suit la cible.

```python
await redis.xadd("feeds:commands", {"data": json.dumps({
    "request_id": str(uuid4()),
    "action": "unsubscribe",
    "platform": "youtube",
    "identifier": "UCX6OQ3DkcsbYNE6H8uQQuVA",   # canonique OU identifiant libre
    # "poll_interval": 300,   # optionnel — voir §5 (intervalle restant le plus exigeant)
})})
```

Comportements :
- Sans `poll_interval` → la cible est **supprimée** (plus aucune guild). Réponse :
  `{ "ok": true, "removed": true, "target_id": "…" }`.
- Avec `poll_interval` → la cible reste, son intervalle est recalculé. Réponse :
  `{ "ok": true, "target_id": "…", "poll_interval": <valeur retenue> }`.
- Cible déjà absente → succès idempotent `{ "ok": true, "removed": true }`.

## 4. Recevoir les événements (`notifications:queue`)

Le service publie chaque nouveauté détectée au **format normalisé identique quelle
que soit la plateforme** :

```json
{
  "event_id": "youtube:dQw4w9WgXcQ",
  "platform": "youtube",
  "type": "video",                   // video | post | live | article
  "target_id": "UCxxxxxxxx",         // corrèle avec social_subscriptions
  "author_name": "MrBeast",
  "author_avatar": "https://…",
  "title": "…",
  "content": "…",
  "url": "https://…",
  "thumbnail": "https://…",
  "published_at": "2026-06-12T14:30:00Z"
}
```

> Les champs optionnels absents (ex. `thumbnail` pour un post Bluesky sans image)
> sont **omis** du JSON. Toujours utiliser `.get()` côté bot.

### Consommer côté bot (consumer group)

```python
await redis.xgroup_create("notifications:queue", "discord-bot", id="0", mkstream=True)

while True:
    resp = await redis.xreadgroup("discord-bot", consumer_name,
                                  {"notifications:queue": ">"}, count=50, block=5000)
    for _stream, messages in resp or []:
        for msg_id, fields in messages:
            event = json.loads(fields["data"])
            # 1) retrouver les guilds qui suivent event["target_id"] (+ platform)
            # 2) dispatcher l'embed Discord
            await redis.xack("notifications:queue", "discord-bot", msg_id)
```

Le service garantit la **déduplication** : un même `event_id` n'est poussé qu'une
fois (fenêtre 7 jours). La queue est bornée à 10 000 entrées (`maxlen` approximatif).

### Mapping suggéré `type` → rendu Discord

| `type` | Plateforme(s) | Suggestion d'embed |
|---|---|---|
| `video` | youtube | « 📺 Nouvelle vidéo » + thumbnail maxres |
| `live` | twitch | « 🔴 En live » + jeu (`content`) + thumbnail |
| `post` | bluesky | « 🦋 Nouveau post » + `content` (≤ 300 car.) |
| `article` | rss | « 📰 Nouvel article » + `content` |

## 5. `poll_interval` — latence configurable

Chaque abonnement peut demander un intervalle de polling. Le service **clamp
silencieusement** dans les bornes de la plateforme et renvoie la valeur retenue
dans la réponse.

| Plateforme | Min | Max | Défaut |
|---|---|---|---|
| youtube | 60 s | 3600 s | 300 s |
| twitch | 30 s | 600 s | 60 s |
| rss | 120 s | 3600 s | 300 s |
| instagram (futur) | 600 s | 86400 s | 1800 s |
| bluesky | — | — | temps réel (paramètre **ignoré**) |

**Cible partagée :** une cible étant commune à plusieurs guilds, le service
retient toujours le **minimum** (le plus exigeant) des intervalles demandés. À
l'`unsubscribe`, le bot doit renvoyer dans `poll_interval` l'intervalle le plus
exigeant **restant** parmi ses guilds ; sans valeur, la cible est supprimée.

## 6. Catalogue des codes d'erreur (`ok: false`)

| `error` | Signification | Action bot suggérée |
|---|---|---|
| `unknown_platform` | plateforme non supportée | message « plateforme invalide » |
| `platform_disabled` | plateforme désactivée côté service (ex. Instagram) | message « bientôt disponible » |
| `missing_identifier` | identifiant vide | redemander la saisie |
| `channel_not_found` / `user_not_found` / `handle_not_found` | cible introuvable | « introuvable, vérifie le nom » |
| `unsafe_url` | URL RSS pointant vers une ressource interne (SSRF) | « URL non autorisée » |
| `no_entries` | flux RSS vide ou non parsable | « ce flux ne contient rien » |
| `fetch_failed` / `bad_status` | échec réseau lors de la résolution | proposer de réessayer |
| `not_supported` | connecteur indisponible (Instagram) | « non disponible » |
| `twitch_not_configured` / `twitch_auth_failed` | secrets Twitch absents/invalides | alerte ops |
| `internal_error` | erreur inattendue | proposer de réessayer + log ops |

## 7. Healthcheck côté backend

Le service écrit `feeds:heartbeat` toutes les 30 s (TTL ~90 s). Le backend peut
surveiller la **présence** de cette clé pour détecter un service mort :

```python
alive = await redis.exists("feeds:heartbeat")   # 0 ⇒ alerter
```

## 8. Checklist d'intégration côté bot

- [ ] Générer un `request_id` unique par commande et corréler la réponse.
- [ ] Timeout de 10 s sur `feeds:replies`, fallback « réessaie plus tard ».
- [ ] Stocker `target_id` canonique (jamais l'identifiant brut) dans `social_subscriptions`.
- [ ] N'envoyer `unsubscribe` (sans `poll_interval`) que lorsque **plus aucune guild** ne suit.
- [ ] Sur `unsubscribe` partiel, renvoyer le `poll_interval` restant le plus exigeant.
- [ ] Consommer `notifications:queue` via consumer group + `xack`.
- [ ] Toujours `.get()` les champs optionnels des événements.
- [ ] Surveiller `feeds:heartbeat` pour l'alerting.
