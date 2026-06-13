# Notes de conception (garanties importantes)

Ce document répond à trois questions structurantes : cibles partagées,
fraîcheur des métadonnées, et résistance au redémarrage.

## 1. Une cible partagée = un seul traitement

**Question :** si une même chaîne / un même compte est suivi par plusieurs
serveurs, est-ce que ça crée plusieurs polls / plusieurs notifications ?

**Réponse : non.** C'est le cœur du design.

- La clé primaire de `targets` est `(platform, target_id)` **canonique**
  (channel_id, DID, user_id, URL). Peu importe le nombre de guilds qui suivent
  une chaîne : il n'y a **qu'une seule ligne**, donc **un seul poll**.
- L'`upsert` à l'abonnement fait un `ON CONFLICT DO UPDATE` : un nouveau serveur
  qui suit une chaîne déjà connue **réutilise** la cible existante (et abaisse au
  besoin son `poll_interval` au minimum demandé).
- Quand une nouveauté est détectée, **un seul événement** est publié dans
  `notifications:queue`. C'est le **bot** qui le dispatche ensuite aux N serveurs
  abonnés (via sa table `social_subscriptions`).
- La **dédup Redis** (`notif:seen:{event_id}`) garantit qu'un même contenu n'est
  jamais publié deux fois, même en cas de poll concurrent ou de redémarrage.

```
200 serveurs suivent MrBeast
        │
        ▼
1 ligne targets (youtube, UCX6OQ3…)   ← 1 poll toutes les ~5 min
        │
        ▼
1 événement dans notifications:queue
        │
        ▼
le bot fan-out vers les 200 guilds
```

**Désabonnement :** le bot n'envoie `unsubscribe` (suppression) que lorsque
**plus aucune guild** ne suit la cible (cf. `integration.md` §3).

## 2. Les métadonnées changent → rafraîchissement intelligent

**Question :** les infos d'une chaîne / d'un compte (nom, avatar) peuvent changer.

L'**ID canonique est stable** (un renommage ne change pas le channel_id / DID /
user_id), donc l'abonnement ne casse jamais. Restent le **nom d'affichage** et
l'**avatar**, qui évoluent. Stratégie à deux régimes, pour ne pas gaspiller
d'appels API :

| Métadonnée | Coût | Stratégie |
|---|---|---|
| Nom YouTube | gratuit (dans le feed Atom) | maj **opportuniste**, seulement si changé |
| Nom Twitch | gratuit (dans `/streams`) | maj **opportuniste**, seulement si changé |
| Avatar Twitch | appel `/users` séparé | **throttlé** : ≤ 1×/24 h, batché par 100, pendant un poll déjà prévu |
| Nom + avatar Bluesky | appel `getProfile` séparé | **throttlé** : cycle horaire, ≤ 50 profils/cycle, les plus périmés d'abord |

- Les événements publiés portent toujours `author_name` / `author_avatar` à jour,
  donc le bot peut aussi rafraîchir son affichage à la réception.
- Le throttle est piloté par `state.meta_at` (timestamp du dernier refresh).

> Voir `connectors/base.py` (`due_for_meta_refresh`, `stamp_meta_refresh`) et
> les méthodes `_refresh_avatars` (Twitch) / `_refresh_profiles_loop` (Bluesky).

## 3. Résistance au redémarrage (timers persistés)

**Question :** que deviennent les timers si le service redémarre ?

Tout ce qui pilote un timer est **persisté** (DB ou Redis), jamais uniquement en
mémoire. Au boot, l'état est rechargé → pas de double notification, pas de
thundering-herd.

| Timer / état | Stockage | Au redémarrage |
|---|---|---|
| `last_poll_at` (prochain poll) | DB `targets` | repris tel quel ; les cibles en retard sont pollées (304 conditionnel ⇒ peu coûteux) |
| Twitch `live` / `offline_cycles` | DB `state` | repris ⇒ pas de fausse notif « re-live » |
| Throttle avatar Twitch (`meta_at`) | DB `state` | repris ⇒ pas de re-fetch massif |
| Throttle profil Bluesky (`meta_at`) | DB `state` (+ hydraté en mémoire au boot) | repris ⇒ pas de re-fetch massif |
| Cursor Bluesky (`time_us`) | Redis | repris ⇒ Jetstream **rejoue** les events manqués (zéro perte) |
| Token Twitch | Redis (TTL) | réutilisé tant que valide |
| Dédup événements | Redis (TTL 7 j) | conservée ⇒ pas de re-notification du contenu déjà vu |

**Conséquences concrètes :**
- Un redémarrage ne renotifie **jamais** un contenu déjà publié (dédup Redis).
- Bluesky ne perd **aucun** post pendant la coupure (reprise par cursor).
- Le premier tick après un boot peut poller un lot de cibles « en retard », mais
  la charge est bornée (`LIMIT 200`/tick, concurrence 20, requêtes conditionnelles
  qui répondent `304`).
- Les marquages « premier poll » (`initialized`) sont en DB ⇒ pas de rafale de
  notifications initiales après un restart.
