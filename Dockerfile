# ─── moddy-feeds — image worker (pas de port exposé) ───────────────────────
FROM python:3.11-slim AS base

# Logs Python non bufferisés → visibles immédiatement dans Railway.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dépendances d'abord (cache de couche Docker tant que requirements.txt ne change pas).
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Code applicatif + migrations.
COPY app/ ./app/
COPY migrations/ ./migrations/

# Utilisateur non-root.
RUN useradd --create-home --uid 10001 moddy
USER moddy

# Aucun EXPOSE : worker pur (commandes/queue via Redis).
CMD ["python", "-m", "app.main"]
