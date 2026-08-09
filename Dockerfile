# ─── moddy-feeds — image worker ────────────────────────────────────────────
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

# Le service reste un worker (commandes/queue via Redis). Le port n'est ouvert
# que si WEBSUB_CALLBACK_URL/WEBSUB_SECRET sont fournis — sinon rien n'écoute.
EXPOSE 8080

CMD ["python", "-m", "app.main"]
