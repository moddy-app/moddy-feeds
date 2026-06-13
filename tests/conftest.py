"""Fixtures de test — fournit des variables d'env minimales pour charger `settings`."""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
