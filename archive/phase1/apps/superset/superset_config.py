"""Superset configuration for the banking DWH demo (local, single node)."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Query results are cached so that repeated dashboard views do not reload the warehouse
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,       # dashboards can be restricted to roles (one per user group)
}
FAB_ADD_SECURITY_API = True       # roles can be created through the REST API (bootstrap)
ROW_LIMIT = 50000
