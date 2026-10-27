"""Superset settings for the CelesTrak dashboards (mounted as /app/pythonpath)."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]

# Marts are small; the map layers need every point (16 800 satellites, 188 000 conjunctions).
ROW_LIMIT = 200_000
SQL_MAX_ROW = 200_000
SAMPLES_ROW_LIMIT = 1_000

# deck.gl base maps without a Mapbox token: Esri satellite imagery and dark gray canvas,
# OpenStreetMap as a fallback (Carto now answers "API key required"). The tile hosts are
# allowed in img-src/connect-src below.
ESRI = "https://server.arcgisonline.com/ArcGIS/rest/services"
DECKGL_BASE_MAP = [
    [f"tile://{ESRI}/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}", "Satellite (Esri)"],
    [f"tile://{ESRI}/Canvas/World_Dark_Gray_Base/MapServer/tile/{{z}}/{{y}}/{{x}}",
     "Dark gray (Esri)"],
    ["tile://https://tile.openstreetmap.org/{z}/{x}/{y}.png", "Streets (OSM)"],
]
TILE_HOSTS = ["https://server.arcgisonline.com", "https://tile.openstreetmap.org"]

# The 3D pages (service `globe`) are embedded into the dashboard as iframes from
# Markdown blocks: allow that one origin as a frame source and the iframe tag in Markdown.
GLOBE_URL = os.environ.get("GLOBE_URL", "http://localhost:8090")
HTML_SANITIZATION_SCHEMA_EXTENSIONS = {
    "tagNames": ["iframe"],
    "attributes": {"iframe": ["src", "width", "height", "style", "frameborder", "allow",
                              "allowfullscreen", "title"]},
}

TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "content_security_policy": {
        "base-uri": ["'self'"],
        "default-src": ["'self'"],
        "img-src": ["'self'", "blob:", "data:", *TILE_HOSTS],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'", *TILE_HOSTS],
        "frame-src": ["'self'", GLOBE_URL],
        "object-src": "'none'",
        "style-src": ["'self'", "'unsafe-inline'"],
        "script-src": ["'self'", "'strict-dynamic'"],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": False,
    "session_cookie_secure": False,
}

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Dashboards are fully cached for a few minutes: marts change only after build_marts.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
