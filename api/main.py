import os
from fastapi import FastAPI

# =========================
# Environment
# =========================
ENV = os.getenv("ENV", "development")

# =========================
# Create app FIRST (critical)
# =========================
app = FastAPI(
    title="UG Board Engine",
    docs_url="/docs" if ENV != "production" else None,
    redoc_url="/redoc" if ENV != "production" else None,
)

# =========================
# Root health check
# =========================
@app.get("/", tags=["Health"])
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online",
        "environment": ENV,
        "docs_enabled": ENV != "production",
    }

# =========================
# Import routers AFTER app exists
# =========================

# Health
from api.admin.health import router as health_router

# Admin (MANUAL / HUMAN-triggered)
from api.admin.alerts import router as alerts_router
from api.admin.publish import router as publish_router

# Internal (SYSTEM / CRON)
from api.admin.internal import router as internal_router
from api.admin.weekly import router as weekly_router

# Charts (READ-ONLY)
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router
from api.charts.index import router as index_router   # ✅ CORRECT LOCATION

# Ingestion (WRITE / INPUT)
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# =========================
# Register routers
# =========================

# Health
app.include_router(
    health_router,
    tags=["Health"],
)

# =========================
# Charts (PUBLIC / READ-ONLY)
# =========================

app.include_router(
    top100_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    trending_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    regions_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    index_router,
    prefix="/charts",
    tags=["Charts"],
)

# =========================
# Ingestion (INPUT / TOKEN)
# =========================

app.include_router(
    youtube_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

app.include_router(
    radio_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

app.include_router(
    tv_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

# =========================
# Admin (HUMAN)
# =========================

app.include_router(
    alerts_router,
    prefix="/admin",
    tags=["Admin"],
)

app.include_router(
    publish_router,
    prefix="/admin",
    tags=["Admin"],
)

# =========================
# Internal (SYSTEM / CRON)
# =========================

app.include_router(
    internal_router,
    prefix="/internal",
    tags=["Internal"],
)

app.include_router(
    weekly_router,
    prefix="/internal",
    tags=["Internal"],
)