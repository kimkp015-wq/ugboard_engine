# api/main.py

import os
from fastapi import FastAPI

# =========================
# Environment
# =========================

ENV = os.getenv("ENV", "development")
IS_PROD = ENV == "production"

# =========================
# Create app FIRST
# =========================

app = FastAPI(
    title="UG Board Engine",
    docs_url=None if IS_PROD else "/docs",
    redoc_url=None if IS_PROD else "/redoc",
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
        "docs_enabled": not IS_PROD,
    }

# =========================
# Startup contract checks
# =========================

def _validate_engine_contracts() -> None:
    """
    Hard fail early if core engine contracts are missing.
    Prevents silent runtime corruption.
    """
    import data.chart_week as chart_week

    for name in (
        "get_current_week_id",
        "current_chart_week",
        "close_tracking_week",
        "open_new_tracking_week",
    ):
        if not hasattr(chart_week, name):
            raise RuntimeError(
                f"Engine startup failed: data.chart_week.{name} missing"
            )

    import data.index as index

    for name in (
        "get_index",
        "record_week_publish",
        "week_already_published",
    ):
        if not hasattr(index, name):
            raise RuntimeError(
                f"Engine startup failed: data.index.{name} missing"
            )

_validate_engine_contracts()

# =========================
# Import routers (AFTER validation)
# =========================

# Charts (PUBLIC / READ-ONLY)
from api.charts.top100 import router as top100_router
from api.charts.index import router as index_router
from api.charts.regions import router as regions_router
from api.charts.trending import router as trending_router

# Ingestion (WRITE)
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# Admin
from api.admin.publish import router as publish_router
from api.admin.index import router as admin_index_router
from api.admin.health import router as admin_health_router
from api.admin.regions import router as admin_regions_router  # expected surface

# =========================
# Register routers
# =========================

# Charts (single source of truth)
app.include_router(top100_router, prefix="/charts", tags=["Charts"])
app.include_router(index_router, prefix="/charts", tags=["Charts"])
app.include_router(regions_router, prefix="/charts", tags=["Charts"])
app.include_router(trending_router, prefix="/charts", tags=["Charts"])

# Ingestion
app.include_router(youtube_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(radio_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(tv_router, prefix="/ingest", tags=["Ingestion"])

# Admin
app.include_router(publish_router, prefix="/admin", tags=["Admin"])
app.include_router(admin_index_router, prefix="/admin", tags=["Admin"])
app.include_router(admin_health_router, prefix="/admin", tags=["Health"])
app.include_router(admin_regions_router, prefix="/admin", tags=["Admin"])