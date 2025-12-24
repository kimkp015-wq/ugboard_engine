# api/main.py

import os
from fastapi import FastAPI

# =========================
# Environment
# =========================

ENV = os.getenv("ENV", "development")
IS_PROD = ENV == "production"

# =========================
# Create app FIRST (critical)
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
    Prevents runtime ImportError crashes.
    """
    # chart week contract
    import data.chart_week as chart_week

    required_chart_week = [
        "get_current_week_id",
        "current_chart_week",
        "close_tracking_week",
        "open_new_tracking_week",
    ]

    for name in required_chart_week:
        if not hasattr(chart_week, name):
            raise RuntimeError(
                f"Engine startup failed: data.chart_week.{name} missing"
            )

    # index contract
    import data.index as index

    for name in ("get_index", "record_week_publish", "week_already_published"):
        if not hasattr(index, name):
            raise RuntimeError(
                f"Engine startup failed: data.index.{name} missing"
            )


_validate_engine_contracts()

# =========================
# Import routers AFTER validation
# =========================

# Charts (PUBLIC / READ-ONLY)
from api.charts.top100 import router as top100_router
from api.charts.index import router as index_router

# Ingestion (WRITE)
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# Admin (HUMAN-triggered)
from api.admin.publish import router as publish_router

# =========================
# Register routers
# =========================

# Charts
app.include_router(
    top100_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    index_router,
    prefix="/charts",
    tags=["Charts"],
)

# Ingestion
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

# Admin
app.include_router(
    publish_router,
    prefix="/admin",
    tags=["Admin"],
)