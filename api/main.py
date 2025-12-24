import os
from fastapi import FastAPI

# =========================
# Environment
# =========================
ENV = os.getenv("ENV", "development")

# =========================
# Create app FIRST
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
# Import routers (verified)
# =========================

# Charts (PUBLIC)
from api.charts.top100 import router as top100_router
from api.charts.index import router as index_router

# Ingestion
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# Admin
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