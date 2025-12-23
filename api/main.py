from fastapi import FastAPI

# =========================
# Create app FIRST (critical)
# =========================
app = FastAPI(
    title="UG Board Engine",
    docs_url="/docs",
    redoc_url="/redoc",
)

# =========================
# Root health check
# (Railway + Cloudflare depend on this)
# =========================
@app.get("/", tags=["Health"])
def root():
    return {
        "status": "ok",
        "engine": "ugboard",
    }

# =========================
# Import routers AFTER app exists
# (prevents NameError & circular imports)
# =========================

# Admin
from api.admin.health import router as health_router
from api.admin.internal import router as internal_router
from api.admin.publish import router as publish_router
from api.admin.alerts import router as alerts_router

# Charts (READ-ONLY)
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

# Ingestion (WRITE)
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# =========================
# Register routers
# =========================

# Health
app.include_router(health_router, tags=["Health"])

# Charts
app.include_router(top100_router, prefix="/charts", tags=["Charts"])
app.include_router(trending_router, prefix="/charts", tags=["Charts"])
app.include_router(regions_router, prefix="/charts", tags=["Charts"])

# Ingestion
app.include_router(youtube_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(radio_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(tv_router, prefix="/ingest", tags=["Ingestion"])

# Admin
app.include_router(internal_router, prefix="/admin", tags=["Admin"])
app.include_router(publish_router, prefix="/admin", tags=["Admin"])
app.include_router(alerts_router, prefix="/admin", tags=["Admin"])
    from fastapi import FastAPI

app = FastAPI(
    title="UG Board Engine",
    docs_url="/docs",
    redoc_url="/redoc",
)

@app.get("/", tags=["Health"])
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online",
        "environment": "production",
        "docs": "/docs",
        "redoc": "/redoc"
    }