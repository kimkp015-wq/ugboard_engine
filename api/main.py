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
# (prevents crashes & circular imports)
# =========================

# Health
from api.admin.health import router as health_router

# Admin (READ-ONLY + INTERNAL SAFE)
from api.admin.alerts import router as alerts_router
from api.admin.internal import router as internal_router

# Charts (READ-ONLY)
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

# =========================
# Register routers
# =========================

# Health
app.include_router(
    health_router,
    tags=["Health"],
)

# Charts
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

# Admin
app.include_router(
    alerts_router,
    prefix="/admin",
    tags=["Admin"],
)

app.include_router(
    internal_router,
    prefix="/admin",
    tags=["Admin"],
)