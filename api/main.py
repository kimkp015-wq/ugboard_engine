import os
from fastapi import FastAPI

ENV = os.getenv("ENV", "development")

# =========================
# Create app
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

# Admin
from api.admin.alerts import router as alerts_router
from api.admin.internal import router as internal_router

# =========================
# Register routers
# =========================

# Health
app.include_router(health_router, tags=["Health"])

# Admin
app.include_router(alerts_router, prefix="/admin", tags=["Admin"])
app.include_router(internal_router, prefix="/admin", tags=["Admin"])

from api.charts.top100 import router as top100_router
app.include_router(
    top100_router,
    prefix="/charts",
    tags=["Charts"]
)