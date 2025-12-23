# api/main.py
from fastapi import FastAPI

# =========================
# ROUTER IMPORTS (ONLY)
# =========================

# Charts
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

# Ingestion
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# Admin
from api.admin.admin import router as admin_router
from api.admin.publish import router as publish_router
from api.admin.internal import router as internal_router
from api.admin.status import router as status_router
from api.admin.health import router as health_router
from api.admin.alerts import router as alerts_router

# =========================
# CREATE APP (ONCE)
# =========================
app = FastAPI(title="UG Board Engine")

# =========================
# ROOT
# =========================
@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

# =========================
# REGISTER ROUTERS
# =========================

# Charts (read-only)
app.include_router(top100_router, prefix="/charts", tags=["Charts"])
app.include_router(trending_router, prefix="/charts", tags=["Charts"])
app.include_router(regions_router, prefix="/charts", tags=["Charts"])

# Ingestion (write)
app.include_router(youtube_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(radio_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(tv_router, prefix="/ingest", tags=["Ingestion"])

# Admin (control)
app.include_router(admin_router, prefix="/admin", tags=["Admin"])
app.include_router(publish_router, prefix="/admin", tags=["Admin"])
app.include_router(internal_router, prefix="/admin", tags=["Admin"])
app.include_router(status_router, prefix="/admin", tags=["Admin"])
app.include_router(health_router, prefix="/admin", tags=["Admin"])
app.include_router(alerts_router, prefix="/admin", tags=["Admin"])