from fastapi import FastAPI

# Create app FIRST
app = FastAPI(title="UG Board Engine")

# Root health check
@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}


# =========================
# Charts (READ-ONLY)
# =========================
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router


# =========================
# Ingestion (WRITE)
# =========================
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router


# =========================
# Admin
# =========================
from api.admin.admin import router as admin_router
from api.admin.publish import router as publish_router
from api.admin.regions_publish import router as regions_publish_router


# =========================
# Router registration
# =========================
app.include_router(top100_router, prefix="/charts", tags=["Charts"])
app.include_router(trending_router, prefix="/charts", tags=["Charts"])
app.include_router(regions_router, prefix="/charts", tags=["Charts"])

app.include_router(youtube_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(radio_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(tv_router, prefix="/ingest", tags=["Ingestion"])

app.include_router(admin_router, prefix="/admin", tags=["Admin"])
app.include_router(publish_router, prefix="/admin", tags=["Admin"])
app.include_router(regions_publish_router, prefix="/admin", tags=["Admin"])