from fastapi import FastAPI

from api.admin.admin import router as admin_router
from api.admin.publish import router as publish_router

from api.charts.trending import router as trending_router
from api.charts.top100 import router as top100_router

from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

app = FastAPI(title="UGBoard Engine")

# Admin
app.include_router(admin_router, prefix="/admin", tags=["admin"])
app.include_router(publish_router, prefix="/admin", tags=["admin"])

# Charts
app.include_router(trending_router, prefix="/charts", tags=["charts"])
app.include_router(top100_router, prefix="/charts", tags=["charts"])

# Ingestion
app.include_router(youtube_router, prefix="/ingest", tags=["ingestion"])
app.include_router(radio_router, prefix="/ingest", tags=["ingestion"])
app.include_router(tv_router, prefix="/ingest", tags=["ingestion"])


@app.get("/")
def health_check():
    return {"status": "ok"}