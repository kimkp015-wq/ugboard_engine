from fastapi import FastAPI

from api.charts.top100 import router as top100_router
from api.admin.publish import router as publish_router
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

# Charts
app.include_router(top100_router, prefix="/charts")

# Admin
app.include_router(publish_router, prefix="/admin")

# Ingestion
app.include_router(youtube_router, prefix="/ingest")
app.include_router(radio_router, prefix="/ingest")
app.include_router(tv_router, prefix="/ingest")
from fastapi import FastAPI

from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

app = FastAPI()

app.include_router(top100_router)
app.include_router(trending_router)
app.include_router(regions_router)