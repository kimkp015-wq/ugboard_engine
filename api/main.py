from fastapi import FastAPI

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

app = FastAPI(title="UG Board Engine")

# Charts
app.include_router(top100_router)
app.include_router(trending_router)
app.include_router(regions_router)

# Ingestion
app.include_router(youtube_router)
app.include_router(radio_router)
app.include_router(tv_router)

# Admin
app.include_router(admin_router)
app.include_router(publish_router)


@app.get("/")
def root():
    return {
        "status": "ok",
        "engine": "ugboard"
    }