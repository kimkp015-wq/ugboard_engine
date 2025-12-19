from fastapi import FastAPI

from api.charts import top100, trending, regions
from api.admin import admin, publish
from api.ingestion import radio, youtube, tv

app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

app.include_router(top100.router, prefix="/charts")
app.include_router(trending.router, prefix="/charts")
app.include_router(regions.router, prefix="/charts")

app.include_router(admin.router, prefix="/admin")
app.include_router(publish.router, prefix="/admin")

app.include_router(radio.router, prefix="/ingestion")
app.include_router(youtube.router, prefix="/ingestion")
app.include_router(tv.router, prefix="/ingestion")