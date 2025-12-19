from fastapi import FastAPI

# Admin
from api.admin import admin, publish

# Charts
from api.charts import top100, trending, regions, boost

# Ingestion (IMPORTANT: folder is ingestion)
from api.ingestion import radio, youtube

app = FastAPI(
    title="UG Board Engine",
    version="1.0.0"
)

# ---- ADMIN ROUTES ----
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(publish.router, prefix="/admin", tags=["Admin Publish"])

# ---- CHART ROUTES ----
app.include_router(top100.router, prefix="/charts", tags=["Charts"])
app.include_router(trending.router, prefix="/charts", tags=["Charts"])
app.include_router(regions.router, prefix="/charts", tags=["Charts"])
app.include_router(boost.router, prefix="/charts", tags=["Charts"])

# ---- INGESTION ROUTES ----
app.include_router(radio.router, tags=["Ingestion"])
app.include_router(youtube.router, tags=["Ingestion"])


@app.get("/")
def root():
    return {
        "engine": "UG Board",
        "status": "running"
    }