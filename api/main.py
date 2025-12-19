from fastapi import FastAPI

from api.charts import top100, trending, regions
from api.admin import admin, publish

app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
    return {
        "status": "ok",
        "engine": "ugboard"
    }

# Charts routes
app.include_router(top100.router, prefix="/charts")
app.include_router(trending.router, prefix="/charts")
app.include_router(regions.router, prefix="/charts")

# Admin routes
app.include_router(admin.router, prefix="/admin")
app.include_router(publish.router, prefix="/admin")