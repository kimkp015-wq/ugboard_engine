from fastapi import FastAPI

# Charts
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

# Admin
from api.admin.admin import router as admin_router
from api.admin.publish import router as publish_router

app = FastAPI(title="UG Board Engine")


@app.get("/")
def root():
    return {
        "status": "ok",
        "engine": "ugboard"
    }


# Charts routes
app.include_router(top100_router)
app.include_router(trending_router)
app.include_router(regions_router)

# Admin routes
app.include_router(admin_router, prefix="/admin")
app.include_router(publish_router, prefix="/admin")