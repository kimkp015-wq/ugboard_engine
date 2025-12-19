from api.admin.publish import router as admin_router
from fastapi import FastAPI
from .charts.top100 import router as top100_router
from .admin.publish import router as admin_router

app = FastAPI(title="UG Board Engine")
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

app.include_router(top100_router, prefix="/charts")
app.include_router(trending_router, prefix="/charts")
app.include_router(regions_router, prefix="/charts")
app.include_router(top100_router)
app.include_router(admin_router)

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}