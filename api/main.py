from fastapi import FastAPI

# import routers
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router
from api.charts.boost import router as boost_router

app = FastAPI(title="UG Board Engine")

# root test
@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

# include routes
app.include_router(top100_router, prefix="/charts")
app.include_router(trending_router, prefix="/charts")
app.include_router(regions_router, prefix="/charts")
app.include_router(boost_router, prefix="/charts")