from fastapi import FastAPI

# existing routers
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router
from api.charts.boost import router as boost_router

from api.admin.admin import router as admin_router
from api.admin.publish import router as publish_router
from api.admin.reset import router as reset_router

app = FastAPI(title="UgBoard Engine")

# ---- include routers ----
app.include_router(top100_router)
app.include_router(trending_router)
app.include_router(regions_router)
app.include_router(boost_router)

app.include_router(admin_router)
app.include_router(publish_router)
app.include_router(reset_router)