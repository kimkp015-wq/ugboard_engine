from fastapi import FastAPI

from api.charts import top100
from api.charts import trending
from api.charts import regions

app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

# Charts
app.include_router(top100.router, prefix="/charts")
app.include_router(trending.router, prefix="/charts")
app.include_router(regions.router, prefix="/charts")