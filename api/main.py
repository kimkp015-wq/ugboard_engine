from api.db import engine
from api.models.song import Base
from fastapi import FastAPI
from api.charts import top100, trending, regions, boost

app = FastAPI(title="UG Board Engine")
Base.metadata.create_all(bind=engine)

# Root health check
@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

# Charts routes
app.include_router(top100.router, prefix="/charts", tags=["Charts"])
app.include_router(trending.router, prefix="/charts", tags=["Charts"])
app.include_router(regions.router, prefix="/charts", tags=["Charts"])
app.include_router(boost.router, prefix="/charts", tags=["Charts"])