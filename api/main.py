from fastapi import FastAPI
from api.charts import top100

app = FastAPI(title="UG Board Engine")

# root
@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

# Top 100
app.include_router(top100.router, prefix="/charts/top100", tags=["Top 100"])