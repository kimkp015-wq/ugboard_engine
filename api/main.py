from fastapi import FastAPI

app = FastAPI()
from api.charts.top import router as top_router
app.include_router(top_router)
@app.get("/")
def root():
    return {
        "status": "ok",
        "engine": "ugboard",
        "message": "UG Board engine running"
    }