from fastapi import FastAPI

from api.charts.top import router as top_router

app.include_router(top_router)
from api.ingestion.youtube import router as youtube_router
app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
app.include_router(youtube_router)
    return {"status": "ok"} 