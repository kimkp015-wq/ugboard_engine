from fastapi import FastAPI

from api.charts.top100 import build_top_100
from api.charts.trending import build_trending

app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

@app.get("/charts/top100")
def charts_top100():
    return build_top_100()

@app.get("/charts/trending")
def charts_trending():
    return build_trending()