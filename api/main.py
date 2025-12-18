from fastapi import FastAPI

from api.charts.top100 import build_top_100
from api.charts.trending import build_trending
from api.charts.regions import build_regions

app = FastAPI(title="UG Board Engine")

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

@app.get("/charts/top100")
def top_100():
    return build_top_100()

@app.get("/charts/trending")
def trending():
    return build_trending()

@app.get("/charts/regions")
def regions():
    return build_regions()