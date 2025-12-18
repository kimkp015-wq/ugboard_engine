from fastapi import FastAPI
from api.ingestion.youtube import fetch_ugandan_music
from api.charts.top100 import build_top_100

app = FastAPI()


@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}


@app.get("/ingest/youtube")
def ingest_youtube():
    return fetch_ugandan_music()


@app.get("/charts/top100")
def charts_top_100():
    return build_top_100()