from fastapi import FastAPI
from api.ingestion.youtube import fetch_ugandan_music

app = FastAPI(title="The UG Board")

@app.get("/")
def home():
    return {
        "engine": "UG Board",
        "status": "running"
    }

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/ingest/youtube")
def ingest_youtube():
    return fetch_ugandan_music(10)