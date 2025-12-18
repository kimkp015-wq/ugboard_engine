from fastapi import FastAPI
from api.ingestion.youtube import fetch_ugandan_music
import os
import uvicorn

app = FastAPI(title="The UG Board")

@app.get("/")
def home():
    return {"engine": "UG Board", "status": "running"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/ingest/youtube")
def ingest_youtube():
    return fetch_ugandan_music(10)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)