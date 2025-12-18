from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}

@app.get("/ingest/youtube")
def ingest_youtube():
    return {
        "status": "ok",
        "message": "YouTube ingestion disabled (free mode)"
    }

@app.get("/charts/top100")
def charts_top100():
    return {
        "status": "ok",
        "charts": []
    }