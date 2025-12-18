from fastapi import FastAPI
from datetime import datetime

app = FastAPI(
    title="The UG Board",
    description="Uganda music charts engine",
    version="0.1.0"
)

@app.get("/")
def home():
    return {
        "engine": "UG Board",
        "status": "running",
        "time": datetime.utcnow()
    }

@app.get("/charts/top100")
def top_100():
    return {
        "chart": "Uganda Top 100",
        "status": "collecting data"
    }

@app.get("/health")
def health():
    return {"ok": True}