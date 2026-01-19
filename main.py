# main.py - AT ROOT LEVEL (not in api/)
"""
UG Board Engine - Render.com Deployment
Root level to fix import issues
"""

from fastapi import FastAPI, HTTPException
from datetime import datetime
import os
import json
from typing import Dict, List

app = FastAPI(
    title="UG Board Engine",
    description="Ugandan Music Chart System",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Simple store
class MusicStore:
    def __init__(self):
        self.songs = [
            {"id": "1", "title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5},
            {"id": "2", "title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3},
            {"id": "3", "title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7},
            {"id": "4", "title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1},
        ]
    
    def get_top_songs(self, limit: int = 100):
        return sorted(self.songs, key=lambda x: x["score"], reverse=True)[:limit]

store = MusicStore()

@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "status": "online",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "health": "/health",
            "charts": "/charts/top100",
            "docs": "/docs",
            "ingest": "/ingest/tv (POST)"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com"
    }

@app.get("/charts/top100")
async def get_top100():
    songs = store.get_top_songs(10)
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/tv")
async def ingest_tv(data: Dict):
    try:
        items = data.get("items", [])
        count = min(len(items), 5)  # Limit to 5
        
        return {
            "status": "success",
            "message": f"Ready to ingest {count} items",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
