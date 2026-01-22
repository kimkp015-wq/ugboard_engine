"""
UG Board Engine - Simplified Production Version
"""
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request, status
from fastapi.security import HTTPBearer
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Simple configuration
class Config:
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
    ENV = os.getenv("ENV", "production")

config = Config()

# Models
class SongItem(BaseModel):
    title: str
    artist: str
    plays: int = 0
    score: float = 0.0
    region: str = "central"
    station: Optional[str] = None

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str

# FastAPI App
app = FastAPI(title="UG Board Engine", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"])

# Database
songs_db = []

# Authentication
security = HTTPBearer()

def verify_youtube(credentials = Depends(security)):
    if credentials.credentials != config.YOUTUBE_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return True

# Endpoints
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "status": "online",
        "environment": config.ENV,
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": ["/health", "/charts/top100", "/ingest/youtube"]
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": {"songs": len(songs_db), "regions": 4},
        "environment": config.ENV
    }

@app.get("/charts/top100")
async def get_top100(limit: int = Query(100, ge=1, le=200)):
    sorted_songs = sorted(songs_db, key=lambda x: x.get("score", 0), reverse=True)[:limit]
    for i, song in enumerate(sorted_songs, 1):
        song["rank"] = i
    return {"chart": "Uganda Top 100", "entries": sorted_songs}

@app.post("/ingest/youtube")
async def ingest_youtube(payload: IngestPayload, auth: bool = Depends(verify_youtube)):
    added = 0
    for item in payload.items:
        song = item.dict()
        song["source"] = f"youtube_{payload.source}"
        song["ingested_at"] = datetime.utcnow().isoformat()
        song["id"] = f"song_{len(songs_db) + 1}"
        songs_db.append(song)
        added += 1
    
    return {
        "status": "success",
        "message": f"Ingested {added} songs",
        "source": payload.source,
        "total_songs": len(songs_db)
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
