# api/main.py - SIMPLIFIED WORKING VERSION
from fastapi import FastAPI, HTTPException, Depends, Header
from datetime import datetime
import os
import json
from typing import Optional, List, Dict
import hashlib

app = FastAPI(
    title="UG Board Engine",
    description="Ugandan Music Chart System",
    version="1.0.0"
)

# Simple in-memory store (will be replaced with your actual store)
class UgandanMusicStore:
    def __init__(self):
        self.songs = []
        self.ingestions = []
    
    def add_song(self, title: str, artist: str, **kwargs):
        song_id = hashlib.md5(f"{title}{artist}".encode()).hexdigest()[:8]
        song = {
            "id": song_id,
            "title": title,
            "artist": artist,
            "added_at": datetime.utcnow().isoformat(),
            **kwargs
        }
        self.songs.append(song)
        return song
    
    def get_top_songs(self, limit: int = 100):
        # Simple scoring
        return sorted(
            self.songs,
            key=lambda x: (x.get("score", 0), x.get("plays", 0)),
            reverse=True
        )[:limit]

store = UgandanMusicStore()

# Pre-populate with some Ugandan songs
def initialize_store():
    ugandan_songs = [
        {"title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5},
        {"title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3},
        {"title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7},
        {"title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1},
    ]
    
    for song in ugandan_songs:
        store.add_song(**song)

initialize_store()

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "UG Board Engine",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "health": "/health",
            "charts": "/charts/top100",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "songs_in_store": len(store.songs),
        "version": "1.0.0"
    }

@app.get("/charts/top100")
async def get_top100():
    top_songs = store.get_top_songs(10)
    
    for i, song in enumerate(top_songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": top_songs,
        "total": len(store.songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/tv")
async def ingest_tv(data: Dict):
    """Simple TV ingestion endpoint"""
    try:
        items = data.get("items", [])
        ingested = []
        
        for item in items:
            if "title" in item and "artist" in item:
                song = store.add_song(
                    title=item["title"],
                    artist=item["artist"],
                    plays=item.get("plays", 1),
                    score=item.get("score", 50.0),
                    source="tv",
                    station=data.get("source", "unknown")
                )
                ingested.append(song)
        
        return {
            "status": "success",
            "ingested": len(ingested),
            "message": f"Successfully ingested {len(ingested)} songs",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"🚀 Starting UG Board Engine on port {port}")
    print(f"📊 API Documentation: http://localhost:{port}/docs")
    uvicorn.run(app, host="0.0.0.0", port=port)
