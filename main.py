# api/main_313.py - Python 3.13 Compatible
"""
UG Board Engine - Python 3.13 Emergency Version
No pandas, no Cython dependencies
"""

from fastapi import FastAPI, HTTPException
from datetime import datetime
import json
import os
from pathlib import Path
import hashlib
from typing import List, Dict, Optional

app = FastAPI(
    title="UG Board Engine - Python 3.13 Emergency",
    description="Running without pandas due to Python 3.13 compatibility issues",
    version="1.0.0"
)

# Simple in-memory store
class UgandanMusicStore:
    def __init__(self):
        self.songs = []
        self.artists = set()
        self.ingestions = []
    
    def add_song(self, title: str, artist: str, **kwargs):
        song_id = hashlib.md5(f"{title}{artist}".encode()).hexdigest()[:8]
        
        # Simple Ugandan artist check
        ugandan_artists = {"bobi wine", "eddy kenzo", "sheebah", "daddy andre"}
        is_ugandan = any(ug_artist in artist.lower() for ug_artist in ugandan_artists)
        
        song = {
            "id": song_id,
            "title": title,
            "artist": artist,
            "is_ugandan": is_ugandan,
            "added_at": datetime.utcnow().isoformat(),
            "plays": kwargs.get("plays", 0),
            "score": kwargs.get("score", 0.0),
            **kwargs
        }
        
        self.songs.append(song)
        self.artists.add(artist)
        return song
    
    def get_top_songs(self, limit: int = 100):
        # Simple scoring: more plays = higher rank
        return sorted(
            self.songs,
            key=lambda x: (x.get("score", 0), x.get("plays", 0)),
            reverse=True
        )[:limit]

store = UgandanMusicStore()

@app.get("/")
async def root():
    return {
        "status": "online",
        "python_version": "3.13",
        "mode": "emergency_no_pandas",
        "message": "Engine running without pandas due to Python 3.13 compatibility",
        "timestamp": datetime.utcnow().isoformat(),
        "stats": {
            "total_songs": len(store.songs),
            "unique_artists": len(store.artists),
            "ugandan_songs": sum(1 for s in store.songs if s.get("is_ugandan"))
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {
            "pandas": "disabled (Python 3.13 incompatible)",
            "numba": "disabled",
            "asyncpg": "disabled"
        }
    }

@app.post("/ingest/tv")
async def ingest_tv(data: Dict):
    """Simple TV ingestion without complex validation"""
    items = data.get("items", [])
    
    ingested = []
    for item in items[:50]:  # Limit to 50 items
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
        "total_songs": len(store.songs),
        "note": "Running in Python 3.13 compatibility mode"
    }

@app.get("/charts/top100")
async def get_top100():
    top_songs = store.get_top_songs(20)  # Get top 20 for now
    
    # Add ranks
    for i, song in enumerate(top_songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100 (Emergency Mode)",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": top_songs,
        "total_songs": len(store.songs),
        "python_version": "3.13",
        "limitations": [
            "No pandas for data processing",
            "Simple in-memory storage",
            "Basic Ugandan artist detection"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    print("🚀 UG Board Engine - Python 3.13 Emergency Mode")
    print("⚠️  Running WITHOUT pandas (incompatible with Python 3.13)")
    print("📡 API available at http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
