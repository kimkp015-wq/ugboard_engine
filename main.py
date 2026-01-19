"""
UG Board Engine - Render.com Deployment Version
Simplified to ensure successful deployment
"""

from fastapi import FastAPI, HTTPException
from datetime import datetime
import os
import json
from typing import Dict, List

app = FastAPI(
    title="UG Board Engine",
    description="Ugandan Music Chart System - Render Deployment",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Simple store for initial deployment
class MusicStore:
    def __init__(self):
        self.songs = []
        self.init_sample_data()
    
    def init_sample_data(self):
        # Sample Ugandan music data
        sample_songs = [
            {"id": "1", "title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5},
            {"id": "2", "title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3},
            {"id": "3", "title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7},
            {"id": "4", "title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1},
            {"id": "5", "title": "Tonny On Low", "artist": "Gravity Omutujju", "plays": 7500, "score": 87.2},
        ]
        self.songs = sample_songs
    
    def get_top_songs(self, limit: int = 100):
        return sorted(self.songs, key=lambda x: x["score"], reverse=True)[:limit]

store = MusicStore()

@app.get("/")
async def root():
    """Root endpoint - shows service status"""
    return {
        "service": "UG Board Engine",
        "status": "online",
        "version": "1.0.0",
        "deployed": True,
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "health": "/health",
            "charts": "/charts/top100",
            "docs": "/docs",
            "ingest_tv": "/ingest/tv (POST)"
        }
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine",
        "environment": os.getenv("ENVIRONMENT", "development")
    }

@app.get("/charts/top100")
async def get_top100():
    """Get Uganda Top 100 chart"""
    top_songs = store.get_top_songs(10)
    
    # Add ranks
    for i, song in enumerate(top_songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": top_songs,
        "total_entries": len(store.songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/tv")
async def ingest_tv(data: Dict):
    """TV ingestion endpoint (simplified for deployment)"""
    try:
        items = data.get("items", [])
        
        # Simple validation
        valid_items = []
        for item in items[:10]:  # Limit to 10 items for now
            if "title" in item and "artist" in item:
                valid_items.append({
                    "id": str(len(store.songs) + 1),
                    "title": item["title"],
                    "artist": item["artist"],
                    "plays": item.get("plays", 1),
                    "score": item.get("score", 50.0),
                    "ingested_at": datetime.utcnow().isoformat(),
                    "source": "tv"
                })
        
        # Add to store
        store.songs.extend(valid_items)
        
        return {
            "status": "success",
            "message": f"Ingested {len(valid_items)} songs",
            "ingested_count": len(valid_items),
            "total_songs": len(store.songs),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Error handling
@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return {
        "error": str(exc),
        "status": "error",
        "timestamp": datetime.utcnow().isoformat(),
        "path": str(request.url.path)
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"🚀 Starting UG Board Engine on port {port}")
    print(f"🌐 Service will be available at: https://ugboard-engine.onrender.com")
    uvicorn.run(app, host="0.0.0.0", port=port)
