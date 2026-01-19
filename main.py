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
# Add these routes to your existing api/main.py

@app.get("/charts/regions/{region}")
async def get_region_chart(region: str):
    """Get regional chart data"""
    valid_regions = ["ug", "eac", "afr", "ww"]
    
    if region not in valid_regions:
        raise HTTPException(
            status_code=404,
            detail=f"Region '{region}' not found. Valid regions: {valid_regions}"
        )
    
    # Mock data - replace with actual data from your store
    region_data = {
        "ug": [
            {"title": "Nalumansi", "artist": "Bobi Wine", "rank": 1, "score": 95.5},
            {"title": "Sitya Loss", "artist": "Eddy Kenzo", "rank": 2, "score": 92.3},
        ],
        "eac": [
            {"title": "Mummy", "artist": "Daddy Andre", "rank": 1, "score": 88.7},
            {"title": "Bailando", "artist": "Sheebah", "rank": 2, "score": 87.2},
        ],
        "afr": [
            {"title": "Tonny On Low", "artist": "Gravity", "rank": 1, "score": 85.4},
        ],
        "ww": [
            {"title": "Zenjye", "artist": "John Blaq", "rank": 1, "score": 83.1},
        ]
    }
    
    return {
        "region": region,
        "name": {
            "ug": "Uganda Top 100",
            "eac": "East African Community",
            "afr": "Africa Chart",
            "ww": "Worldwide (Diaspora)"
        }.get(region, "Unknown"),
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": region_data.get(region, []),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending")
async def get_trending():
    """Get trending songs (last 24 hours)"""
    return {
        "chart": "Trending Now",
        "period": "24 hours",
        "entries": [
            {"title": "Nalumansi", "artist": "Bobi Wine", "trend_score": 95.5, "change": "up"},
            {"title": "Sitya Loss", "artist": "Eddy Kenzo", "trend_score": 92.3, "change": "up"},
            {"title": "Mummy", "artist": "Daddy Andre", "trend_score": 88.7, "change": "same"},
        ],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/artists/{artist_name}/stats")
async def get_artist_stats(artist_name: str):
    """Get statistics for a specific artist"""
    # Mock data - replace with actual database query
    artist_stats = {
        "bobi wine": {
            "name": "Bobi Wine",
            "total_songs": 15,
            "total_plays": 150000,
            "highest_chart_position": 1,
            "weeks_on_chart": 52,
            "collaborations": ["Eddy Kenzo", "Daddy Andre"]
        },
        "eddy kenzo": {
            "name": "Eddy Kenzo", 
            "total_songs": 12,
            "total_plays": 120000,
            "highest_chart_position": 1,
            "weeks_on_chart": 48,
            "collaborations": ["Bobi Wine", "Niniola"]
        }
    }
    
    stats = artist_stats.get(artist_name.lower())
    if not stats:
        raise HTTPException(status_code=404, detail=f"Artist '{artist_name}' not found")
    
    return {
        "artist": stats,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/radio")
async def ingest_radio(data: dict, authorization: str = Header(None)):
    """Radio ingestion endpoint"""
    # Verify token
    expected_token = f"Bearer {os.getenv('INGEST_TOKEN')}"
    if authorization != expected_token:
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    
    items = data.get("items", [])
    
    # Validate Ugandan music rules
    valid_items = []
    for item in items:
        if "title" in item and "artist" in item:
            # Add validation logic here
            valid_items.append(item)
    
    return {
        "status": "success",
        "source": "radio",
        "ingested": len(valid_items),
        "timestamp": datetime.utcnow().isoformat(),
        "validation": {
            "total_received": len(items),
            "valid": len(valid_items),
            "invalid": len(items) - len(valid_items)
        }
    }        

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
