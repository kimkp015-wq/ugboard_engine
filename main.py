# main.py - AT ROOT LEVEL (not in api/)
"""
UG Board Engine - Render.com Deployment
Root level to fix import issues
"""

# Add these imports if not already there
from fastapi import HTTPException, Query
from typing import List, Optional
# Add these imports if not already there
from typing import Optional, List
from pydantic import BaseModel

# Define request models
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    station: Optional[str] = None

class TVIngestionPayload(BaseModel):
    items: List[SongItem]
    source: str
    timestamp: Optional[str] = None

# Add these endpoints
@app.get("/charts/top100")
async def get_top100(limit: int = 100):
    """Get Uganda Top 100 chart"""
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": [],
        "total_entries": 0,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/regions/{region}")
async def get_region_chart(region: str):
    """Get regional chart"""
    valid_regions = ["ug", "eac", "afr", "ww"]
    
    if region not in valid_regions:
        raise HTTPException(
            status_code=404,
            detail=f"Invalid region. Valid: {', '.join(valid_regions)}"
        )
    
    return {
        "region": region,
        "name": {
            "ug": "Uganda",
            "eac": "East African Community",
            "afr": "Africa",
            "ww": "Worldwide"
        }.get(region),
        "entries": [],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending")
async def get_trending():
    """Get trending songs"""
    return {
        "chart": "Trending Now",
        "period": "24 hours",
        "entries": [],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/tv")
async def ingest_tv(payload: TVIngestionPayload, authorization: Optional[str] = Header(None)):
    """TV ingestion endpoint with authentication"""
    # Verify token
    expected_token = f"Bearer {os.getenv('INGEST_TOKEN')}"
    if authorization != expected_token:
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    
    # Process items
    valid_items = []
    for item in payload.items:
        # Validate Ugandan music rules here
        valid_items.append(item.dict())
    
    return {
        "status": "success",
        "source": payload.source,
        "ingested": len(valid_items),
        "timestamp": datetime.utcnow().isoformat()
    }
    
# Add these routes after your existing routes

@app.get("/charts/top100")
async def get_top100(limit: int = Query(100, ge=1, le=100)):
    """Get Uganda Top 100 chart"""
    # This would come from your database
    # For now, return sample data
    sample_chart = [
        {"rank": 1, "title": "Nalumansi", "artist": "Bobi Wine", "score": 95.5, "plays": 10000, "change": "up"},
        {"rank": 2, "title": "Sitya Loss", "artist": "Eddy Kenzo", "score": 92.3, "plays": 8500, "change": "same"},
        {"rank": 3, "title": "Mummy", "artist": "Daddy Andre", "score": 88.7, "plays": 7800, "change": "down"},
        {"rank": 4, "title": "Bailando", "artist": "Sheebah Karungi", "score": 94.1, "plays": 9200, "change": "up"},
        {"rank": 5, "title": "Tonny On Low", "artist": "Gravity Omutujju", "score": 87.2, "plays": 7500, "change": "new"},
    ]
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": sample_chart[:limit],
        "total_entries": 100,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/regions/{region}")
async def get_region_chart(region: str = "ug"):
    """Get regional chart"""
    valid_regions = ["ug", "eac", "afr", "ww"]
    
    if region not in valid_regions:
        raise HTTPException(
            status_code=404,
            detail=f"Invalid region. Must be one of: {', '.join(valid_regions)}"
        )
    
    region_names = {
        "ug": "Uganda",
        "eac": "East African Community", 
        "afr": "Africa",
        "ww": "Worldwide (Diaspora)"
    }
    
    # Sample data - replace with actual data
    region_data = {
        "rank": 1,
        "title": "Sample Song",
        "artist": "Sample Artist",
        "region": region,
        "score": 85.0
    }
    
    return {
        "region": region,
        "region_name": region_names.get(region, "Unknown"),
        "chart_name": f"{region_names.get(region, 'Regional')} Chart",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": [region_data],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending")
async def get_trending(limit: int = Query(10, ge=1, le=50)):
    """Get trending songs (last 24 hours)"""
    trending = [
        {"title": "Nalumansi", "artist": "Bobi Wine", "trend_score": 95.5, "change": "+2", "velocity": "rising"},
        {"title": "Sitya Loss", "artist": "Eddy Kenzo", "trend_score": 92.3, "change": "+1", "velocity": "rising"},
        {"title": "Bailando", "artist": "Sheebah Karungi", "trend_score": 94.1, "change": "+3", "velocity": "rising"},
        {"title": "Mummy", "artist": "Daddy Andre", "trend_score": 88.7, "change": "-1", "velocity": "falling"},
    ]
    
    return {
        "chart": "Trending Now",
        "period": "24 hours",
        "entries": trending[:limit],
        "updated": datetime.utcnow().isoformat()
    }

@app.get("/artists/stats")
async def get_artist_stats(artist: Optional[str] = None):
    """Get artist statistics"""
    if artist:
        # Return specific artist stats
        return {
            "artist": artist,
            "total_songs": 15,
            "total_plays": 150000,
            "highest_chart_position": 1,
            "weeks_on_chart": 52,
            "timestamp": datetime.utcnow().isoformat()
        }
    else:
        # Return overall stats
        return {
            "total_artists": 20,
            "ugandan_artists": 18,
            "foreign_collaborators": 2,
            "collaboration_rate": "25%",
            "top_artists": [
                {"name": "Bobi Wine", "chart_entries": 5, "total_plays": 50000},
                {"name": "Eddy Kenzo", "chart_entries": 4, "total_plays": 45000},
                {"name": "Sheebah Karungi", "chart_entries": 3, "total_plays": 42000},
            ],
            "timestamp": datetime.utcnow().isoformat()
        }

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
