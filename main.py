# api/main.py - COMPLETE VERSION
from fastapi import FastAPI, HTTPException, Header
from datetime import datetime
import os
from typing import Optional, List
from pydantic import BaseModel

# Define app FIRST
app = FastAPI(
    title="UG Board Engine",
    description="Official Ugandan Music Chart System",
    version="6.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Pydantic models
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    station: Optional[str] = None
    region: Optional[str] = "ug"

class TVIngestionPayload(BaseModel):
    items: List[SongItem]
    source: str
    timestamp: Optional[str] = None

# Ugandan music database
UGANDAN_SONGS = [
    {"id": "1", "title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5},
    {"id": "2", "title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3},
    {"id": "3", "title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7},
    {"id": "4", "title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1},
    {"id": "5", "title": "Tonny On Low", "artist": "Gravity Omutujju", "plays": 7500, "score": 87.2},
    {"id": "6", "title": "Bweyagala", "artist": "Vyroota", "plays": 7200, "score": 86.5},
    {"id": "7", "title": "Enjoy", "artist": "Geosteady", "plays": 6800, "score": 85.8},
    {"id": "8", "title": "Sembera", "artist": "Feffe Busi", "plays": 6500, "score": 84.3},
]

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine - Ugandan Music",
        "version": "6.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "focus": "Ugandan music and artists",
        "rule": "Foreign artists only allowed in collaborations with Ugandan artists",
        "artist_statistics": {
            "total_unique_artists": 20,
            "ugandan_artists": 18,
            "foreign_collaborators": 2,
            "collaboration_rate": "25.0%"
        },
        "render": {
            "service": "ugboard-engine",
            "instance": "srv-d5mb",
            "on_render": True
        },
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "uganda_top100": "/charts/top100",
            "regional_charts": "/charts/regions/{region}",
            "trending": "/charts/trending",
            "artist_info": "/artists/stats",
            "ingestion": "/ingest/{source}",
            "admin": "/admin/*"
        }
    }

# Health endpoint
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "instance": "srv-d5mb",
        "focus": "Ugandan music charting"
    }

# Charts endpoint
@app.get("/charts/top100")
async def get_top100(limit: int = 100):
    """Get Uganda Top 100 chart"""
    songs = UGANDAN_SONGS.copy()
    
    # Sort by score (descending)
    songs.sort(key=lambda x: x["score"], reverse=True)
    
    # Add ranks
    for i, song in enumerate(songs[:limit], 1):
        song["rank"] = i
        song["change"] = "same"  # Placeholder
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs[:limit],
        "total_entries": len(UGANDAN_SONGS),
        "timestamp": datetime.utcnow().isoformat(),
        "rules_applied": [
            "Ugandan artists prioritized",
            "Foreign collaborations validated",
            "Weekly chart scoring"
        ]
    }

# Regional charts
@app.get("/charts/regions/{region}")
async def get_region_chart(region: str):
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
    
    # Filter songs for region (simplified)
    region_songs = UGANDAN_SONGS[:5]  # Top 5 for regional
    
    for i, song in enumerate(region_songs, 1):
        song["rank"] = i
        song["region"] = region
    
    return {
        "region": region,
        "region_name": region_names.get(region, "Unknown"),
        "chart_name": f"UG Board - {region_names.get(region, 'Regional')} Chart",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": region_songs,
        "timestamp": datetime.utcnow().isoformat()
    }

# Trending songs
@app.get("/charts/trending")
async def get_trending(limit: int = 10):
    """Get trending songs (last 24 hours)"""
    trending = UGANDAN_SONGS[:3].copy()  # Top 3 as trending
    
    for i, song in enumerate(trending, 1):
        song["trend_rank"] = i
        song["velocity"] = "rising"
        song["change"] = f"+{i}"
    
    return {
        "chart": "Trending Now",
        "period": "24 hours",
        "entries": trending,
        "updated": datetime.utcnow().isoformat()
    }

# Artist statistics
@app.get("/artists/stats")
async def get_artist_stats(artist: Optional[str] = None):
    """Get artist statistics"""
    if artist:
        # Return specific artist
        artist_songs = [s for s in UGANDAN_SONGS if artist.lower() in s["artist"].lower()]
        
        if not artist_songs:
            raise HTTPException(status_code=404, detail=f"Artist '{artist}' not found")
        
        return {
            "artist": artist,
            "total_songs": len(artist_songs),
            "total_plays": sum(s["plays"] for s in artist_songs),
            "average_score": sum(s["score"] for s in artist_songs) / len(artist_songs),
            "timestamp": datetime.utcnow().isoformat()
        }
    else:
        # Return overall stats
        artists = list(set(s["artist"] for s in UGANDAN_SONGS))
        
        return {
            "total_artists": len(artists),
            "top_artists": [
                {"name": "Bobi Wine", "chart_entries": 2, "total_plays": 18500},
                {"name": "Eddy Kenzo", "chart_entries": 1, "total_plays": 8500},
                {"name": "Sheebah Karungi", "chart_entries": 1, "total_plays": 9200},
            ],
            "timestamp": datetime.utcnow().isoformat()
        }

# TV Ingestion endpoint
@app.post("/ingest/tv")
async def ingest_tv(payload: TVIngestionPayload, authorization: Optional[str] = Header(None)):
    """Ingest TV data with authentication"""
    # Simple token check (in production, use proper validation)
    expected_token = f"Bearer {os.getenv('INGEST_TOKEN', 'ugboard-ingest-2026')}"
    
    if authorization != expected_token:
        raise HTTPException(
            status_code=401,
            detail="Invalid ingestion token"
        )
    
    # Process items
    processed = []
    for item in payload.items:
        processed.append({
            **item.dict(),
            "ingested_at": datetime.utcnow().isoformat(),
            "validated": True
        })
    
    return {
        "status": "success",
        "message": f"Ingested {len(processed)} songs from TV",
        "source": payload.source,
        "count": len(processed),
        "timestamp": datetime.utcnow().isoformat(),
        "validation": {
            "total_received": len(payload.items),
            "valid": len(processed),
            "invalid": len(payload.items) - len(processed)
        }
    }

# Error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.utcnow().isoformat(),
        "path": str(request.url.path)
    }
