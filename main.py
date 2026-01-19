# api/main.py - WITH PROPER AUTHENTICATION
from fastapi import FastAPI, HTTPException, Header, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime
import os
from typing import Optional, List
from pydantic import BaseModel

# Security
security = HTTPBearer()

app = FastAPI(
    title="UG Board Engine",
    description="Official Ugandan Music Chart System",
    version="6.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Get tokens from environment
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# Authentication functions
def verify_admin_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")
    return True

def verify_ingest_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != INGEST_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    return True

def verify_internal_token(x_internal_token: Optional[str] = Header(None)):
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    return True

# Pydantic models
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    station: Optional[str] = None
    region: Optional[str] = "ug"
    genre: Optional[str] = "afrobeat"

class TVIngestionPayload(BaseModel):
    items: List[SongItem]
    source: str
    timestamp: Optional[str] = None
    metadata: Optional[dict] = {}

# Ugandan music database
UGANDAN_SONGS = [
    {"id": "1", "title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5, "genre": "kadongo kamu"},
    {"id": "2", "title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3, "genre": "afrobeat"},
    {"id": "3", "title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7, "genre": "dancehall"},
    {"id": "4", "title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1, "genre": "dancehall"},
    {"id": "5", "title": "Tonny On Low", "artist": "Gravity Omutujju", "plays": 7500, "score": 87.2, "genre": "hip hop"},
    {"id": "6", "title": "Bweyagala", "artist": "Vyroota", "plays": 7200, "score": 86.5, "genre": "kidandali"},
    {"id": "7", "title": "Enjoy", "artist": "Geosteady", "plays": 6800, "score": 85.8, "genre": "rnb"},
    {"id": "8", "title": "Sembera", "artist": "Feffe Busi", "plays": 6500, "score": 84.3, "genre": "hip hop"},
]

# Root endpoint - Public
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine - Ugandan Music",
        "version": "6.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "focus": "Ugandan music and artists",
        "rule": "Foreign artists only allowed in collaborations with Ugandan artists",
        "environment": os.getenv("ENV", "development"),
        "instance": "srv-d5mb",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "uganda_top100": "/charts/top100",
            "regional_charts": "/charts/regions/{region}",
            "trending": "/charts/trending",
            "artist_info": "/artists/stats",
            "ingestion": "/ingest/{source} (Authenticated)",
            "admin": "/admin/* (Admin only)"
        }
    }

# Health endpoint - Public
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "environment": os.getenv("ENV", "development"),
        "tokens_configured": bool(ADMIN_TOKEN and INGEST_TOKEN)
    }

# Charts endpoint - Public
@app.get("/charts/top100")
async def get_top100(limit: int = 100):
    """Get Uganda Top 100 chart - Public"""
    songs = UGANDAN_SONGS.copy()
    songs.sort(key=lambda x: x["score"], reverse=True)
    
    for i, song in enumerate(songs[:limit], 1):
        song["rank"] = i
        song["change"] = "same"
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs[:limit],
        "total_entries": len(UGANDAN_SONGS),
        "timestamp": datetime.utcnow().isoformat(),
        "access": "public"
    }

# TV Ingestion endpoint - Authenticated
@app.post("/ingest/tv")
async def ingest_tv(
    payload: TVIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
):
    """Ingest TV data - Requires INGEST_TOKEN"""
    
    # Validate Ugandan music rules
    ugandan_artists = ["bobi wine", "eddy kenzo", "sheebah", "daddy andre", "gravity"]
    valid_items = []
    
    for item in payload.items:
        # Check if artist is Ugandan
        artist_lower = item.artist.lower()
        is_ugandan = any(ug_artist in artist_lower for ug_artist in ugandan_artists)
        
        if is_ugandan:
            valid_items.append({
                **item.dict(),
                "ingested_at": datetime.utcnow().isoformat(),
                "validated": True,
                "is_ugandan": True
            })
    
    return {
        "status": "success",
        "message": f"Ingested {len(valid_items)} Ugandan songs from TV",
        "source": payload.source,
        "valid_count": len(valid_items),
        "invalid_count": len(payload.items) - len(valid_items),
        "timestamp": datetime.utcnow().isoformat(),
        "environment": os.getenv("ENV", "development")
    }

# Radio Ingestion endpoint - Authenticated
@app.post("/ingest/radio")
async def ingest_radio(
    payload: TVIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
):
    """Ingest radio data - Requires INGEST_TOKEN"""
    
    valid_items = []
    for item in payload.items:
        valid_items.append({
            **item.dict(),
            "ingested_at": datetime.utcnow().isoformat(),
            "source_type": "radio"
        })
    
    return {
        "status": "success",
        "message": f"Ingested {len(valid_items)} songs from radio",
        "station": payload.source,
        "count": len(valid_items),
        "timestamp": datetime.utcnow().isoformat()
    }

# Admin endpoints - Admin only
@app.get("/admin/status", dependencies=[Depends(verify_admin_token)])
async def admin_status():
    """Admin status endpoint - Requires ADMIN_TOKEN"""
    return {
        "status": "admin_authenticated",
        "environment": os.getenv("ENV", "development"),
        "tokens": {
            "admin_configured": bool(ADMIN_TOKEN),
            "ingest_configured": bool(INGEST_TOKEN)
        },
        "statistics": {
            "total_songs": len(UGANDAN_SONGS),
            "unique_artists": len(set(s["artist"] for s in UGANDAN_SONGS)),
            "total_plays": sum(s["plays"] for s in UGANDAN_SONGS)
        },
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/admin/week/publish", dependencies=[Depends(verify_admin_token)])
async def publish_week():
    """Publish weekly chart - Admin only"""
    return {
        "status": "success",
        "message": "Week published successfully",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "published_at": datetime.utcnow().isoformat(),
        "note": "Chart week is now immutable"
    }

# Internal endpoints - Service-to-service
@app.post("/internal/health")
async def internal_health(auth: bool = Depends(verify_internal_token)):
    """Internal health check - Requires INTERNAL_TOKEN"""
    return {
        "status": "healthy",
        "service": "ugboard-engine",
        "environment": os.getenv("ENV", "development"),
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": {
            "memory_usage": "N/A",
            "uptime": "N/A",
            "requests_served": "N/A"
        }
    }

# Error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.utcnow().isoformat(),
        "path": str(request.url.path),
        "environment": os.getenv("ENV", "development")
    }
