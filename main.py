"""
UG Board Engine - Production Ready with Original Security
"""
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, HTTPException, Header, Depends, Security, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==================== YOUR ORIGINAL SECURITY ====================
# Get tokens from environment - KEEPING YOUR ORIGINAL DEFAULT VALUES
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# Security - YOUR ORIGINAL IMPLEMENTATION
security = HTTPBearer()

# Authentication functions - YOUR ORIGINAL CODE
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

# Environment
ENVIRONMENT = os.getenv("ENV", "production")

# ==================== YOUR ORIGINAL MODELS ====================
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

# ==================== YOUR DATABASE ====================
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

# ==================== FastAPI App ====================
app = FastAPI(
    title="UG Board Engine",
    description="Official Ugandan Music Chart System",
    version="6.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware (simple version)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== Health Endpoints ====================
@app.get("/health")
async def health():
    """Health endpoint - MUST exist for Render.com"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "environment": ENVIRONMENT,
        "tokens_configured": bool(ADMIN_TOKEN and INGEST_TOKEN),
        "deployment": "srv-d5mbvjlactks73bopsug"
    }

@app.get("/")
async def root():
    """Root endpoint - YOUR ORIGINAL CODE"""
    return {
        "service": "UG Board Engine - Ugandan Music",
        "version": "6.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "focus": "Ugandan music and artists",
        "rule": "Foreign artists only allowed in collaborations with Ugandan artists",
        "environment": ENVIRONMENT,
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

@app.get("/charts/top100")
async def get_top100(limit: int = 100):
    """Get Uganda Top 100 chart - YOUR ORIGINAL CODE"""
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

# ==================== Authenticated Endpoints ====================
@app.post("/ingest/tv")
async def ingest_tv(
    payload: TVIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
):
    """Ingest TV data - YOUR ORIGINAL CODE"""
    
    ugandan_artists = ["bobi wine", "eddy kenzo", "sheebah", "daddy andre", "gravity"]
    valid_items = []
    
    for item in payload.items:
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
        "environment": ENVIRONMENT
    }

@app.post("/ingest/radio")
async def ingest_radio(
    payload: TVIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
):
    """Ingest radio data - YOUR ORIGINAL CODE"""
    
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

# ==================== Admin Endpoints ====================
@app.get("/admin/status", dependencies=[Depends(verify_admin_token)])
async def admin_status():
    """Admin status endpoint - YOUR ORIGINAL CODE"""
    return {
        "status": "admin_authenticated",
        "environment": ENVIRONMENT,
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
    """Publish weekly chart - YOUR ORIGINAL CODE"""
    return {
        "status": "success",
        "message": "Week published successfully",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "published_at": datetime.utcnow().isoformat(),
        "note": "Chart week is now immutable"
    }

# ==================== Internal Endpoints ====================
@app.post("/internal/health")
async def internal_health(auth: bool = Depends(verify_internal_token)):
    """Internal health check - YOUR ORIGINAL CODE"""
    return {
        "status": "healthy",
        "service": "ugboard-engine",
        "environment": ENVIRONMENT,
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": {
            "memory_usage": "N/A",
            "uptime": "N/A",
            "requests_served": "N/A"
        }
    }

# ==================== Error handling ====================
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Error handler - YOUR ORIGINAL CODE"""
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.utcnow().isoformat(),
        "path": str(request.url.path),
        "environment": ENVIRONMENT
    }

# ==================== Application Entry Point ====================
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    print(f"Starting UG Board Engine on port {port}")
    print(f"Environment: {ENVIRONMENT}")
    print(f"Docs: http://localhost:{port}/docs")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=ENVIRONMENT != "production",
        log_level="info"
    )
