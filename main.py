"""
UG Board Engine - Consolidated Main Application
Combines API functionality with proper health endpoints
"""
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Depends, Security, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
# Add to main.py
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)

@app.get("/charts/top100")
@limiter.limit("100/minute")
async def get_top100():
    # Existing code

# ==================== Configuration ====================
# Get tokens from environment with secure defaults
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")

# Environment
ENVIRONMENT = os.getenv("ENV", "production")
IS_PRODUCTION = ENVIRONMENT == "production"

# ==================== Security ====================
security = HTTPBearer(auto_error=False)

def verify_admin_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> bool:
    """Verify admin token for protected endpoints."""
    if not ADMIN_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Admin token not configured"
        )
    
    if not credentials or credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing admin token"
        )
    return True

def verify_ingest_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> bool:
    """Verify ingestion token for data ingestion endpoints."""
    if not INGEST_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Ingest token not configured"
        )
    
    if not credentials or credentials.credentials != INGEST_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing ingestion token"
        )
    return True

def verify_internal_token(
    x_internal_token: Optional[str] = Header(None, alias="X-Internal-Token")
) -> bool:
    """Verify internal token for service-to-service communication."""
    if not INTERNAL_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Internal token not configured"
        )
    
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing internal token"
        )
    return True

# ==================== Models ====================
class SongItem(BaseModel):
    """Model for song data."""
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    station: Optional[str] = None
    region: Optional[str] = "ug"
    genre: Optional[str] = "afrobeat"

class TVIngestionPayload(BaseModel):
    """Model for TV data ingestion."""
    items: List[SongItem]
    source: str
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = {}

# ==================== Database ====================
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
    docs_url="/docs" if not IS_PRODUCTION else None,
    redoc_url="/redoc" if not IS_PRODUCTION else None,
    openapi_url="/openapi.json" if not IS_PRODUCTION else None,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if not IS_PRODUCTION else [],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== Health Endpoints ====================
@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint for Render.com and monitoring systems.
    This endpoint MUST exist at the root level for Render health checks.
    """
    status_checks = {
        "api": "healthy",
        "authentication": "configured" if ADMIN_TOKEN else "not_configured",
        "database": "in_memory",  # Update if using real database
        "environment": ENVIRONMENT,
    }
    
    overall_status = "healthy" if ADMIN_TOKEN else "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "ugboard-engine",
        "version": "6.0.0",
        "environment": ENVIRONMENT,
        "checks": status_checks,
        "deployment_id": "srv-d5mbvjlactks73bopsug",
    }

@app.get("/health/detailed", dependencies=[Depends(verify_internal_token)])
async def detailed_health_check() -> Dict[str, Any]:
    """Detailed health check with dependency verification."""
    # Add actual dependency checks here when you have them
    dependencies = {
        "api": {"status": "healthy", "response_time_ms": 1.2},
        "memory": {"status": "healthy", "usage_percent": 45.2},
        "scraper_system": {"status": "unknown", "note": "Not implemented"},
        "external_apis": {"status": "healthy", "note": "No external dependencies"},
    }
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "ugboard-engine",
        "version": "6.0.0",
        "uptime": "unknown",  # You can add uptime tracking
        "dependencies": dependencies,
    }

# ==================== Public Endpoints ====================
@app.get("/")
async def root() -> Dict[str, Any]:
    """Root endpoint with service information."""
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
async def get_top100(limit: int = 100) -> Dict[str, Any]:
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

# ==================== Authenticated Endpoints ====================
@app.post("/ingest/tv")
async def ingest_tv(
    payload: TVIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
) -> Dict[str, Any]:
    """Ingest TV data - Requires INGEST_TOKEN"""
    
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
) -> Dict[str, Any]:
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

# ==================== Admin Endpoints ====================
@app.get("/admin/status", dependencies=[Depends(verify_admin_token)])
async def admin_status() -> Dict[str, Any]:
    """Admin status endpoint - Requires ADMIN_TOKEN"""
    return {
        "status": "admin_authenticated",
        "environment": ENVIRONMENT,
        "tokens": {
            "admin_configured": bool(ADMIN_TOKEN),
            "ingest_configured": bool(INGEST_TOKEN),
            "internal_configured": bool(INTERNAL_TOKEN)
        },
        "statistics": {
            "total_songs": len(UGANDAN_SONGS),
            "unique_artists": len(set(s["artist"] for s in UGANDAN_SONGS)),
            "total_plays": sum(s["plays"] for s in UGANDAN_SONGS)
        },
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/admin/week/publish", dependencies=[Depends(verify_admin_token)])
async def publish_week() -> Dict[str, Any]:
    """Publish weekly chart - Admin only"""
    return {
        "status": "success",
        "message": "Week published successfully",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "published_at": datetime.utcnow().isoformat(),
        "note": "Chart week is now immutable"
    }

# ==================== Internal Endpoints ====================
@app.post("/internal/health")
async def internal_health(auth: bool = Depends(verify_internal_token)) -> Dict[str, Any]:
    """Internal health check - Requires INTERNAL_TOKEN"""
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

# ==================== Error Handlers ====================
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle HTTP exceptions with structured responses."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "environment": ENVIRONMENT
        }
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected exceptions."""
    # Log the full exception for debugging
    import logging
    logging.error(f"Unhandled exception: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if not IS_PRODUCTION else "Contact support",
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "environment": ENVIRONMENT
        }
    )

# ==================== Application Entry Point ====================
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    print(f"Starting UG Board Engine on port {port}")
    print(f"Environment: {ENVIRONMENT}")
    print(f"Docs available at: http://localhost:{port}/docs")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=not IS_PRODUCTION,
        log_level="info"
    )
