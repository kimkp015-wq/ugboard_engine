"""
UG Board Engine - Production Ready Application
Enhanced with monitoring, caching, and security features
"""
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, Security, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from pydantic import BaseModel, Field, validator
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log") if os.getenv("ENV") != "production" else None
    ]
)
logger = logging.getLogger(__name__)

# ==================== Configuration ====================
class Config:
    """Application configuration."""
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
    ENVIRONMENT = os.getenv("ENV", "production")
    IS_PRODUCTION = ENVIRONMENT == "production"
    CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 minutes default
    RATE_LIMIT_DEFAULT = os.getenv("RATE_LIMIT_DEFAULT", "100/minute")
    
    @classmethod
    def validate_tokens(cls):
        """Validate that required tokens are set in production."""
        if cls.IS_PRODUCTION:
            missing = []
            if not cls.ADMIN_TOKEN:
                missing.append("ADMIN_TOKEN")
            if not cls.INGEST_TOKEN:
                missing.append("INGEST_TOKEN")
            if missing:
                logger.warning(f"Missing tokens in production: {missing}")
        return cls

config = Config.validate_tokens()

# ==================== Rate Limiter Setup ====================
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[config.RATE_LIMIT_DEFAULT],
    storage_uri="memory://",
    strategy="moving-window",
    headers_enabled=True
)

# ==================== Models ====================
class SongItem(BaseModel):
    """Model for song data with validation."""
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=100)
    plays: Optional[int] = Field(0, ge=0)
    score: Optional[float] = Field(0.0, ge=0.0, le=100.0)
    station: Optional[str] = Field(None, max_length=50)
    region: Optional[str] = Field("ug", min_length=2, max_length=2)
    genre: Optional[str] = Field("afrobeat", max_length=50)
    
    @validator('region')
    def validate_region(cls, v):
        valid_regions = ["ug", "ke", "tz", "rw"]
        if v.lower() not in valid_regions:
            raise ValueError(f"Region must be one of {valid_regions}")
        return v.lower()

class TVIngestionPayload(BaseModel):
    """Model for TV data ingestion."""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000)
    source: str = Field(..., min_length=1, max_length=100)
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

# ==================== Database Layer ====================
class InMemorySongDB:
    """In-memory database for songs with basic CRUD operations."""
    def __init__(self):
        self.songs = [
            {"id": "1", "title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5, "genre": "kadongo kamu"},
            {"id": "2", "title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3, "genre": "afrobeat"},
            {"id": "3", "title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7, "genre": "dancehall"},
            {"id": "4", "title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1, "genre": "dancehall"},
            {"id": "5", "title": "Tonny On Low", "artist": "Gravity Omutujju", "plays": 7500, "score": 87.2, "genre": "hip hop"},
            {"id": "6", "title": "Bweyagala", "artist": "Vyroota", "plays": 7200, "score": 86.5, "genre": "kidandali"},
            {"id": "7", "title": "Enjoy", "artist": "Geosteady", "plays": 6800, "score": 85.8, "genre": "rnb"},
            {"id": "8", "title": "Sembera", "artist": "Feffe Busi", "plays": 6500, "score": 84.3, "genre": "hip hop"},
        ]
        self.requests_served = 0
    
    def get_all(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all songs sorted by score."""
        sorted_songs = sorted(self.songs, key=lambda x: x["score"], reverse=True)
        return sorted_songs[:limit]
    
    def add_songs(self, songs: List[Dict[str, Any]]) -> int:
        """Add new songs to the database."""
        current_max_id = max(int(s["id"]) for s in self.songs) if self.songs else 0
        for i, song in enumerate(songs, 1):
            song["id"] = str(current_max_id + i)
            self.songs.append(song)
        return len(songs)
    
    def increment_request_count(self):
        """Increment request counter."""
        self.requests_served += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        return {
            "total_songs": len(self.songs),
            "unique_artists": len(set(s["artist"] for s in self.songs)),
            "total_plays": sum(s["plays"] for s in self.songs),
            "requests_served": self.requests_served
        }

db = InMemorySongDB()

# ==================== Authentication ====================
security = HTTPBearer(auto_error=False)

class AuthService:
    """Authentication service."""
    
    @staticmethod
    def verify_admin_token(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify admin token."""
        if not config.ADMIN_TOKEN:
            logger.error("Admin token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Admin authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.ADMIN_TOKEN:
            logger.warning(f"Invalid admin token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing admin token"
            )
        return True
    
    @staticmethod
    def verify_ingest_token(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify ingestion token."""
        if not config.INGEST_TOKEN:
            logger.error("Ingest token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ingestion authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.INGEST_TOKEN:
            logger.warning(f"Invalid ingest token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing ingestion token"
            )
        return True

# ==================== Application Lifespan ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    # Startup
    start_time = time.time()
    logger.info("🚀 Starting UG Board Engine...")
    logger.info(f"📁 Environment: {config.ENVIRONMENT}")
    logger.info(f"🔒 Admin token configured: {bool(config.ADMIN_TOKEN)}")
    
    # Initialize cache
    FastAPICache.init(InMemoryBackend())
    logger.info("💾 Cache initialized")
    
    yield
    
    # Shutdown
    uptime = time.time() - start_time
    logger.info(f"👋 Shutting down UG Board Engine")
    logger.info(f"⏱️  Uptime: {uptime:.2f} seconds")
    logger.info(f"📊 Total requests served: {db.requests_served}")

# ==================== FastAPI App ====================
app = FastAPI(
    title="UG Board Engine",
    description="Official Ugandan Music Chart System API",
    version="6.1.0",
    docs_url="/docs" if not config.IS_PRODUCTION else None,
    redoc_url="/redoc" if not config.IS_PRODUCTION else None,
    openapi_url="/openapi.json" if not config.IS_PRODUCTION else None,
    lifespan=lifespan
)

# Configure rate limiter
app.state.limiter = limiter

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if not config.IS_PRODUCTION else [],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"]
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# ==================== Rate Limit Error Handler ====================
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Handle rate limit exceeded."""
    logger.warning(f"Rate limit exceeded for {request.client.host}")
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        headers={
            "Retry-After": str(exc.retry_after),
            "X-RateLimit-Limit": str(exc.limit.limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(int(time.time() + exc.retry_after))
        },
        content={
            "error": "Rate limit exceeded",
            "detail": f"Too many requests. Please try again in {exc.retry_after} seconds.",
            "retry_after": exc.retry_after,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

# ==================== Health Endpoints ====================
@app.get("/health")
@limiter.limit("30/minute")
async def health_check(request: Request) -> Dict[str, Any]:
    """Health check endpoint."""
    db.increment_request_count()
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "ugboard-engine",
        "version": "6.1.0",
        "environment": config.ENVIRONMENT,
        "uptime": "N/A",  # Would need to track start time
        "checks": {
            "api": "healthy",
            "database": "in_memory",
            "authentication": "configured" if config.ADMIN_TOKEN else "not_configured",
            "cache": "enabled",
            "rate_limiting": "enabled"
        }
    }

@app.get("/metrics")
@limiter.limit("10/minute")
async def metrics(request: Request) -> Dict[str, Any]:
    """Prometheus-style metrics endpoint."""
    stats = db.get_stats()
    
    return {
        "metrics": {
            "ugboard_requests_total": stats["requests_served"],
            "ugboard_songs_total": stats["total_songs"],
            "ugboard_unique_artists": stats["unique_artists"],
            "ugboard_total_plays": stats["total_plays"]
        },
        "timestamp": datetime.utcnow().isoformat()
    }

# ==================== Public Endpoints ====================
@app.get("/")
@limiter.limit("60/minute")
@cache(expire=config.CACHE_TTL)
async def root(request: Request) -> Dict[str, Any]:
    """Root endpoint with service information."""
    db.increment_request_count()
    
    return {
        "service": "UG Board Engine - Ugandan Music",
        "version": "6.1.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": config.ENVIRONMENT,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "metrics": "/metrics",
            "top100": "/charts/top100",
            "ingestion": {
                "tv": "/ingest/tv (POST)",
                "radio": "/ingest/radio (POST)"
            },
            "admin": "/admin/status (GET)"
        },
        "documentation": "https://github.com/kimkp015-wq/ugboard_engine"
    }

@app.get("/charts/top100")
@limiter.limit("100/minute")
@cache(expire=60)  # Cache for 1 minute
async def get_top100(
    request: Request,
    limit: int = 100,
    format: Optional[str] = "json"
) -> Union[Dict[str, Any], Response]:
    """Get Uganda Top 100 chart."""
    db.increment_request_count()
    
    songs = db.get_all(limit)
    for i, song in enumerate(songs, 1):
        song["rank"] = i
        song["change"] = "same"
    
    result = {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs,
        "total_entries": len(songs),
        "timestamp": datetime.utcnow().isoformat(),
        "generated_in_ms": 0  # Add timing in production
    }
    
    if format == "csv":
        # Convert to CSV
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["rank", "title", "artist", "plays", "score", "genre"])
        writer.writeheader()
        for song in songs:
            writer.writerow({
                "rank": song["rank"],
                "title": song["title"],
                "artist": song["artist"],
                "plays": song["plays"],
                "score": song["score"],
                "genre": song["genre"]
            })
        
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=uganda_top100.csv"}
        )
    
    return result

# ==================== Authenticated Endpoints ====================
@app.post("/ingest/tv")
@limiter.limit("50/minute")
async def ingest_tv(
    request: Request,
    payload: TVIngestionPayload,
    auth: bool = Depends(AuthService.verify_ingest_token)
) -> Dict[str, Any]:
    """Ingest TV data."""
    db.increment_request_count()
    
    ugandan_artists = {"bobi wine", "eddy kenzo", "sheebah", "daddy andre", "gravity", "vyroota", "geosteady", "feffe busi"}
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
    
    # Add to database
    added_count = db.add_songs(valid_items)
    
    logger.info(f"Ingested {added_count} songs from TV source: {payload.source}")
    
    return {
        "status": "success",
        "message": f"Ingested {added_count} Ugandan songs from TV",
        "source": payload.source,
        "valid_count": len(valid_items),
        "added_to_db": added_count,
        "timestamp": datetime.utcnow().isoformat(),
        "environment": config.ENVIRONMENT
    }

@app.post("/ingest/radio")
@limiter.limit("50/minute")
async def ingest_radio(
    request: Request,
    payload: TVIngestionPayload,
    auth: bool = Depends(AuthService.verify_ingest_token)
) -> Dict[str, Any]:
    """Ingest radio data."""
    db.increment_request_count()
    
    valid_items = []
    for item in payload.items:
        valid_items.append({
            **item.dict(),
            "ingested_at": datetime.utcnow().isoformat(),
            "source_type": "radio"
        })
    
    added_count = db.add_songs(valid_items)
    
    logger.info(f"Ingested {added_count} songs from radio station: {payload.source}")
    
    return {
        "status": "success",
        "message": f"Ingested {added_count} songs from radio",
        "station": payload.source,
        "count": added_count,
        "timestamp": datetime.utcnow().isoformat()
    }

# ==================== Admin Endpoints ====================
@app.get("/admin/status")
@limiter.limit("30/minute")
async def admin_status(
    request: Request,
    auth: bool = Depends(AuthService.verify_admin_token)
) -> Dict[str, Any]:
    """Admin status endpoint."""
    db.increment_request_count()
    stats = db.get_stats()
    
    return {
        "status": "admin_authenticated",
        "environment": config.ENVIRONMENT,
        "timestamp": datetime.utcnow().isoformat(),
        "statistics": stats,
        "configuration": {
            "admin_token_configured": bool(config.ADMIN_TOKEN),
            "ingest_token_configured": bool(config.INGEST_TOKEN),
            "environment": config.ENVIRONMENT,
            "cache_ttl": config.CACHE_TTL,
            "rate_limit_default": config.RATE_LIMIT_DEFAULT
        }
    }

# ==================== Error Handlers ====================
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions."""
    logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "environment": config.ENVIRONMENT
        }
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    error_detail = str(exc) if not config.IS_PRODUCTION else "Internal server error"
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": error_detail,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "environment": config.ENVIRONMENT,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )

# ==================== Application Entry Point ====================
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    print(f"""
    🎵 UG Board Engine v6.1.0
    🌍 Environment: {config.ENVIRONMENT}
    🚀 Starting on port: {port}
    📊 Docs: http://localhost:{port}/docs
    🔒 Rate Limiting: Enabled
    💾 Caching: Enabled
    📈 Metrics: /metrics
    """)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=not config.IS_PRODUCTION,
        log_level="info",
        access_log=True
    )
