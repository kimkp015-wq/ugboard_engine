"""
UG Board Engine - Production Ready with Pydantic v2 Fix
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query, Path as FPath, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from pydantic import ConfigDict

# ====== CONFIGURE LOGGING ======
def setup_logging():
    """Configure structured logging"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(logs_dir / "ugboard_engine.log")
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ====== CONFIGURATION ======
class Config:
    """Application configuration"""
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
    ENVIRONMENT = os.getenv("ENV", "production")
    
    # Data directory
    DATA_DIR = Path("data")
    DATA_DIR.mkdir(exist_ok=True)
    
    # Valid regions
    VALID_REGIONS = {"ug", "ke", "tz", "rw"}
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        if cls.ENVIRONMENT == "production" and not cls.ADMIN_TOKEN:
            logger.warning("⚠️ ADMIN_TOKEN not set in production")
        return cls

config = Config.validate()

# ====== MODELS ======
class SongItem(BaseModel):
    """Song data model with Pydantic v2 syntax"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    title: str = Field(..., min_length=1, max_length=200, description="Song title")
    artist: str = Field(..., min_length=1, max_length=100, description="Artist name")
    plays: int = Field(0, ge=0, description="Number of plays")
    score: float = Field(0.0, ge=0.0, le=100.0, description="Chart score (0-100)")
    station: Optional[str] = Field(None, max_length=50, description="TV/Radio station")
    region: str = Field("ug", pattern="^(ug|ke|tz|rw)$", description="Region code")
    timestamp: Optional[str] = Field(None, description="ISO 8601 timestamp")
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISO 8601 timestamp"""
        if v:
            try:
                # Handle Z suffix and timezone offsets
                if v.endswith('Z'):
                    v = v[:-1] + '+00:00'
                datetime.fromisoformat(v)
            except ValueError:
                raise ValueError('Invalid ISO 8601 timestamp format')
        return v

class IngestPayload(BaseModel):
    """Base ingestion payload"""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000, description="List of songs")
    source: str = Field(..., min_length=1, max_length=100, description="Data source")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")

class YouTubeIngestPayload(IngestPayload):
    """YouTube-specific ingestion payload"""
    channel_id: Optional[str] = Field(None, max_length=50, description="YouTube channel ID")
    video_id: Optional[str] = Field(None, max_length=20, description="YouTube video ID")
    category: Optional[str] = Field("music", max_length=50, description="Content category")

# ====== DATABASE LAYER ======
class JSONDatabase:
    """File-based JSON database with thread safety"""
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._lock = None  # In production, use threading.Lock
        
        # Initialize data structures
        self.songs: List[Dict[str, Any]] = []
        self.chart_history: List[Dict[str, Any]] = []
        self.regions: Dict[str, Dict[str, Any]] = {
            "ug": {"name": "Uganda", "songs": []},
            "ke": {"name": "Kenya", "songs": []},
            "tz": {"name": "Tanzania", "songs": []},
            "rw": {"name": "Rwanda", "songs": []}
        }
        
        # Load existing data
        self._load_data()
    
    def _load_data(self) -> None:
        """Load data from JSON files"""
        try:
            # Load songs
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                logger.info(f"Loaded {len(self.songs)} songs from disk")
            
            # Load chart history
            history_file = self.data_dir / "chart_history.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    self.chart_history = json.load(f)
            
            # Load regions
            regions_file = self.data_dir / "regions.json"
            if regions_file.exists():
                with open(regions_file, 'r') as f:
                    loaded_regions = json.load(f)
                    # Merge with default structure
                    for region, data in loaded_regions.items():
                        if region in self.regions:
                            self.regions[region].update(data)
        
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load data: {e}")
            # Keep default empty structures
    
    def _save_data(self) -> None:
        """Save data to JSON files with error handling"""
        try:
            # Save songs
            with open(self.data_dir / "songs.json", 'w') as f:
                json.dump(self.songs, f, indent=2, default=str)
            
            # Save chart history
            with open(self.data_dir / "chart_history.json", 'w') as f:
                json.dump(self.chart_history, f, indent=2, default=str)
            
            # Save regions
            with open(self.data_dir / "regions.json", 'w') as f:
                json.dump(self.regions, f, indent=2, default=str)
            
            logger.debug("Data saved to disk")
        
        except IOError as e:
            logger.error(f"Failed to save data: {e}")
            # Continue operation in memory-only mode
    
    def add_songs(self, songs: List[SongItem], source: str) -> int:
        """
        Add songs to database with deduplication
        Returns: Number of songs added
        """
        added_count = 0
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + added_count + 1}"
            
            # Basic deduplication: same title + artist within 24 hours
            is_duplicate = any(
                s.get("title") == song.title and 
                s.get("artist") == song.artist and
                datetime.fromisoformat(s.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
                datetime.utcnow() - timedelta(hours=24)
                for s in self.songs[-100:]  # Check last 100 songs
            )
            
            if not is_duplicate:
                self.songs.append(song_dict)
                
                # Add to region
                region = song.region.lower()
                if region in self.regions:
                    self.regions[region]["songs"].append(song_dict)
                
                added_count += 1
        
        # Limit total songs to prevent memory issues
        if len(self.songs) > 10000:
            self.songs = self.songs[-10000:]
            logger.info("Trimmed songs database to 10,000 entries")
        
        # Save to disk
        if added_count > 0:
            self._save_data()
        
        return added_count
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs sorted by score"""
        source_list = self.regions[region]["songs"] if region and region in self.regions else self.songs
        
        sorted_songs = sorted(
            source_list,
            key=lambda x: x.get("score", 0),
            reverse=True
        )
        return sorted_songs[:limit]
    
    def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs based on recent activity"""
        if not self.songs:
            return []
        
        # Calculate trending score: 70% score + 30% recent plays
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        
        def trending_score(song: Dict[str, Any]) -> float:
            score = song.get("score", 0) * 0.7
            plays = song.get("plays", 0) * 0.3
            
            # Recency bonus
            ingested_at = song.get("ingested_at")
            if ingested_at:
                try:
                    ingest_time = datetime.fromisoformat(ingested_at.replace('Z', '+00:00'))
                    if ingest_time > recent_cutoff:
                        score += 10.0
                except (ValueError, AttributeError):
                    pass
            
            return score + plays
        
        sorted_trending = sorted(self.songs, key=trending_score, reverse=True)
        return sorted_trending[:limit]
    
    def publish_weekly_chart(self, current_week: str) -> Dict[str, Any]:
        """Publish weekly chart snapshot"""
        snapshot = {
            "week": current_week,
            "published_at": datetime.utcnow().isoformat(),
            "top100": self.get_top_songs(100),
            "regions": {
                region: self.get_top_songs(5, region)
                for region in self.regions.keys()
            }
        }
        
        self.chart_history.append(snapshot)
        
        # Keep only last 52 weeks (1 year)
        if len(self.chart_history) > 52:
            self.chart_history = self.chart_history[-52:]
        
        self._save_data()
        return snapshot

# Initialize database
db = JSONDatabase(config.DATA_DIR)

# ====== AUTHENTICATION ======
security = HTTPBearer(auto_error=False)

class AuthService:
    """Authentication service"""
    
    @staticmethod
    def verify_admin(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify admin token"""
        if not config.ADMIN_TOKEN:
            logger.error("Admin token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Admin authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.ADMIN_TOKEN:
            logger.warning("Invalid admin token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing admin token"
            )
        return True
    
    @staticmethod
    def verify_ingest(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify ingestion token"""
        if not config.INGEST_TOKEN:
            logger.error("Ingest token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ingestion authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.INGEST_TOKEN:
            logger.warning("Invalid ingest token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing ingestion token"
            )
        return True
    
    @staticmethod
    def verify_youtube(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify YouTube ingestion token"""
        if not config.YOUTUBE_TOKEN:
            logger.error("YouTube token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="YouTube authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.YOUTUBE_TOKEN:
            logger.warning("Invalid YouTube token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing YouTube ingestion token"
            )
        return True

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    # Startup
    logger.info("🚀 UG Board Engine starting up")
    logger.info(f"🌍 Environment: {config.ENVIRONMENT}")
    logger.info(f"📊 Database: {len(db.songs)} songs loaded")
    
    # Initial data check
    if len(db.songs) == 0:
        logger.info("📝 Database is empty - ready for ingestion")
    
    yield
    
    # Shutdown
    logger.info("🛑 UG Board Engine shutting down")
    logger.info(f"📈 Total songs processed: {len(db.songs)}")

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine",
    version="8.1.0",
    description="Official Ugandan Music Chart System with YouTube Worker Integration",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Root", "description": "Root endpoint and health checks"},
        {"name": "Charts", "description": "Music chart endpoints"},
        {"name": "Regions", "description": "Regional chart data"},
        {"name": "Trending", "description": "Trending songs"},
        {"name": "Ingestion", "description": "Data ingestion endpoints"},
        {"name": "Admin", "description": "Administrative functions"},
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if config.ENVIRONMENT != "production" else [
        "https://ugboard-engine.onrender.com",
        "https://your-frontend.com"  # Add your frontend URL
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ====== GLOBAL STATE ======
app_start_time = datetime.utcnow()
request_count = 0
current_chart_week = datetime.utcnow().strftime("%Y-W%W")

# ====== VALIDATION SERVICE ======
class ValidationService:
    """Business logic validation"""
    
    UGANDAN_ARTISTS = {
        "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
        "gravity", "vyroota", "geosteady", "feffe busi",
        "jose chameleone", "bebe cool", "radio and weasel"
    }
    
    @classmethod
    def is_ugandan_artist(cls, artist_name: str) -> bool:
        """Check if artist is Ugandan"""
        artist_lower = artist_name.lower()
        return any(ug_artist in artist_lower for ug_artist in cls.UGANDAN_ARTISTS)

# ====== API ENDPOINTS ======
@app.get("/", tags=["Root"], summary="Service information")
async def root():
    """Root endpoint with service information"""
    global request_count
    request_count += 1
    
    return {
        "service": "UG Board Engine",
        "version": "8.1.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "environment": config.ENVIRONMENT,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "charts": "/charts/*",
            "ingestion": "/ingest/*",
            "admin": "/admin/*"
        }
    }

@app.get("/health", tags=["Root"], summary="Health check")
async def health():
    """Health check endpoint for monitoring"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "database": {
            "songs": len(db.songs),
            "regions": len(db.regions),
            "chart_history": len(db.chart_history)
        },
        "requests_served": request_count,
        "chart_week": current_chart_week,
        "environment": config.ENVIRONMENT
    }

@app.get("/charts/top100", tags=["Charts"], summary="Uganda Top 100 chart")
async def get_top100(
    limit: int = Query(100, ge=1, le=200, description="Number of songs to return")
):
    """Get Uganda Top 100 chart for current week"""
    songs = db.get_top_songs(limit)
    
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100",
        "week": current_chart_week,
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/index", tags=["Charts"], summary="Chart publication index")
async def get_chart_index():
    """Get chart publication history index"""
    return {
        "current_week": current_chart_week,
        "available_weeks": [h["week"] for h in db.chart_history[-10:]],
        "history_count": len(db.chart_history),
        "last_published": db.chart_history[-1]["published_at"] if db.chart_history else None
    }

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"], summary="Top songs by region")
async def get_region_top5(
    region: str = FPath(..., description="Region code: ug, ke, tz, rw")
):
    """Get top 5 songs for a specific region"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    songs = db.get_top_songs(5, region)
    
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "region": region,
        "region_name": db.regions[region]["name"],
        "songs": songs,
        "count": len(songs),
        "week": current_chart_week,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending", tags=["Charts", "Trending"], summary="Trending songs")
async def get_trending(
    limit: int = Query(10, ge=1, le=50, description="Number of trending songs")
):
    """Get currently trending songs"""
    songs = db.get_trending_songs(limit)
    
    for i, song in enumerate(songs, 1):
        song["trend_rank"] = i
    
    return {
        "chart": "Trending Now",
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/youtube", tags=["Ingestion"], summary="Ingest YouTube data")
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube)
):
    """Ingest YouTube data with Ugandan content validation"""
    try:
        valid_items = []
        
        for item in payload.items:
            if ValidationService.is_ugandan_artist(item.artist):
                item_dict = item.model_dump()
                item_dict["source"] = f"youtube_{payload.source}"
                item_dict["category"] = payload.category
                if payload.channel_id:
                    item_dict["channel_id"] = payload.channel_id
                if payload.video_id:
                    item_dict["video_id"] = payload.video_id
                
                valid_items.append(item_dict)
        
        # Add to database
        added_count = db.add_songs([SongItem(**item) for item in valid_items], f"youtube_{payload.source}")
        
        logger.info(f"YouTube ingestion: {added_count} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} YouTube songs",
            "source": payload.source,
            "valid_count": len(valid_items),
            "added_count": added_count,
            "duplicate_count": len(valid_items) - added_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"YouTube ingestion error: {str(e)}"
        )

@app.post("/ingest/radio", tags=["Ingestion"], summary="Ingest radio data")
async def ingest_radio(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest radio data"""
    try:
        added_count = db.add_songs(payload.items, f"radio_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} radio songs",
            "source": payload.source,
            "added_count": added_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Radio ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Radio ingestion error: {str(e)}"
        )

@app.post("/ingest/tv", tags=["Ingestion"], summary="Ingest TV data")
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest TV data"""
    try:
        added_count = db.add_songs(payload.items, f"tv_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} TV songs",
            "source": payload.source,
            "added_count": added_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"TV ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"TV ingestion error: {str(e)}"
        )

@app.get("/admin/health", tags=["Admin"], summary="Admin health check")
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Admin health check with system details"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "environment": config.ENVIRONMENT
        },
        "database": {
            "total_songs": len(db.songs),
            "unique_artists": len(set(s.get("artist", "") for s in db.songs)),
            "regions": list(db.regions.keys()),
            "chart_history": len(db.chart_history)
        },
        "authentication": {
            "admin_configured": bool(config.ADMIN_TOKEN),
            "ingest_configured": bool(config.INGEST_TOKEN),
            "youtube_configured": bool(config.YOUTUBE_TOKEN)
        }
    }

@app.post("/admin/publish/weekly", tags=["Admin"], summary="Publish weekly chart")
async def publish_weekly(auth: bool = Depends(AuthService.verify_admin)):
    """Publish weekly chart for all regions"""
    try:
        result = db.publish_weekly_chart(current_chart_week)
        
        return {
            "status": "success",
            "message": "Weekly chart published successfully",
            "week": result["week"],
            "published_at": result["published_at"],
            "summary": {
                "top100_count": len(result["top100"]),
                "regions_published": len(result["regions"]),
                "total_songs": len(db.songs)
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Weekly publish error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Publish error: {str(e)}"
        )

@app.get("/admin/index", tags=["Admin"], summary="Admin publication index")
async def admin_index(auth: bool = Depends(AuthService.verify_admin)):
    """Admin-only detailed publication index"""
    return {
        "current_week": current_chart_week,
        "chart_history": db.chart_history[-5:],
        "publication_stats": {
            "total_publications": len(db.chart_history),
            "first_publication": db.chart_history[0]["week"] if db.chart_history else None,
            "last_publication": db.chart_history[-1]["week"] if db.chart_history else None
        },
        "database_stats": {
            "total_songs": len(db.songs),
            "songs_by_region": {region: len(data["songs"]) for region, data in db.regions.items()}
        }
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with structured logging"""
    logger.warning(f"HTTP {exc.status_code} at {request.url.path}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    logger.error(f"Unhandled exception at {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": str(exc) if config.ENVIRONMENT != "production" else "Contact support",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    logger.info(f"""
    🎵 UG Board Engine v8.1.0
    📅 Chart Week: {current_chart_week}
    🌍 Environment: {config.ENVIRONMENT}
    🌐 URL: http://localhost:{port}
    📚 Docs: http://localhost:{port}/docs
    🗄️  Database: {len(db.songs)} songs loaded
    """)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info" if config.ENVIRONMENT == "production" else "debug"
    )
