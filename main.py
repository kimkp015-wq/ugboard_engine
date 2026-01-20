"""
UG Board Engine - Production Ready Ugandan Music Chart System
Version: 8.3.0
"""
import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator, ConfigDict

# ====== LOGGING ======
def setup_logging():
    """Configure production logging"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    log_level = logging.INFO
    if os.getenv("ENV", "production") == "development":
        log_level = logging.DEBUG
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        handlers=[console_handler],
        force=True
    )
    
    logger = logging.getLogger("ugboard_engine")
    logger.info("Logging initialized")
    
    return logger

logger = setup_logging()

# ====== CONFIGURATION ======
class Config:
    """Centralized configuration"""
    # Security
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin_ug_board_secure_token_2025")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "ug_board_ingest_secure_token_2025")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
    
    # Environment
    ENVIRONMENT = os.getenv("ENV", "production")
    DEBUG = ENVIRONMENT != "production"
    
    # Data
    DATA_DIR = Path("data")
    DATA_DIR.mkdir(exist_ok=True)
    
    # Ugandan Regions (Only Uganda, no other countries)
    UGANDAN_REGIONS = {
        "central": {
            "name": "Central Region",
            "districts": ["Kampala", "Mukono", "Wakiso", "Masaka", "Luwero"],
            "musicians": ["Bobi Wine", "Eddy Kenzo", "Sheebah", "Daddy Andre", "Alien Skin", "Azawi"]
        },
        "eastern": {
            "name": "Eastern Region", 
            "districts": ["Jinja", "Mbale", "Soroti", "Iganga", "Tororo", "Mayuge"],
            "musicians": ["Geosteady", "Victor Ruz", "Temperature Touch", "Rexy"]
        },
        "western": {
            "name": "Western Region",
            "districts": ["Mbarara", "Fort Portal", "Hoima", "Kabale", "Kasese", "Ntungamo"],
            "musicians": ["Ray G", "Rema Namakula", "Truth 256", "Sister Charity"]
        },
        "northern": {
            "name": "Northern Region",
            "districts": ["Gulu", "Lira", "Arua", "Kitgum"],
            "musicians": ["Fik Fameica", "Bosmic Otim", "Eezzy", "Laxzy Mover"]
        }
    }
    
    VALID_REGIONS = set(UGANDAN_REGIONS.keys())
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        if cls.ENVIRONMENT == "production":
            if not cls.ADMIN_TOKEN or "replace_this" in cls.ADMIN_TOKEN:
                raise ValueError("ADMIN_TOKEN must be set in production")
        return cls

config = Config.validate()

# ====== MODELS ======
class SongItem(BaseModel):
    """Song data model for Ugandan content"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=100)
    plays: int = Field(0, ge=0)
    score: float = Field(0.0, ge=0.0, le=100.0)
    station: Optional[str] = Field(None, max_length=50)
    region: str = Field("central", pattern="^(central|eastern|western|northern)$")
    district: Optional[str] = Field(None, max_length=50)
    timestamp: Optional[str] = Field(None)
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        try:
            if v.endswith('Z'):
                v = v[:-1] + '+00:00'
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('Invalid ISO 8601 timestamp')

class IngestPayload(BaseModel):
    """Ingestion payload"""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000)
    source: str = Field(..., min_length=1, max_length=100)
    metadata: Optional[Dict[str, Any]] = None

class YouTubeIngestPayload(IngestPayload):
    """YouTube ingestion payload"""
    channel_id: Optional[str] = Field(None, max_length=50)
    video_id: Optional[str] = Field(None, max_length=20)
    category: str = Field("music", max_length=50)

# ====== TRENDING SERVICE ======
class TrendingService:
    """8-hour trending window service"""
    
    @staticmethod
    def get_current_trending_window() -> Dict[str, Any]:
        """Get current 8-hour window information"""
        current_time = time.time()
        hours_since_epoch = int(current_time // 3600)
        
        window_number = hours_since_epoch // 8
        window_start_hour = (window_number * 8) % 24
        window_end_hour = (window_start_hour + 8) % 24
        
        next_window_start = (window_number + 1) * 8 * 3600
        seconds_remaining = max(0, next_window_start - current_time)
        
        return {
            "window_number": window_number,
            "window_start_utc": f"{window_start_hour:02d}:00",
            "window_end_utc": f"{window_end_hour:02d}:00",
            "seconds_remaining": int(seconds_remaining),
            "description": f"8-hour window {window_start_hour:02d}:00-{window_end_hour:02d}:00 UTC"
        }
    
    @staticmethod
    def get_current_trending_song(youtube_songs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get single trending song for current 8-hour window"""
        if not youtube_songs:
            return None
        
        hours_since_epoch = int(time.time() // 3600)
        trending_index = (hours_since_epoch // 8) % len(youtube_songs)
        
        return youtube_songs[trending_index]
    
    @staticmethod
    def get_trending_songs(youtube_songs: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs based on 8-hour window algorithm"""
        if not youtube_songs:
            return []
        
        window_info = TrendingService.get_current_trending_window()
        window_number = window_info["window_number"]
        
        # Filter recent songs (last 72 hours)
        recent_songs = [
            song for song in youtube_songs
            if datetime.fromisoformat(song.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
            datetime.utcnow() - timedelta(hours=72)
        ]
        
        if not recent_songs:
            return []
        
        # Deterministic sorting based on window number
        def sort_key(song: Dict[str, Any]) -> Tuple[int, float]:
            hash_val = hash(f"{window_number}_{song.get('id', '')}") % 1000
            popularity = song.get("score", 0) * 0.6 + song.get("plays", 0) * 0.4
            return (hash_val, popularity)
        
        sorted_songs = sorted(recent_songs, key=sort_key, reverse=True)[:limit]
        
        # Add trend ranks
        for i, song in enumerate(sorted_songs, 1):
            song["trend_rank"] = i
        
        return sorted_songs

# ====== DATABASE ======
class JSONDatabase:
    """Enhanced database with file persistence and caching"""
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        
        # Initialize data structures
        self.songs: List[Dict[str, Any]] = []
        self.chart_history: List[Dict[str, Any]] = []
        self.regions: Dict[str, Dict[str, Any]] = {}
        self.youtube_songs: List[Dict[str, Any]] = []
        
        # Initialize regions
        for region_code, region_data in config.UGANDAN_REGIONS.items():
            self.regions[region_code] = {
                "name": region_data["name"],
                "districts": region_data["districts"],
                "musicians": region_data["musicians"],
                "songs": []
            }
        
        # Load existing data
        self._load_data()
    
    def _load_data(self):
        """Load data from JSON files"""
        try:
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                
                # Process loaded songs
                self.youtube_songs = [
                    song for song in self.songs
                    if song.get("source", "").startswith("youtube_")
                ]
                
                # Reset region songs
                for region in self.regions.values():
                    region["songs"] = []
                
                # Reassign songs to regions
                for song in self.songs:
                    region = song.get("region")
                    if region in self.regions:
                        self.regions[region]["songs"].append(song)
                
                logger.info(f"Loaded {len(self.songs)} songs from disk")
            
            history_file = self.data_dir / "chart_history.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    self.chart_history = json.load(f)
        
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load data: {e}")
    
    def _save_data(self):
        """Save data to JSON files"""
        try:
            # Save songs
            with open(self.data_dir / "songs.json", 'w') as f:
                json.dump(self.songs, f, indent=2, default=str)
            
            # Save chart history
            with open(self.data_dir / "chart_history.json", 'w') as f:
                json.dump(self.chart_history, f, indent=2, default=str)
            
            logger.debug("Data saved to disk")
        
        except IOError as e:
            logger.error(f"Failed to save data: {e}")
    
    def add_songs(self, songs: List[SongItem], source: str) -> int:
        """Add songs with deduplication"""
        added = 0
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + added + 1}"
            
            # Basic deduplication
            is_duplicate = any(
                s.get("title", "").lower() == song.title.lower() and
                s.get("artist", "").lower() == song.artist.lower()
                for s in self.songs[-100:]
            )
            
            if not is_duplicate:
                self.songs.append(song_dict)
                
                # Add to region
                if song.region in self.regions:
                    self.regions[song.region]["songs"].append(song_dict)
                
                # Track YouTube songs
                if source.startswith("youtube_"):
                    self.youtube_songs.append(song_dict)
                
                added += 1
        
        # Limit total songs
        if len(self.songs) > 10000:
            self.songs = self.songs[-10000:]
        
        # Save if songs were added
        if added > 0:
            self._save_data()
        
        return added
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs sorted by score"""
        source_list = self.regions[region]["songs"] if region and region in self.regions else self.songs
        
        sorted_songs = sorted(
            source_list,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:limit]
        
        return sorted_songs
    
    def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs using 8-hour window algorithm"""
        return TrendingService.get_trending_songs(self.youtube_songs, limit)
    
    def get_current_trending_song(self) -> Optional[Dict[str, Any]]:
        """Get single trending song for current window"""
        return TrendingService.get_current_trending_song(self.youtube_songs)
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for each region"""
        stats = {}
        
        for region_code, region_data in self.regions.items():
            songs = region_data["songs"]
            
            if songs:
                total_plays = sum(s.get("plays", 0) for s in songs)
                avg_score = sum(s.get("score", 0) for s in songs) / len(songs)
                top_song = max(songs, key=lambda x: x.get("score", 0))
                
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": len(songs),
                    "total_plays": total_plays,
                    "average_score": round(avg_score, 2),
                    "top_song": {
                        "title": top_song.get("title"),
                        "artist": top_song.get("artist"),
                        "score": top_song.get("score")
                    },
                    "districts": region_data["districts"],
                    "musicians": region_data["musicians"]
                }
            else:
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": 0,
                    "total_plays": 0,
                    "average_score": 0.0,
                    "top_song": None,
                    "districts": region_data["districts"],
                    "musicians": region_data["musicians"]
                }
        
        return stats

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
        if not config.ADMIN_TOKEN:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Admin authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.ADMIN_TOKEN:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing admin token"
            )
        return True
    
    @staticmethod
    def verify_ingest(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        if not config.INGEST_TOKEN:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ingestion authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.INGEST_TOKEN:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing ingestion token"
            )
        return True
    
    @staticmethod
    def verify_youtube(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        if not config.YOUTUBE_TOKEN:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="YouTube authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.YOUTUBE_TOKEN:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing YouTube token"
            )
        return True

# ====== VALIDATION SERVICE ======
class ValidationService:
    """Ugandan content validation"""
    
    UGANDAN_ARTISTS = {
        "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
        "gravity", "vyroota", "geosteady", "feffe busi",
        "jose chameleone", "bebe cool", "alien skin", "azawi",
        "vinka", "rema", "ray g", "truth 256", "sister charity",
        "victor ruz", "temperature touch", "rexy", "fik fameica",
        "bosmic otim", "eezzy", "laxzy mover", "john blaq",
        "spice diana", "pallaso", "zex bilangilangi"
    }
    
    @classmethod
    def is_ugandan_artist(cls, artist_name: str) -> bool:
        artist_lower = artist_name.lower()
        
        # Exact match
        if artist_lower in cls.UGANDAN_ARTISTS:
            return True
        
        # Partial match
        for ug_artist in cls.UGANDAN_ARTISTS:
            if ug_artist in artist_lower:
                return True
        
        return False

# ====== LIFECYCLE ======
current_chart_week = datetime.utcnow().strftime("%Y-W%W")
app_start_time = datetime.utcnow()
request_count = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle"""
    # Startup
    logger.info(f"UG Board Engine v8.3.0 starting up")
    logger.info(f"Environment: {config.ENVIRONMENT}")
    logger.info(f"Database: {len(db.songs)} songs loaded")
    
    # Show trending window info
    window_info = TrendingService.get_current_trending_window()
    logger.info(f"Trending window: {window_info['description']}")
    
    yield
    
    # Shutdown
    logger.info(f"UG Board Engine shutting down")
    logger.info(f"Total requests: {request_count}")
    logger.info(f"Total songs: {len(db.songs)}")

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine - Ugandan Music Charts",
    version="8.3.0",
    description="Official Ugandan Music Chart System with Regional Support",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Root", "description": "Service information"},
        {"name": "Charts", "description": "Music chart endpoints"},
        {"name": "Regions", "description": "Ugandan regional data"},
        {"name": "Trending", "description": "Trending songs with 8-hour rotation"},
        {"name": "Ingestion", "description": "Data ingestion endpoints"},
        {"name": "Admin", "description": "Administrative functions"},
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if config.DEBUG else [
        "https://ugboard-engine.onrender.com",
        "https://your-frontend.com"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# ====== API ENDPOINTS ======
@app.get("/", tags=["Root"])
async def root(request: Request):
    """Root endpoint"""
    global request_count
    request_count += 1
    
    window_info = TrendingService.get_current_trending_window()
    
    return {
        "service": "UG Board Engine",
        "version": "8.3.0",
        "status": "online",
        "environment": config.ENVIRONMENT,
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "trending_window": window_info,
        "ugandan_regions": list(config.VALID_REGIONS),
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "charts": "/charts/top100",
            "regions": "/charts/regions",
            "trending": "/charts/trending"
        }
    }

@app.get("/health", tags=["Root"])
async def health():
    """Health check"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "database": {
            "songs": len(db.songs),
            "regions": len(db.regions)
        },
        "requests_served": request_count,
        "chart_week": current_chart_week
    }

@app.get("/charts/top100", tags=["Charts"])
async def get_top100(
    limit: int = Query(100, ge=1, le=200),
    region: Optional[str] = Query(None)
):
    """Get Uganda Top 100 chart"""
    songs = db.get_top_songs(limit, region)
    
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100" + (f" - {region.capitalize()} Region" if region else ""),
        "week": current_chart_week,
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/regions", tags=["Charts", "Regions"])
async def get_all_regions():
    """Get all Ugandan region statistics"""
    region_stats = db.get_region_stats()
    
    return {
        "regions": region_stats,
        "count": len(region_stats),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"])
async def get_region_detail(
    region: str = FPath(..., description="Ugandan region: central, eastern, western, northern")
):
    """Get detailed region information"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    songs = db.get_top_songs(10, region)
    region_data = config.UGANDAN_REGIONS[region]
    
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "region": region,
        "region_name": region_data["name"],
        "songs": songs,
        "count": len(songs),
        "districts": region_data["districts"],
        "musicians": region_data["musicians"],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending", tags=["Charts", "Trending"])
async def get_trending(
    limit: int = Query(10, ge=1, le=50)
):
    """Get trending songs with 8-hour rotation"""
    songs = db.get_trending_songs(limit)
    window_info = TrendingService.get_current_trending_window()
    
    return {
        "chart": "Trending Now - Uganda",
        "entries": songs,
        "count": len(songs),
        "trending_window": window_info,
        "rotation": "8-hour window rotation",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending/now", tags=["Charts", "Trending"])
async def get_current_trending():
    """Get current trending song"""
    trending_song = db.get_current_trending_song()
    window_info = TrendingService.get_current_trending_window()
    
    if not trending_song:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No trending song available"
        )
    
    return {
        "trending_song": trending_song,
        "trending_window": window_info,
        "next_change": f"{window_info['seconds_remaining'] // 3600}h {(window_info['seconds_remaining'] % 3600) // 60}m",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/youtube", tags=["Ingestion"])
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube)
):
    """Ingest YouTube data with Ugandan validation"""
    try:
        valid_items = []
        
        for item in payload.items:
            if ValidationService.is_ugandan_artist(item.artist):
                valid_items.append(item)
        
        added_count = db.add_songs(valid_items, f"youtube_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} Ugandan YouTube songs",
            "source": payload.source,
            "valid_count": len(valid_items),
            "added_count": added_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"YouTube ingestion failed: {str(e)}"
        )

@app.post("/ingest/tv", tags=["Ingestion"])
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
            detail=f"TV ingestion failed: {str(e)}"
        )

@app.get("/admin/health", tags=["Admin"])
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Admin health check"""
    uptime = datetime.utcnow() - app_start_time
    region_stats = db.get_region_stats()
    
    total_plays = sum(stats["total_plays"] for stats in region_stats.values())
    total_songs = sum(stats["total_songs"] for stats in region_stats.values())
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "environment": config.ENVIRONMENT
        },
        "database": {
            "total_songs": total_songs,
            "total_plays": total_plays,
            "regions": len(region_stats)
        },
        "trending_window": TrendingService.get_current_trending_window()
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
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
    logger.error(f"Unhandled exception at {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

# ====== MAIN ENTRY ======
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    banner = f"""
    ============================================
    UG Board Engine v8.3.0 - Ugandan Music Charts
    ============================================
    Environment: {config.ENVIRONMENT}
    Database: {len(db.songs)} songs loaded
    Regions: {', '.join(sorted(config.VALID_REGIONS))}
    Chart Week: {current_chart_week}
    ============================================
    Server: http://0.0.0.0:{port}
    Docs: http://0.0.0.0:{port}/docs
    Health: http://0.0.0.0:{port}/health
    ============================================
    """
    
    print(banner)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info" if config.ENVIRONMENT == "production" else "debug"
    )
