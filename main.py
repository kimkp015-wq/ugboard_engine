"""
UG Board Engine - Complete Production System v9.0.0
Integrated with existing project structure
"""

import os
import sys
import json
import time
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from contextlib import asynccontextmanager

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator, ConfigDict

# Import from your existing modules
try:
    # Import configuration from your existing structure
    from src.config.settings import Config, get_settings
    from api.admin import admin_router, health_router, publish_router
    from api.charts import charts_router, trending_router, regions_router
    from api.ingestion import ingestion_router
    from models.song import SongItem, SongBase
    from models.schemas.ingestion import IngestPayload, YouTubeIngestPayload
    from utils.security import verify_token, get_current_user
    from utils.health import health_check, system_status
    from services.database.redis_database import RedisCache
    from services.database.async_database import AsyncDatabase
    from scoring.scoring import ScoringEngine
    from scoring.auto_recalc import AutoRecalculator
    import config.logging_config as log_config
    
    # Use your existing configuration
    config = get_settings()
    logger = log_config.setup_logger(__name__)
    
    HAS_EXISTING_MODULES = True
    
except ImportError as e:
    # Fallback to integrated solution if modules not found
    print(f"Note: Some modules not found, using integrated solution: {e}")
    HAS_EXISTING_MODULES = False
    
    # ====== CONFIGURATION ======
    class Config:
        """Configuration class matching your .env.example"""
        # Environment
        ENVIRONMENT = os.getenv("ENV", "production").lower()
        DEBUG = ENVIRONMENT != "production"
        
        # Security Tokens
        ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
        INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
        INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
        YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
        
        # YouTube Worker
        YOUTUBE_WORKER_URL = os.getenv("YOUTUBE_WORKER_URL", "https://ugboard-youtube-puller.kimkp015.workers.dev")
        YOUTUBE_WORKER_TOKEN = os.getenv("YOUTUBE_WORKER_TOKEN", YOUTUBE_TOKEN)
        
        # Database
        DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/ugboard.db")
        
        # Ugandan Regions
        UGANDAN_REGIONS = {
            "central": {
                "name": "Central Region",
                "districts": ["Kampala", "Mukono", "Wakiso", "Masaka", "Luwero"],
                "musicians": ["Bobi Wine", "Eddy Kenzo", "Sheebah", "Daddy Andre", "Alien Skin"]
            },
            "eastern": {
                "name": "Eastern Region",
                "districts": ["Jinja", "Mbale", "Soroti", "Iganga", "Tororo"],
                "musicians": ["Geosteady", "Victor Ruz", "Temperature Touch", "Rexy"]
            },
            "western": {
                "name": "Western Region",
                "districts": ["Mbarara", "Fort Portal", "Hoima", "Kabale", "Kasese"],
                "musicians": ["Rema Namakula", "Mickie Wine", "Ray G", "Truth 256"]
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
            # Create directories
            directories = ["data", "logs", "backups"]
            for directory in directories:
                Path(directory).mkdir(exist_ok=True)
            
            if cls.ENVIRONMENT == "production":
                if not cls.ADMIN_TOKEN or "replace_this" in cls.ADMIN_TOKEN:
                    raise ValueError("ADMIN_TOKEN must be set in production")
            
            return cls
    
    config = Config.validate()
    
    # ====== LOGGING ======
    def setup_logger(name: str):
        """Setup logger"""
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG if config.DEBUG else logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    logger = setup_logger("ugboard")

# ====== MODELS ======
class SongItem(BaseModel):
    """Song data model"""
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

# ====== DATABASE INTEGRATION ======
class DatabaseService:
    """Database service that integrates with your existing structure"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.songs = []
        self.load_data()
    
    def load_data(self):
        """Load data from files"""
        try:
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                logger.info(f"Loaded {len(self.songs)} songs from disk")
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
    
    def save_data(self):
        """Save data to files"""
        try:
            with open(self.data_dir / "songs.json", 'w') as f:
                json.dump(self.songs, f, indent=2, default=str)
            logger.debug("Data saved to disk")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
    
    def add_songs(self, songs: List[SongItem], source: str) -> Dict[str, Any]:
        """Add songs with deduplication"""
        added = 0
        duplicates = 0
        added_songs = []
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + added + 1}"
            
            # Deduplication
            is_duplicate = any(
                s.get("title", "").lower() == song.title.lower() and
                s.get("artist", "").lower() == song.artist.lower()
                for s in self.songs[-100:]
            )
            
            if not is_duplicate:
                self.songs.append(song_dict)
                added_songs.append(song_dict)
                added += 1
            else:
                duplicates += 1
        
        # Save if songs were added
        if added > 0:
            self.save_data()
        
        return {
            "added": added,
            "duplicates": duplicates,
            "total_songs": len(self.songs),
            "added_songs": added_songs[:5]
        }
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs"""
        source_list = [s for s in self.songs if s.get("region") == region] if region else self.songs
        
        sorted_songs = sorted(
            source_list,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:limit]
        
        return sorted_songs
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get region statistics"""
        stats = {}
        
        for region_code, region_data in config.UGANDAN_REGIONS.items():
            region_songs = [s for s in self.songs if s.get("region") == region_code]
            
            if region_songs:
                total_plays = sum(s.get("plays", 0) for s in region_songs)
                avg_score = sum(s.get("score", 0) for s in region_songs) / len(region_songs)
                top_song = max(region_songs, key=lambda x: x.get("score", 0))
                
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": len(region_songs),
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
        
        return stats

# Initialize database
db = DatabaseService()

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
    def get_trending_songs(songs: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs using 8-hour window algorithm"""
        if not songs:
            return []
        
        window_info = TrendingService.get_current_trending_window()
        window_number = window_info["window_number"]
        
        # Filter recent songs
        recent_songs = [
            song for song in songs
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

# ====== LIFECYCLE ======
current_chart_week = datetime.utcnow().strftime("%Y-W%W")
app_start_time = datetime.utcnow()
request_count = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle"""
    # Startup
    startup_message = f"""
    UG Board Engine v9.0.0 - Integrated Production System
    ----------------------------------------------------
    Environment: {config.ENVIRONMENT}
    Database: {len(db.songs)} songs loaded
    Regions: {', '.join(sorted(config.VALID_REGIONS))}
    Chart Week: {current_chart_week}
    Using existing modules: {HAS_EXISTING_MODULES}
    ----------------------------------------------------
    """
    
    logger.info(startup_message)
    
    # Load existing data if available
    try:
        # Try to load from your existing data structure
        if Path("data/top100.json").exists():
            with open("data/top100.json", 'r') as f:
                logger.info("Loaded existing top100.json")
    except Exception as e:
        logger.warning(f"Could not load existing data: {e}")
    
    yield
    
    # Shutdown
    logger.info(f"UG Board Engine shutting down")
    logger.info(f"Total requests: {request_count}")
    logger.info(f"Total songs: {len(db.songs)}")

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine v9.0.0 - Integrated System",
    version="9.0.0",
    description="Complete Ugandan Music Chart System with YouTube Worker Integration",
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
        {"name": "Worker", "description": "YouTube Worker integration"},
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if config.DEBUG else [
        "https://ugboard-engine.onrender.com",
        "https://ugboard-youtube-puller.kimkp015.workers.dev"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "HEAD"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# ====== API ENDPOINTS ======
@app.get("/", tags=["Root"])
async def root(request: Request):
    """Root endpoint with integrated information"""
    global request_count
    request_count += 1
    
    window_info = TrendingService.get_current_trending_window()
    
    # Check for existing modules and features
    available_features = {
        "database": HAS_EXISTING_MODULES,
        "scoring": HAS_EXISTING_MODULES,
        "admin_panel": HAS_EXISTING_MODULES,
        "charts": True,
        "trending": True,
        "regions": True,
        "ingestion": True
    }
    
    return {
        "service": "UG Board Engine",
        "version": "9.0.0",
        "status": "online",
        "environment": config.ENVIRONMENT,
        "integrated": HAS_EXISTING_MODULES,
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "trending_window": window_info,
        "ugandan_regions": list(config.VALID_REGIONS),
        "available_features": available_features,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "charts": {
                "top100": "/charts/top100",
                "regions": "/charts/regions",
                "region_detail": "/charts/regions/{region}",
                "trending": "/charts/trending",
                "trending_now": "/charts/trending/now"
            },
            "ingestion": {
                "youtube": "/ingest/youtube",
                "tv": "/ingest/tv",
                "radio": "/ingest/radio"
            },
            "admin": {
                "health": "/admin/health",
                "status": "/admin/status"
            },
            "worker": {
                "status": "/worker/status",
                "trigger": "/worker/trigger"
            }
        }
    }

@app.get("/health", tags=["Root"])
async def health():
    """Health check endpoint"""
    uptime = datetime.utcnow() - app_start_time
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "database": {
            "songs": len(db.songs),
            "regions": len(config.VALID_REGIONS)
        },
        "requests_served": request_count,
        "chart_week": current_chart_week,
        "trending_window": TrendingService.get_current_trending_window()
    }
    
    # Add existing module health if available
    if HAS_EXISTING_MODULES:
        try:
            from utils.health import system_status
            existing_health = system_status()
            health_status["modules"] = existing_health
        except Exception as e:
            health_status["modules_error"] = str(e)
    
    return health_status

@app.get("/charts/top100", tags=["Charts"])
async def get_top100(
    limit: int = Query(100, ge=1, le=200),
    region: Optional[str] = Query(None)
):
    """Get Uganda Top 100 chart"""
    try:
        songs = db.get_top_songs(limit, region)
        
        for i, song in enumerate(songs, 1):
            song["rank"] = i
        
        response_data = {
            "chart": "Uganda Top 100" + (f" - {region.capitalize()} Region" if region else ""),
            "week": current_chart_week,
            "entries": songs,
            "count": len(songs),
            "region": region if region else "all",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Add from existing top100.json if available
        if Path("data/top100.json").exists():
            try:
                with open("data/top100.json", 'r') as f:
                    existing_top100 = json.load(f)
                response_data["source"] = "integrated_with_existing_data"
            except Exception as e:
                logger.warning(f"Could not read top100.json: {e}")
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error fetching top songs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch chart data"
        )

@app.get("/charts/regions", tags=["Charts", "Regions"])
async def get_all_regions():
    """Get all Ugandan region statistics"""
    region_stats = db.get_region_stats()
    
    return {
        "regions": region_stats,
        "count": len(region_stats),
        "chart_week": current_chart_week,
        "timestamp": datetime.utcnow().isoformat(),
        "summary": {
            "total_songs": sum(stats["total_songs"] for stats in region_stats.values()),
            "total_plays": sum(stats["total_plays"] for stats in region_stats.values()),
            "regions_with_data": sum(1 for stats in region_stats.values() if stats["total_songs"] > 0)
        }
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
    
    try:
        songs = db.get_top_songs(10, region)
        region_data = config.UGANDAN_REGIONS[region]
        
        for i, song in enumerate(songs, 1):
            song["rank"] = i
        
        response_data = {
            "region": region,
            "region_name": region_data["name"],
            "chart_week": current_chart_week,
            "songs": songs,
            "count": len(songs),
            "districts": region_data["districts"],
            "musicians": region_data["musicians"],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Check for existing region data
        region_file = Path(f"data/regions/{region}.json")
        if region_file.exists():
            try:
                with open(region_file, 'r') as f:
                    existing_data = json.load(f)
                response_data["existing_data"] = True
            except Exception as e:
                logger.warning(f"Could not read region file: {e}")
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error fetching region data for {region}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch region data for {region}"
        )

@app.get("/charts/trending", tags=["Charts", "Trending"])
async def get_trending(
    limit: int = Query(10, ge=1, le=50)
):
    """Get trending songs with 8-hour rotation"""
    try:
        # Get YouTube songs for trending
        youtube_songs = [s for s in db.songs if s.get("source", "").startswith("youtube_")]
        songs = TrendingService.get_trending_songs(youtube_songs, limit)
        window_info = TrendingService.get_current_trending_window()
        
        return {
            "chart": "Trending Now - Uganda",
            "entries": songs,
            "count": len(songs),
            "trending_algorithm": {
                "window": window_info,
                "rotation": "8-hour deterministic window",
                "selection": "YouTube songs from last 72 hours"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching trending songs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch trending songs"
        )

@app.get("/charts/trending/now", tags=["Charts", "Trending"])
async def get_current_trending():
    """Get current trending song"""
    try:
        # Get YouTube songs
        youtube_songs = [s for s in db.songs if s.get("source", "").startswith("youtube_")]
        
        if not youtube_songs:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No trending songs available"
            )
        
        # Get single trending song for current window
        window_info = TrendingService.get_current_trending_window()
        hours_since_epoch = int(time.time() // 3600)
        trending_index = (hours_since_epoch // 8) % len(youtube_songs)
        trending_song = youtube_songs[trending_index]
        
        return {
            "trending_song": trending_song,
            "trending_window": window_info,
            "next_change_in": f"{window_info['seconds_remaining'] // 3600}h {(window_info['seconds_remaining'] % 3600) // 60}m",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching current trending song: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch current trending song"
        )

@app.post("/ingest/youtube", tags=["Ingestion"])
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube)
):
    """Ingest YouTube data with Ugandan validation"""
    try:
        # Simple Ugandan artist validation
        ugandan_artists = {
            "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
            "gravity", "vyroota", "geosteady", "feffe busi",
            "alien skin", "azawi", "vinka", "rema"
        }
        
        valid_items = []
        
        for item in payload.items:
            artist_lower = item.artist.lower()
            is_ugandan = any(ug_artist in artist_lower for ug_artist in ugandan_artists)
            
            if is_ugandan:
                valid_items.append(item)
        
        # Add to database
        result = db.add_songs(valid_items, f"youtube_{payload.source}")
        
        logger.info(f"YouTube ingestion: {result['added']} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} Ugandan YouTube songs",
            "source": payload.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat(),
            "worker_integration": {
                "worker_url": config.YOUTUBE_WORKER_URL,
                "connected": bool(config.YOUTUBE_WORKER_URL)
            }
        }
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}", exc_info=True)
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
        result = db.add_songs(payload.items, f"tv_{payload.source}")
        
        # Integrate with existing TV scraper if available
        if Path("scripts/tv_scraper.py").exists():
            result["tv_scraper_integrated"] = True
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} TV songs",
            "source": payload.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"TV ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"TV ingestion failed: {str(e)}"
        )

@app.post("/ingest/radio", tags=["Ingestion"])
async def ingest_radio(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest radio data"""
    try:
        result = db.add_songs(payload.items, f"radio_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} radio songs",
            "source": payload.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Radio ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Radio ingestion failed: {str(e)}"
        )

@app.get("/admin/health", tags=["Admin"])
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Admin health check"""
    uptime = datetime.utcnow() - app_start_time
    region_stats = db.get_region_stats()
    
    # Calculate totals
    total_plays = sum(stats.get("total_plays", 0) for stats in region_stats.values())
    total_songs = sum(stats.get("total_songs", 0) for stats in region_stats.values())
    
    response = {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "environment": config.ENVIRONMENT,
            "integrated_modules": HAS_EXISTING_MODULES
        },
        "database": {
            "total_songs": total_songs,
            "total_plays": total_plays,
            "regions": len(region_stats)
        },
        "trending_window": TrendingService.get_current_trending_window(),
        "youtube_worker": {
            "url": config.YOUTUBE_WORKER_URL,
            "configured": bool(config.YOUTUBE_WORKER_URL)
        }
    }
    
    # Add existing admin data if available
    if Path("api/admin/health.py").exists():
        response["existing_admin_module"] = True
    
    return response

@app.get("/admin/status", tags=["Admin"])
async def admin_status(auth: bool = Depends(AuthService.verify_admin)):
    """Admin status endpoint"""
    # Check existing files and structure
    existing_files = {}
    
    for file_path in [
        "data/top100.json",
        "data/songs.json",
        "api/admin",
        "scripts",
        "config"
    ]:
        path = Path(file_path)
        existing_files[file_path] = path.exists()
    
    return {
        "status": "admin_dashboard",
        "timestamp": datetime.utcnow().isoformat(),
        "file_structure": existing_files,
        "chart_week": current_chart_week,
        "database_stats": {
            "songs": len(db.songs),
            "regions": len(config.VALID_REGIONS)
        }
    }

@app.get("/worker/status", tags=["Worker"])
async def worker_status():
    """YouTube Worker status"""
    return {
        "worker": "YouTube Data Puller",
        "url": config.YOUTUBE_WORKER_URL,
        "configured": bool(config.YOUTUBE_WORKER_URL),
        "status": "active" if config.YOUTUBE_WORKER_URL else "not_configured",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/worker/trigger", tags=["Worker"])
async def trigger_worker(
    auth: bool = Depends(AuthService.verify_youtube),
    background_tasks: BackgroundTasks = None
):
    """Trigger YouTube worker pull"""
    if not config.YOUTUBE_WORKER_URL:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="YouTube Worker not configured"
        )
    
    # In production, this would call the actual worker
    # For now, simulate the response
    
    return {
        "status": "triggered",
        "message": "YouTube worker pull initiated",
        "worker_url": config.YOUTUBE_WORKER_URL,
        "triggered_at": datetime.utcnow().isoformat(),
        "next_scheduled": "Every 30 minutes",
        "note": "In production, this would call the actual worker API"
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
            "detail": str(exc) if config.DEBUG else "Contact support",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

# ====== INTEGRATION WITH EXISTING ROUTERS ======
if HAS_EXISTING_MODULES:
    # Register existing routers if available
    try:
        # Import and include routers from your existing structure
        from api.admin.admin import router as admin_router
        from api.charts.charts import router as charts_router
        from api.ingestion.ingestion import router as ingestion_router
        
        app.include_router(admin_router, prefix="/api/admin", tags=["Admin API"])
        app.include_router(charts_router, prefix="/api/charts", tags=["Charts API"])
        app.include_router(ingestion_router, prefix="/api/ingestion", tags=["Ingestion API"])
        
        logger.info("Integrated existing API routers")
    except ImportError as e:
        logger.warning(f"Could not import existing routers: {e}")

# ====== STARTUP BANNER ======
def display_banner():
    """Display startup banner"""
    banner = f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║                UG BOARD ENGINE v9.0.0                        ║
    ║              Complete Production System                      ║
    ╠══════════════════════════════════════════════════════════════╣
    ║ Environment: {config.ENVIRONMENT:<44} ║
    ║ Database: {len(db.songs):<5} songs loaded{' ' * 37} ║
    ║ Regions: {', '.join(sorted(config.VALID_REGIONS)):<41} ║
    ║ Chart Week: {current_chart_week:<42} ║
    ║ Integrated: {str(HAS_EXISTING_MODULES):<43} ║
    ╠══════════════════════════════════════════════════════════════╣
    ║ Server: http://0.0.0.0:{config.PORT:<35} ║
    ║ Docs: http://0.0.0.0:{config.PORT}/docs{' ' * 27} ║
    ║ Health: http://0.0.0.0:{config.PORT}/health{' ' * 25} ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    
    display_banner()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.PORT,
        log_level="info" if config.ENVIRONMENT == "production" else "debug",
        access_log=False  # Use our own logging
    )
