"""
UG Board Engine - Production-Ready Integrated System v10.0.0
Root-level main.py with proper integration to existing structure
"""

import os
import sys
import json
import time
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager

# Add the current directory to path for local imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Third-party imports
from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, ConfigDict
import uvicorn

# ====== CONFIGURATION ======
class Config:
    """Centralized configuration matching your .env.example"""
    
    # Environment
    ENVIRONMENT = os.getenv("ENV", "production").lower()
    DEBUG = ENVIRONMENT != "production"
    PORT = int(os.getenv("PORT", 8000))
    
    # Security Tokens - Use existing tokens from your structure
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
    
    # Paths
    BASE_DIR = current_dir
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"
    SCRIPTS_DIR = BASE_DIR / "scripts"
    
    # YouTube Integration
    YOUTUBE_WORKER_URL = os.getenv("YOUTUBE_WORKER_URL", "https://ugboard-youtube-puller.kimkp015.workers.dev")
    
    # Ugandan Regions (aligned with your existing structure)
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
    
    # Chart settings
    CHART_WEEK_FORMAT = "%Y-W%W"
    TRENDING_WINDOW_HOURS = 8
    
    @classmethod
    def setup_directories(cls):
        """Create necessary directories"""
        directories = [
            cls.DATA_DIR,
            cls.LOGS_DIR,
            cls.DATA_DIR / "regions",
            cls.DATA_DIR / "backups",
            cls.DATA_DIR / "cache"
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        return cls
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        cls.setup_directories()
        
        if cls.ENVIRONMENT == "production":
            if not cls.ADMIN_TOKEN or "replace_this" in cls.ADMIN_TOKEN:
                raise ValueError("ADMIN_TOKEN must be set in production")
            if not cls.INGEST_TOKEN or "replace_this" in cls.INGEST_TOKEN:
                raise ValueError("INGEST_TOKEN must be set in production")
        
        return cls

config = Config.validate()

# ====== LOGGING ======
logging.basicConfig(
    level=logging.DEBUG if config.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(config.LOGS_DIR / "ugboard.log")
    ]
)
logger = logging.getLogger("ugboard")

# ====== MODELS ======
class SongItem(BaseModel):
    """Song data model - aligned with your existing structure"""
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

# ====== DATA SERVICE ======
class DataService:
    """Service to integrate with your existing data structure"""
    
    def __init__(self):
        self.data_dir = config.DATA_DIR
        self.regions_dir = self.data_dir / "regions"
        
        # Initialize data structures
        self.songs = []
        self.top100 = []
        self.region_data = {}
        
        self.load_existing_data()
    
    def load_existing_data(self):
        """Load data from your existing JSON files"""
        try:
            # Load top100.json if exists
            top100_file = self.data_dir / "top100.json"
            if top100_file.exists():
                with open(top100_file, 'r') as f:
                    self.top100 = json.load(f)
                logger.info(f"Loaded {len(self.top100)} songs from top100.json")
            
            # Load songs.json if exists
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                logger.info(f"Loaded {len(self.songs)} songs from songs.json")
            
            # Load region files
            for region_file in self.regions_dir.glob("*.json"):
                region = region_file.stem
                try:
                    with open(region_file, 'r') as f:
                        self.region_data[region] = json.load(f)
                    logger.info(f"Loaded region data for {region}")
                except Exception as e:
                    logger.warning(f"Failed to load {region_file}: {e}")
            
            # If no songs loaded, use top100 as base
            if not self.songs and self.top100:
                self.songs = self.top100[:100]
                logger.info("Using top100 data as songs base")
                
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            # Initialize empty structures
            self.songs = []
            self.top100 = []
            self.region_data = {}
    
    def save_songs(self):
        """Save songs to JSON file"""
        try:
            songs_file = self.data_dir / "songs.json"
            with open(songs_file, 'w') as f:
                json.dump(self.songs, f, indent=2, default=str)
            logger.debug(f"Saved {len(self.songs)} songs to songs.json")
        except Exception as e:
            logger.error(f"Failed to save songs: {e}")
    
    def save_top100(self, top100_data):
        """Save top100 data to JSON file"""
        try:
            top100_file = self.data_dir / "top100.json"
            with open(top100_file, 'w') as f:
                json.dump(top100_data, f, indent=2, default=str)
            logger.info(f"Saved top100 data to top100.json")
        except Exception as e:
            logger.error(f"Failed to save top100: {e}")
    
    def save_region_data(self, region: str, data: Dict[str, Any]):
        """Save region data to JSON file"""
        try:
            region_file = self.regions_dir / f"{region}.json"
            with open(region_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.debug(f"Saved data for region {region}")
        except Exception as e:
            logger.error(f"Failed to save region data for {region}: {e}")
    
    def add_songs(self, songs: List[SongItem], source: str) -> Dict[str, Any]:
        """Add songs with deduplication and integration"""
        added = 0
        duplicates = 0
        added_songs = []
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + added + 1}"
            
            # Simple deduplication (title + artist in last 1000 songs)
            is_duplicate = any(
                s.get("title", "").lower() == song.title.lower() and
                s.get("artist", "").lower() == song.artist.lower()
                for s in self.songs[-1000:]
            )
            
            if not is_duplicate:
                self.songs.append(song_dict)
                added_songs.append(song_dict)
                added += 1
            else:
                duplicates += 1
        
        # Save if songs were added
        if added > 0:
            self.save_songs()
            
            # Also update region data
            region = songs[0].region if songs else "central"
            if region in config.VALID_REGIONS:
                region_songs = [s for s in self.songs if s.get("region") == region]
                self.save_region_data(region, {
                    "songs": region_songs[:50],
                    "updated_at": datetime.utcnow().isoformat()
                })
        
        return {
            "added": added,
            "duplicates": duplicates,
            "total_songs": len(self.songs),
            "added_songs": added_songs[:3]  # Return first 3 for preview
        }
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs"""
        # Use existing top100 if available and region not specified
        if not region and self.top100:
            return self.top100[:limit]
        
        # Filter by region if specified
        source_list = [s for s in self.songs if s.get("region") == region] if region else self.songs
        
        # Sort by score
        sorted_songs = sorted(
            source_list,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:limit]
        
        return sorted_songs
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get region statistics from existing data"""
        stats = {}
        
        for region_code, region_data in config.UGANDAN_REGIONS.items():
            # Try to get from loaded region data first
            if region_code in self.region_data:
                region_songs = self.region_data[region_code].get("songs", [])
            else:
                # Fallback to filtering songs
                region_songs = [s for s in self.songs if s.get("region") == region_code]
            
            if region_songs:
                total_plays = sum(s.get("plays", 0) for s in region_songs)
                avg_score = sum(s.get("score", 0) for s in region_songs) / len(region_songs) if region_songs else 0
                
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": len(region_songs),
                    "total_plays": total_plays,
                    "average_score": round(avg_score, 2),
                    "districts": region_data["districts"],
                    "musicians": region_data["musicians"],
                    "data_source": "existing" if region_code in self.region_data else "calculated"
                }
        
        return stats

# Initialize data service
data_service = DataService()

# ====== TRENDING SERVICE ======
class TrendingService:
    """8-hour trending window service"""
    
    @staticmethod
    def get_current_trending_window() -> Dict[str, Any]:
        """Get current 8-hour window information"""
        current_time = time.time()
        hours_since_epoch = int(current_time // 3600)
        
        window_number = hours_since_epoch // config.TRENDING_WINDOW_HOURS
        window_start_hour = (window_number * config.TRENDING_WINDOW_HOURS) % 24
        window_end_hour = (window_start_hour + config.TRENDING_WINDOW_HOURS) % 24
        
        next_window_start = (window_number + 1) * config.TRENDING_WINDOW_HOURS * 3600
        seconds_remaining = max(0, next_window_start - current_time)
        
        return {
            "window_number": window_number,
            "window_start_utc": f"{window_start_hour:02d}:00",
            "window_end_utc": f"{window_end_hour:02d}:00",
            "seconds_remaining": int(seconds_remaining),
            "hours_remaining": int(seconds_remaining // 3600),
            "description": f"{config.TRENDING_WINDOW_HOURS}-hour window {window_start_hour:02d}:00-{window_end_hour:02d}:00 UTC"
        }
    
    @staticmethod
    def get_trending_songs(all_songs: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs using deterministic 8-hour window algorithm"""
        if not all_songs:
            return []
        
        window_info = TrendingService.get_current_trending_window()
        window_number = window_info["window_number"]
        
        # Filter recent songs (last 72 hours)
        recent_songs = [
            song for song in all_songs
            if datetime.fromisoformat(song.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
            datetime.utcnow() - timedelta(hours=72)
        ]
        
        if not recent_songs:
            return []
        
        # Deterministic selection based on window number
        def trending_score(song: Dict[str, Any]) -> int:
            # Create a deterministic hash based on window number and song ID
            combined = f"{window_number}_{song.get('id', '0')}_{song.get('title', '')}"
            return hash(combined) % 10000
        
        sorted_songs = sorted(recent_songs, key=trending_score, reverse=True)[:limit]
        
        # Add trend information
        for i, song in enumerate(sorted_songs, 1):
            song["trend_rank"] = i
            song["trend_window"] = window_info["window_number"]
            song["trend_score"] = trending_score(song)
        
        return sorted_songs

# ====== AUTHENTICATION ======
security = HTTPBearer(auto_error=False)

class AuthService:
    """Unified authentication service"""
    
    @staticmethod
    def verify_token(
        token_type: str,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify token based on type"""
        expected_token = getattr(config, f"{token_type.upper()}_TOKEN", None)
        
        if not expected_token:
            logger.error(f"{token_type.upper()}_TOKEN not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"{token_type} authentication not configured"
            )
        
        if not credentials:
            logger.warning(f"Missing credentials for {token_type}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Missing {token_type} token"
            )
        
        if credentials.credentials != expected_token:
            logger.warning(f"Invalid {token_type} token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid {token_type} token"
            )
        
        logger.debug(f"Successful {token_type} authentication")
        return True
    
    @staticmethod
    def verify_admin(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("admin", credentials)
    
    @staticmethod
    def verify_ingest(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("ingest", credentials)
    
    @staticmethod
    def verify_youtube(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("youtube", credentials)

# ====== GLOBAL STATE ======
current_chart_week = datetime.utcnow().strftime(config.CHART_WEEK_FORMAT)
app_start_time = datetime.utcnow()
request_count = 0

# ====== LIFECYCLE ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    
    # Startup
    logger.info("=" * 60)
    logger.info(f"🚀 UG Board Engine v10.0.0 Starting Up")
    logger.info(f"📅 Chart Week: {current_chart_week}")
    logger.info(f"🗺️  Regions: {', '.join(sorted(config.VALID_REGIONS))}")
    logger.info(f"🗃️  Data: {len(data_service.songs)} songs loaded")
    logger.info(f"🔐 Environment: {config.ENVIRONMENT}")
    logger.info("=" * 60)
    
    # Load and validate existing scripts
    if (config.SCRIPTS_DIR / "tv_scraper.py").exists():
        logger.info("✅ TV Scraper script found")
    if (config.SCRIPTS_DIR / "radio_scraper.py").exists():
        logger.info("✅ Radio Scraper script found")
    
    # Create initial data if none exists
    if not data_service.songs and not data_service.top100:
        logger.info("📝 No existing data found, creating sample data")
        sample_song = SongItem(
            title="Welcome to UG Board",
            artist="System",
            plays=100,
            score=50.0,
            region="central",
            station="UG Board Engine"
        )
        data_service.add_songs([sample_song], "system_init")
    
    yield
    
    # Shutdown
    logger.info("=" * 60)
    logger.info(f"🛑 UG Board Engine Shutting Down")
    logger.info(f"📊 Total Requests: {request_count}")
    logger.info(f"🎵 Total Songs: {len(data_service.songs)}")
    logger.info(f"💾 Saving final data...")
    data_service.save_songs()
    logger.info("=" * 60)

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine v10.0.0",
    version="10.0.0",
    description="Complete Ugandan Music Chart System - Root Level Integration",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Root", "description": "Service information and health"},
        {"name": "Charts", "description": "Music chart endpoints"},
        {"name": "Regions", "description": "Ugandan regional charts and statistics"},
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
        "https://*.onrender.com",
        "https://*.workers.dev",
        "https://ugboard.com"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Mount static files for monitor.html
if (config.BASE_DIR / "monitor.html").exists():
    app.mount("/static", StaticFiles(directory=config.BASE_DIR), name="static")

# ====== API ENDPOINTS ======

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with integrated system information"""
    global request_count
    request_count += 1
    
    window_info = TrendingService.get_current_trending_window()
    region_stats = data_service.get_region_stats()
    
    # Check for existing modules
    existing_modules = {
        "admin_panel": (config.BASE_DIR / "api" / "admin").exists(),
        "charts_api": (config.BASE_DIR / "api" / "charts").exists(),
        "ingestion_api": (config.BASE_DIR / "api" / "ingestion").exists(),
        "scripts": (config.BASE_DIR / "scripts").exists(),
        "tests": (config.BASE_DIR / "tests").exists(),
    }
    
    return {
        "service": "UG Board Engine",
        "version": "10.0.0",
        "status": "online",
        "environment": config.ENVIRONMENT,
        "integrated": True,
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "trending_window": window_info,
        "data_summary": {
            "songs": len(data_service.songs),
            "regions": len(region_stats),
            "top100_exists": len(data_service.top100) > 0
        },
        "existing_modules": existing_modules,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "monitor": "/monitor" if (config.BASE_DIR / "monitor.html").exists() else None,
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
                "status": "/admin/status",
                "data": "/admin/data"
            }
        }
    }

@app.get("/health", tags=["Root"])
async def health():
    """Comprehensive health check endpoint"""
    uptime = datetime.utcnow() - app_start_time
    
    # Check system health
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "uptime_human": str(uptime).split('.')[0],
        "requests_served": request_count,
        "memory_usage_mb": "N/A",  # Could add psutil for actual metrics
        "data": {
            "songs": len(data_service.songs),
            "top100": len(data_service.top100),
            "regions": len(data_service.region_data)
        },
        "chart_week": current_chart_week,
        "trending_window": TrendingService.get_current_trending_window(),
        "environment": config.ENVIRONMENT
    }
    
    # Check critical paths
    critical_paths = [
        ("data_dir", config.DATA_DIR, config.DATA_DIR.exists()),
        ("logs_dir", config.LOGS_DIR, config.LOGS_DIR.exists()),
        ("scripts_dir", config.SCRIPTS_DIR, config.SCRIPTS_DIR.exists()),
    ]
    
    health_status["paths"] = {
        name: {"path": str(path), "exists": exists}
        for name, path, exists in critical_paths
    }
    
    # Check if all critical paths exist
    all_paths_ok = all(exists for _, _, exists in critical_paths)
    if not all_paths_ok:
        health_status["status"] = "degraded"
        health_status["issues"] = "Some directories missing"
    
    return health_status

@app.get("/monitor", tags=["Root"], include_in_schema=False)
async def monitor_page():
    """Serve monitor.html if it exists"""
    monitor_file = config.BASE_DIR / "monitor.html"
    if monitor_file.exists():
        return FileResponse(monitor_file)
    raise HTTPException(status_code=404, detail="Monitor page not found")

@app.get("/charts/top100", tags=["Charts"])
async def get_top100(
    limit: int = Query(100, ge=1, le=200, description="Number of songs to return"),
    region: Optional[str] = Query(None, description="Filter by region")
):
    """Get Uganda Top 100 chart - Integrates with existing top100.json"""
    try:
        # Get songs
        songs = data_service.get_top_songs(limit, region)
        
        # Add ranks
        for i, song in enumerate(songs, 1):
            song["rank"] = i
            song["change"] = "stable"  # Placeholder for change tracking
        
        response = {
            "chart": "Uganda Top 100" + (f" - {region.capitalize()}" if region else ""),
            "week": current_chart_week,
            "entries": songs,
            "count": len(songs),
            "region": region if region else "all",
            "data_source": "existing_top100" if not region and data_service.top100 else "calculated",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error in /charts/top100: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch top100 chart: {str(e)}"
        )

@app.get("/charts/regions", tags=["Charts", "Regions"])
async def get_all_regions():
    """Get statistics for all Ugandan regions"""
    try:
        region_stats = data_service.get_region_stats()
        
        return {
            "regions": region_stats,
            "count": len(region_stats),
            "chart_week": current_chart_week,
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_songs": sum(stats.get("total_songs", 0) for stats in region_stats.values()),
                "total_plays": sum(stats.get("total_plays", 0) for stats in region_stats.values()),
                "regions_with_data": sum(1 for stats in region_stats.values() if stats.get("total_songs", 0) > 0)
            }
        }
        
    except Exception as e:
        logger.error(f"Error in /charts/regions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch region statistics: {str(e)}"
        )

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"])
async def get_region_detail(
    region: str = FPath(..., description="Ugandan region: central, eastern, western, northern")
):
    """Get detailed information for a specific region"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    try:
        # Get top songs for region
        songs = data_service.get_top_songs(10, region)
        region_data = config.UGANDAN_REGIONS[region]
        
        # Add ranks
        for i, song in enumerate(songs, 1):
            song["rank"] = i
        
        response = {
            "region": region,
            "region_name": region_data["name"],
            "chart_week": current_chart_week,
            "songs": songs,
            "count": len(songs),
            "districts": region_data["districts"],
            "musicians": region_data["musicians"],
            "data_source": "existing" if region in data_service.region_data else "calculated",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error in /charts/regions/{region}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch region data for {region}: {str(e)}"
        )

@app.get("/charts/trending", tags=["Charts", "Trending"])
async def get_trending(
    limit: int = Query(10, ge=1, le=50, description="Number of trending songs")
):
    """Get trending songs with 8-hour rotation"""
    try:
        # Get all songs
        all_songs = data_service.songs
        
        # Get trending songs using the algorithm
        trending_songs = TrendingService.get_trending_songs(all_songs, limit)
        window_info = TrendingService.get_current_trending_window()
        
        return {
            "chart": "Trending Now - Uganda",
            "algorithm": "8-hour deterministic window rotation",
            "entries": trending_songs,
            "count": len(trending_songs),
            "window_info": window_info,
            "next_change_in": f"{window_info['hours_remaining']}h {window_info['seconds_remaining'] % 3600 // 60}m",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /charts/trending: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch trending songs: {str(e)}"
        )

@app.get("/charts/trending/now", tags=["Charts", "Trending"])
async def get_current_trending():
    """Get the currently trending song for this window"""
    try:
        # Get all songs
        all_songs = data_service.songs
        
        if not all_songs:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No songs available for trending"
            )
        
        # Get trending songs (just 1 for current trending)
        trending_songs = TrendingService.get_trending_songs(all_songs, 1)
        window_info = TrendingService.get_current_trending_window()
        
        if not trending_songs:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No trending song available for current window"
            )
        
        current_song = trending_songs[0]
        
        return {
            "trending_song": current_song,
            "trending_window": window_info,
            "next_change_in": f"{window_info['hours_remaining']}h {window_info['seconds_remaining'] % 3600 // 60}m",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in /charts/trending/now: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch current trending song: {str(e)}"
        )

@app.post("/ingest/youtube", tags=["Ingestion"])
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube)
):
    """Ingest YouTube data with Ugandan artist validation"""
    try:
        # Ugandan artist validation (case-insensitive)
        ugandan_artists = {
            "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
            "gravity", "vyroota", "geosteady", "feffe busi",
            "alien skin", "azawi", "vinka", "rema", "rickman",
            "fik fameica", "john blaq", "dax", "vivian tosh"
        }
        
        # Filter for Ugandan artists
        valid_items = []
        for item in payload.items:
            artist_lower = item.artist.lower()
            is_ugandan = any(ug_artist in artist_lower for ug_artist in ugandan_artists)
            
            if is_ugandan:
                valid_items.append(item)
        
        if not valid_items:
            logger.warning(f"YouTube ingestion from {payload.source}: No Ugandan artists found")
            return {
                "status": "filtered",
                "message": "No Ugandan artists found in payload",
                "filtered_count": len(payload.items),
                "passed_count": 0
            }
        
        # Add to database
        result = data_service.add_songs(valid_items, f"youtube_{payload.source}")
        
        logger.info(f"YouTube ingestion: {result['added']} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} Ugandan YouTube songs",
            "source": payload.source,
            "results": result,
            "validation": {
                "ugandan_artists_checked": len(ugandan_artists),
                "passed_validation": len(valid_items),
                "failed_validation": len(payload.items) - len(valid_items)
            },
            "timestamp": datetime.utcnow().isoformat(),
            "worker_integration": {
                "worker_url": config.YOUTUBE_WORKER_URL,
                "configured": bool(config.YOUTUBE_WORKER_URL)
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
    """Ingest TV data - Integrates with existing TV scraper"""
    try:
        # Check for TV scraper integration
        tv_scraper_exists = (config.SCRIPTS_DIR / "tv_scraper.py").exists()
        
        # Add songs
        result = data_service.add_songs(payload.items, f"tv_{payload.source}")
        
        logger.info(f"TV ingestion: {result['added']} songs from {payload.source}")
        
        response = {
            "status": "success",
            "message": f"Ingested {result['added']} TV songs",
            "source": payload.source,
            "results": result,
            "tv_scraper_integration": {
                "scraper_exists": tv_scraper_exists,
                "scraper_path": str(config.SCRIPTS_DIR / "tv_scraper.py") if tv_scraper_exists else None
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return response
        
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
    """Ingest radio data - Integrates with existing radio scraper"""
    try:
        # Check for radio scraper integration
        radio_scraper_exists = (config.SCRIPTS_DIR / "radio_scraper.py").exists()
        
        # Add songs
        result = data_service.add_songs(payload.items, f"radio_{payload.source}")
        
        logger.info(f"Radio ingestion: {result['added']} songs from {payload.source}")
        
        response = {
            "status": "success",
            "message": f"Ingested {result['added']} radio songs",
            "source": payload.source,
            "results": result,
            "radio_scraper_integration": {
                "scraper_exists": radio_scraper_exists,
                "scraper_path": str(config.SCRIPTS_DIR / "radio_scraper.py") if radio_scraper_exists else None
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Radio ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Radio ingestion failed: {str(e)}"
        )

@app.get("/admin/health", tags=["Admin"])
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Admin health check with detailed system information"""
    uptime = datetime.utcnow() - app_start_time
    region_stats = data_service.get_region_stats()
    window_info = TrendingService.get_current_trending_window()
    
    # Calculate totals
    total_songs = sum(stats.get("total_songs", 0) for stats in region_stats.values())
    total_plays = sum(stats.get("total_plays", 0) for stats in region_stats.values())
    
    # Check existing API modules
    existing_api_modules = []
    api_base = config.BASE_DIR / "api"
    if api_base.exists():
        existing_api_modules = [d.name for d in api_base.iterdir() if d.is_dir()]
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "environment": config.ENVIRONMENT,
            "start_time": app_start_time.isoformat(),
            "python_version": sys.version
        },
        "database": {
            "total_songs": total_songs,
            "total_plays": total_plays,
            "regions_with_data": sum(1 for stats in region_stats.values() if stats.get("total_songs", 0) > 0),
            "top100_entries": len(data_service.top100),
            "existing_data_files": list(data_service.region_data.keys())
        },
        "trending": {
            "current_window": window_info,
            "algorithm": "8-hour deterministic rotation"
        },
        "integrations": {
            "youtube_worker": {
                "url": config.YOUTUBE_WORKER_URL,
                "configured": bool(config.YOUTUBE_WORKER_URL)
            },
            "existing_api_modules": existing_api_modules,
            "scripts_available": len(list(config.SCRIPTS_DIR.glob("*.py")))
        }
    }

@app.get("/admin/status", tags=["Admin"])
async def admin_status(auth: bool = Depends(AuthService.verify_admin)):
    """Admin status dashboard"""
    # Collect file and directory information
    file_info = {}
    
    important_paths = [
        ("data_dir", config.DATA_DIR),
        ("logs_dir", config.LOGS_DIR),
        ("scripts_dir", config.SCRIPTS_DIR),
        ("api_dir", config.BASE_DIR / "api"),
        ("models_dir", config.BASE_DIR / "models"),
        ("config_dir", config.BASE_DIR / "config"),
    ]
    
    for name, path in important_paths:
        if path.exists():
            if path.is_dir():
                items = list(path.iterdir())
                file_info[name] = {
                    "exists": True,
                    "type": "directory",
                    "item_count": len(items),
                    "items": [item.name for item in items[:5]]  # First 5 items
                }
            else:
                file_info[name] = {
                    "exists": True,
                    "type": "file",
                    "size_bytes": path.stat().st_size if path.exists() else 0
                }
        else:
            file_info[name] = {"exists": False}
    
    # Count Python files
    python_files = list(config.BASE_DIR.rglob("*.py"))
    
    return {
        "status": "admin_dashboard",
        "timestamp": datetime.utcnow().isoformat(),
        "file_structure": file_info,
        "python_files": len(python_files),
        "chart_week": current_chart_week,
        "data_summary": {
            "songs": len(data_service.songs),
            "top100": len(data_service.top100),
            "regions": len(data_service.region_data)
        }
    }

@app.get("/admin/data", tags=["Admin"])
async def admin_data(auth: bool = Depends(AuthService.verify_admin)):
    """Admin data management endpoint"""
    # Get data statistics
    data_files = []
    for file_path in config.DATA_DIR.rglob("*.json"):
        try:
            stat = file_path.stat()
            data_files.append({
                "name": file_path.name,
                "path": str(file_path.relative_to(config.BASE_DIR)),
                "size_kb": round(stat.st_size / 1024, 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
        except Exception as e:
            logger.warning(f"Could not stat {file_path}: {e}")
    
    # Get recent songs
    recent_songs = sorted(
        data_service.songs,
        key=lambda x: x.get("ingested_at", ""),
        reverse=True
    )[:10]
    
    return {
        "status": "data_management",
        "timestamp": datetime.utcnow().isoformat(),
        "data_files": sorted(data_files, key=lambda x: x["size_kb"], reverse=True)[:10],
        "recent_songs": recent_songs,
        "actions": {
            "save_songs": "/admin/data/save",
            "reload_data": "/admin/data/reload",
            "backup": "/admin/data/backup"
        }
    }

@app.post("/admin/data/save", tags=["Admin"])
async def admin_save_data(auth: bool = Depends(AuthService.verify_admin)):
    """Force save all data"""
    try:
        data_service.save_songs()
        
        # Also save top100 if exists
        if data_service.top100:
            data_service.save_top100(data_service.top100)
        
        return {
            "status": "success",
            "message": "Data saved successfully",
            "timestamp": datetime.utcnow().isoformat(),
            "saved_files": ["songs.json", "top100.json"]
        }
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save data: {str(e)}"
        )

@app.get("/worker/status", tags=["Worker"])
async def worker_status():
    """YouTube Worker status check"""
    return {
        "worker": "YouTube Data Puller",
        "url": config.YOUTUBE_WORKER_URL,
        "configured": bool(config.YOUTUBE_WORKER_URL),
        "status": "active" if config.YOUTUBE_WORKER_URL else "not_configured",
        "integration": {
            "endpoint": "/ingest/youtube",
            "authentication": "Bearer token required",
            "payload_format": "YouTubeIngestPayload"
        },
        "timestamp": datetime.utcnow().isoformat()
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions"""
    logger.warning(f"HTTP {exc.status_code} at {request.url.path}: {exc.detail}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "method": request.method
        },
        headers={"Cache-Control": "no-store"}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unhandled exceptions"""
    logger.error(f"Unhandled exception at {request.url.path}: {exc}", exc_info=True)
    
    error_detail = str(exc) if config.DEBUG else "Internal server error"
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": error_detail,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "request_id": getattr(request.state, "request_id", "unknown")
        },
        headers={"Cache-Control": "no-store"}
    )

# ====== INTEGRATION WITH EXISTING MODULES ======
def integrate_existing_modules():
    """Attempt to integrate with existing API modules"""
    try:
        # Check for existing API structure
        api_base = config.BASE_DIR / "api"
        if api_base.exists():
            logger.info("Found existing API structure, attempting integration...")
            
            # Import existing routers if they exist
            routers_to_import = []
            
            # Admin router
            admin_router_path = api_base / "admin" / "admin.py"
            if admin_router_path.exists():
                try:
                    # Dynamic import
                    import importlib.util
                    spec = importlib.util.spec_from_file_location("admin_router", admin_router_path)
                    admin_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(admin_module)
                    
                    if hasattr(admin_module, 'router'):
                        app.include_router(admin_module.router, prefix="/api/v1/admin", tags=["Admin API"])
                        routers_to_import.append("admin")
                        logger.info("✅ Integrated existing admin router")
                except Exception as e:
                    logger.warning(f"Could not import admin router: {e}")
            
            # Charts router
            charts_router_path = api_base / "charts" / "charts.py"
            if charts_router_path.exists():
                try:
                    spec = importlib.util.spec_from_file_location("charts_router", charts_router_path)
                    charts_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(charts_module)
                    
                    if hasattr(charts_module, 'router'):
                        app.include_router(charts_module.router, prefix="/api/v1/charts", tags=["Charts API"])
                        routers_to_import.append("charts")
                        logger.info("✅ Integrated existing charts router")
                except Exception as e:
                    logger.warning(f"Could not import charts router: {e}")
            
            if routers_to_import:
                logger.info(f"Successfully integrated modules: {', '.join(routers_to_import)}")
            else:
                logger.info("No existing routers found to integrate")
    
    except Exception as e:
        logger.warning(f"Failed to integrate existing modules: {e}")

# Call integration function
integrate_existing_modules()

# ====== STARTUP BANNER ======
def display_startup_banner():
    """Display a professional startup banner"""
    banner = f"""
    ╔{'═' * 60}╗
    ║{'UG BOARD ENGINE v10.0.0 - Production System':^60}║
    ╠{'═' * 60}╣
    ║ {'Environment:':<20} {config.ENVIRONMENT:<38} ║
    ║ {'Chart Week:':<20} {current_chart_week:<38} ║
    ║ {'Data Songs:':<20} {len(data_service.songs):<38} ║
    ║ {'Regions:':<20} {', '.join(sorted(config.VALID_REGIONS)):<38} ║
    ║ {'Python:':<20} {sys.version.split()[0]:<38} ║
    ╠{'═' * 60}╣
    ║ {'Server:':<20} http://0.0.0.0:{config.PORT:<35} ║
    ║ {'Docs:':<20} http://0.0.0.0:{config.PORT}/docs{' ' * 28} ║
    ║ {'Health:':<20} http://0.0.0.0:{config.PORT}/health{' ' * 26} ║
    ╚{'═' * 60}╝
    """
    print(banner)

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    display_startup_banner()
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=config.PORT,
        log_level="info",
        reload=config.DEBUG,
        access_log=True,
        proxy_headers=True
    )
