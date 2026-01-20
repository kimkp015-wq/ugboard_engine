"""
UG Board Engine - Production Ready Architecture v8.4.0
World-Class Implementation for Ugandan Music Charts
"""

import os
import sys
import json
import time
import asyncio
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import asynccontextmanager
from enum import Enum

# Optional imports with fallbacks
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False
    aiofiles = None

from fastapi import (
    FastAPI, HTTPException, Header, Depends, Query, 
    Path as FPath, Request, status, Response
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator, ConfigDict

# ============================================================================
# CONFIGURATION
# ============================================================================

class Environment(str, Enum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"

class Config:
    """Configuration management"""
    
    # Required tokens (from environment)
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "ug-board-ingest-2025")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "ug-board-internal-2025")
    
    # Environment
    ENVIRONMENT = Environment(os.getenv("ENV", "production"))
    
    # Paths
    DATA_DIR = Path("data")
    LOGS_DIR = Path("logs")
    
    # Database settings
    MAX_DB_SIZE = int(os.getenv("MAX_DB_SIZE", "10000"))
    TRENDING_WINDOW_HOURS = int(os.getenv("TRENDING_WINDOW_HOURS", "8"))
    CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 minutes
    
    # Redis (optional)
    REDIS_URL = os.getenv("REDIS_URL")
    
    # Performance
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
    
    # Ugandan Regions
    UGANDAN_REGIONS = {
        "central": {
            "name": "Central Region",
            "districts": ["Kampala", "Wakiso", "Mukono", "Luwero", "Masaka"],
            "musicians": ["Alien Skin", "Azawi", "Sheebah Karungi", "Vinka", 
                         "Eddy Kenzo", "Bobi Wine", "Daddy Andre"]
        },
        "western": {
            "name": "Western Region",
            "districts": ["Mbarara", "Kasese", "Ntungamo", "Kabale", 
                         "Fort Portal", "Hoima"],
            "musicians": ["Ray G", "T-Paul", "Truth 256", "Sister Charity", 
                         "Omega 256", "Rema Namakula"]
        },
        "eastern": {
            "name": "Eastern Region",
            "districts": ["Jinja", "Mbale", "Tororo", "Mayuge", "Soroti", "Iganga"],
            "musicians": ["Victor Ruz", "Davido Spider", "Temperature Touch", 
                         "Idi Amasaba", "Rexy", "Geosteady"]
        },
        "northern": {
            "name": "Northern Region",
            "districts": ["Gulu", "Lira", "Arua", "Kitgum"],
            "musicians": ["Bosmic Otim", "Odongo Romeo", "Eezzy", 
                         "Jenneth Prischa", "Laxzy Mover", "Fik Fameica"]
        }
    }
    
    VALID_REGIONS = set(UGANDAN_REGIONS.keys())
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        cls.DATA_DIR.mkdir(exist_ok=True, parents=True)
        cls.LOGS_DIR.mkdir(exist_ok=True, parents=True)
        return cls

config = Config.validate()

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging():
    """Configure logging"""
    logs_dir = config.LOGS_DIR
    
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

# ============================================================================
# EXCEPTIONS
# ============================================================================

class UGBaseException(Exception):
    """Base exception for UG Board Engine"""
    def __init__(self, message: str, code: str = "INTERNAL_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)

class ValidationError(UGBaseException):
    """Validation errors"""
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")

class AuthenticationError(UGBaseException):
    """Authentication errors"""
    def __init__(self, message: str):
        super().__init__(message, "AUTHENTICATION_ERROR")

class DatabaseError(UGBaseException):
    """Database errors"""
    def __init__(self, message: str):
        super().__init__(message, "DATABASE_ERROR")

# ============================================================================
# MODELS
# ============================================================================

class SongItem(BaseModel):
    """Song data model"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=100)
    plays: int = Field(default=0, ge=0)
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    station: Optional[str] = Field(None, max_length=50)
    region: str = Field("central", pattern="^(central|western|eastern|northern)$")
    district: Optional[str] = Field(None, max_length=50)
    timestamp: Optional[str] = Field(None)
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISO 8601 timestamp"""
        if v:
            try:
                if v.endswith('Z'):
                    v = v[:-1] + '+00:00'
                datetime.fromisoformat(v)
            except (ValueError, AttributeError):
                raise ValueError('Invalid ISO 8601 timestamp format')
        return v

class IngestPayload(BaseModel):
    """Ingestion payload"""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000)
    source: str = Field(..., min_length=1, max_length=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)

class YouTubeIngestPayload(IngestPayload):
    """YouTube ingestion payload"""
    channel_id: Optional[str] = Field(None, max_length=50)
    video_id: Optional[str] = Field(None, max_length=20)
    category: str = Field(default="music", max_length=50)

# ============================================================================
# TRENDING SERVICE
# ============================================================================

class TrendingService:
    """Trending songs service with 8-hour window rotation"""
    
    @staticmethod
    def get_current_trending_window() -> Dict[str, Any]:
        """Get current trending window information"""
        current_time = time.time()
        hours_since_epoch = int(current_time // 3600)
        
        window_number = hours_since_epoch // config.TRENDING_WINDOW_HOURS
        window_start_hour = (window_number * config.TRENDING_WINDOW_HOURS) % 24
        window_end_hour = (window_start_hour + config.TRENDING_WINDOW_HOURS) % 24
        
        next_window_start = (window_number + 1) * config.TRENDING_WINDOW_HOURS * 3600
        seconds_remaining = next_window_start - current_time
        
        return {
            "window_number": window_number,
            "window_start_hour": window_start_hour,
            "window_end_hour": window_end_hour,
            "seconds_remaining": int(seconds_remaining),
            "current_hour": datetime.utcnow().hour,
            "description": f"{config.TRENDING_WINDOW_HOURS}-hour window {window_start_hour:02d}:00 - {window_end_hour:02d}:00 UTC"
        }
    
    @staticmethod
    def get_trending_songs(youtube_songs: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs based on YouTube ingestion"""
        if not youtube_songs:
            return []
        
        # Get current window
        window_info = TrendingService.get_current_trending_window()
        window_number = window_info["window_number"]
        
        # Filter recent songs (last 3 days)
        recent_cutoff = datetime.utcnow() - timedelta(days=3)
        recent_songs = [
            song for song in youtube_songs
            if datetime.fromisoformat(song.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) > recent_cutoff
        ]
        
        if not recent_songs:
            return []
        
        # Deterministic ordering based on window number
        def trending_score(song: Dict[str, Any]) -> float:
            base_score = song.get("score", 0) * 0.6 + song.get("plays", 0) * 0.4
            window_hash = hash(f"{window_number}_{song.get('id', '')}") % 1000
            return base_score + (window_hash / 1000.0)
        
        sorted_songs = sorted(recent_songs, key=trending_score, reverse=True)
        return sorted_songs[:limit]

# ============================================================================
# DATABASE LAYER
# ============================================================================

class JSONDatabase:
    """File-based JSON database with Ugandan regions support"""
    
    def __init__(self, data_dir: Path, redis_url: Optional[str] = None):
        self.data_dir = data_dir
        self._redis = None
        self._redis_url = redis_url
        
        # Initialize data structures
        self.songs: List[Dict[str, Any]] = []
        self.chart_history: List[Dict[str, Any]] = []
        self.regions: Dict[str, Dict[str, Any]] = {}
        self.youtube_songs: List[Dict[str, Any]] = []
        
        # Initialize regions from config
        for region_code, region_data in config.UGANDAN_REGIONS.items():
            self.regions[region_code] = {
                "name": region_data["name"],
                "districts": region_data["districts"],
                "musicians": region_data["musicians"],
                "songs": []
            }
        
        # Initialize Redis if available
        self._init_redis()
        
        # Load existing data
        self._load_data()
    
    def _init_redis(self):
        """Initialize Redis connection if available"""
        if self._redis_url and REDIS_AVAILABLE:
            try:
                self._redis = redis.from_url(
                    self._redis_url,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    decode_responses=True
                )
                self._redis.ping()
                logger.info("Redis cache initialized")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")
                self._redis = None
    
    def _load_data(self):
        """Load data from JSON files"""
        try:
            # Load songs
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                    self.youtube_songs = [
                        song for song in self.songs 
                        if song.get("source", "").startswith("youtube_")
                    ]
                logger.info(f"Loaded {len(self.songs)} songs")
            
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
                    for region_code, data in loaded_regions.items():
                        if region_code in self.regions:
                            self.regions[region_code].update(data)
            
            # Rebuild region songs
            self._rebuild_region_songs()
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
    
    def _rebuild_region_songs(self):
        """Rebuild region songs from main songs list"""
        for region_data in self.regions.values():
            region_data["songs"] = []
        
        for song in self.songs:
            region = song.get("region")
            if region in self.regions:
                self.regions[region]["songs"].append(song)
    
    def _save_data(self):
        """Save data to JSON files"""
        try:
            # Save songs
            with open(self.data_dir / "songs.json", 'w') as f:
                json.dump(self.songs, f, indent=2, default=str)
            
            # Save chart history
            with open(self.data_dir / "chart_history.json", 'w') as f:
                json.dump(self.chart_history, f, indent=2, default=str)
            
            # Save regions (without songs to reduce size)
            regions_to_save = {
                region_code: {
                    "name": data["name"],
                    "districts": data["districts"],
                    "musicians": data["musicians"]
                }
                for region_code, data in self.regions.items()
            }
            with open(self.data_dir / "regions.json", 'w') as f:
                json.dump(regions_to_save, f, indent=2, default=str)
            
            logger.debug("Data saved to disk")
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
    
    def _generate_song_id(self, song: SongItem) -> str:
        """Generate deterministic ID for song"""
        content = f"{song.title}_{song.artist}_{song.region}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def add_songs(self, songs: List[SongItem], source: str) -> int:
        """Add songs to database"""
        added_count = 0
        now = datetime.utcnow()
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["id"] = self._generate_song_id(song)
            song_dict["source"] = source
            song_dict["ingested_at"] = now.isoformat()
            
            # Deduplication check
            is_duplicate = any(
                s.get("id") == song_dict["id"] and
                datetime.fromisoformat(s.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
                now - timedelta(hours=48)
                for s in self.songs[-500:]
            )
            
            if not is_duplicate:
                self.songs.append(song_dict)
                
                # Add to region
                if song.region in self.regions:
                    self.regions[song.region]["songs"].append(song_dict)
                
                # Track YouTube songs
                if source.startswith("youtube_"):
                    self.youtube_songs.append(song_dict)
                
                # Invalidate cache
                if self._redis:
                    self._invalidate_cache(song.region)
                
                added_count += 1
        
        # Enforce size limit
        if len(self.songs) > config.MAX_DB_SIZE:
            self.songs = self.songs[-config.MAX_DB_SIZE:]
            self._rebuild_region_songs()
            self.youtube_songs = [s for s in self.songs if s.get("source", "").startswith("youtube_")]
            logger.info(f"Trimmed database to {config.MAX_DB_SIZE} songs")
        
        # Save to disk
        if added_count > 0:
            self._save_data()
        
        return added_count
    
    def _invalidate_cache(self, region: str):
        """Invalidate Redis cache"""
        if self._redis:
            try:
                keys_to_delete = [
                    f"top_songs:{region}",
                    f"top_songs:all",
                    f"trending_songs",
                    f"region_stats"
                ]
                self._redis.delete(*keys_to_delete)
            except Exception:
                pass
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs"""
        cache_key = f"top_songs:{region or 'all'}:{limit}"
        
        # Try cache
        if self._redis:
            try:
                cached = self._redis.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception:
                pass
        
        # Get data
        source_list = self.regions[region]["songs"] if region and region in self.regions else self.songs
        
        sorted_songs = sorted(
            source_list,
            key=lambda x: (x.get("score", 0), x.get("plays", 0)),
            reverse=True
        )[:limit]
        
        # Cache result
        if self._redis and sorted_songs:
            try:
                self._redis.setex(cache_key, config.CACHE_TTL, json.dumps(sorted_songs))
            except Exception:
                pass
        
        return sorted_songs
    
    def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs"""
        cache_key = f"trending_songs:{limit}"
        
        if self._redis:
            try:
                cached = self._redis.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception:
                pass
        
        trending_songs = TrendingService.get_trending_songs(self.youtube_songs, limit)
        
        if self._redis and trending_songs:
            try:
                self._redis.setex(cache_key, 1800, json.dumps(trending_songs))  # 30 minutes
            except Exception:
                pass
        
        return trending_songs
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get region statistics"""
        cache_key = "region_stats"
        
        if self._redis:
            try:
                cached = self._redis.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception:
                pass
        
        stats = {}
        
        for region_code, region_data in self.regions.items():
            region_songs = region_data["songs"]
            
            if region_songs:
                total_plays = sum(s.get("plays", 0) for s in region_songs)
                avg_score = sum(s.get("score", 0) for s in region_songs) / len(region_songs)
                
                top_song = max(region_songs, key=lambda x: x.get("score", 0))
                
                artist_counts = {}
                for song in region_songs:
                    artist = song.get("artist", "Unknown")
                    artist_counts[artist] = artist_counts.get(artist, 0) + 1
                
                top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": len(region_songs),
                    "total_plays": total_plays,
                    "average_score": round(avg_score, 2),
                    "top_song": {
                        "title": top_song.get("title"),
                        "artist": top_song.get("artist"),
                        "score": top_song.get("score", 0),
                        "plays": top_song.get("plays", 0)
                    },
                    "top_artists": [
                        {"artist": artist, "count": count}
                        for artist, count in top_artists
                    ],
                    "districts": region_data["districts"],
                    "notable_musicians": region_data["musicians"],
                    "last_updated": datetime.utcnow().isoformat()
                }
            else:
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": 0,
                    "total_plays": 0,
                    "average_score": 0,
                    "top_song": None,
                    "top_artists": [],
                    "districts": region_data["districts"],
                    "notable_musicians": region_data["musicians"],
                    "last_updated": datetime.utcnow().isoformat()
                }
        
        if self._redis:
            try:
                self._redis.setex(cache_key, config.CACHE_TTL, json.dumps(stats))
            except Exception:
                pass
        
        return stats
    
    def publish_weekly_chart(self, week: str) -> Dict[str, Any]:
        """Publish weekly chart"""
        try:
            snapshot = {
                "week": week,
                "published_at": datetime.utcnow().isoformat(),
                "top100": self.get_top_songs(100),
                "regions": {
                    region_code: self.get_top_songs(5, region_code)
                    for region_code in self.regions.keys()
                },
                "region_stats": self.get_region_stats(),
                "trending": self.get_trending_songs(10),
                "summary": {
                    "total_songs": len(self.songs),
                    "total_plays": sum(s.get("plays", 0) for s in self.songs),
                    "average_score": sum(s.get("score", 0) for s in self.songs) / max(1, len(self.songs))
                }
            }
            
            self.chart_history.append(snapshot)
            
            # Keep last 52 weeks
            if len(self.chart_history) > 52:
                self.chart_history = self.chart_history[-52:]
            
            self._save_data()
            
            # Invalidate all caches
            if self._redis:
                try:
                    self._redis.flushdb()
                except Exception:
                    pass
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Failed to publish weekly chart: {e}")
            raise DatabaseError(f"Failed to publish weekly chart: {str(e)}")

# Initialize database
db = JSONDatabase(config.DATA_DIR, config.REDIS_URL)

# ============================================================================
# AUTHENTICATION
# ============================================================================

security = HTTPBearer(auto_error=False)

class AuthService:
    """Authentication service"""
    
    @staticmethod
    async def verify_admin(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify admin token"""
        if not credentials or credentials.credentials != config.ADMIN_TOKEN:
            raise AuthenticationError("Invalid or missing admin token")
        return True
    
    @staticmethod
    async def verify_ingest(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify ingestion token"""
        if not credentials or credentials.credentials != config.INGEST_TOKEN:
            raise AuthenticationError("Invalid or missing ingestion token")
        return True
    
    @staticmethod
    async def verify_youtube(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify YouTube ingestion token"""
        if not credentials or credentials.credentials != config.YOUTUBE_TOKEN:
            raise AuthenticationError("Invalid or missing YouTube token")
        return True

# ============================================================================
# VALIDATION SERVICE
# ============================================================================

class ValidationService:
    """Ugandan content validation"""
    
    UGANDAN_ARTISTS = {
        "bobi wine", "eddy kenzo", "sheebah", "daddy andre", "gravity",
        "vyroota", "geosteady", "feffe busi", "jose chameleone", "bebe cool",
        "alien skin", "azawi", "vinka", "cindy", "fille", "lyrical", "navio",
        "ray g", "t-paul", "truth 256", "sister charity", "omega 256",
        "rema namakula", "irinah", "mickie wine", "victor ruz", "davido spider",
        "temperature touch", "idi amasaba", "rexy", "kadongo kamu", "bosmic otim",
        "odongo romeo", "eezzy", "jenneth prischa", "laxzy mover", "fik fameica",
        "john blaq", "pia pounds", "catherine kusasira", "martha mukisa",
        "spice diana", "zex bilangilangi"
    }
    
    @classmethod
    def is_ugandan_artist(cls, artist_name: str) -> bool:
        """Check if artist is Ugandan"""
        if not artist_name:
            return False
        
        artist_lower = artist_name.lower()
        
        # Direct match
        if artist_lower in cls.UGANDAN_ARTISTS:
            return True
        
        # Partial match
        for ug_artist in cls.UGANDAN_ARTISTS:
            if ug_artist in artist_lower:
                return True
        
        # Ugandan name patterns
        ugandan_patterns = [
            " omutujju", " omulangira", " kawalya", " kigozi",
            " nakimera", " nakitto", " namale", " nantale",
            " ssali", " ssebagala", " ssemakula", " ssempijja"
        ]
        
        return any(pattern in artist_lower for pattern in ugandan_patterns)
    
    @classmethod
    def validate_song(cls, song: SongItem) -> Tuple[bool, Optional[str]]:
        """Validate song content"""
        if not cls.is_ugandan_artist(song.artist):
            return False, f"Artist '{song.artist}' is not recognized as Ugandan"
        
        if song.region not in config.VALID_REGIONS:
            return False, f"Invalid region: {song.region}"
        
        return True, None

# ============================================================================
# LIFECYCLE MANAGEMENT
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle"""
    # Startup
    logger.info("UG Board Engine starting up")
    logger.info(f"Environment: {config.ENVIRONMENT}")
    logger.info(f"Database: {len(db.songs)} songs loaded")
    logger.info(f"Regions: {', '.join(config.VALID_REGIONS)}")
    
    yield
    
    # Shutdown
    logger.info("UG Board Engine shutting down")
    logger.info(f"Total songs processed: {len(db.songs)}")

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="UG Board Engine",
    version="8.4.0",
    description="Official Ugandan Music Chart System with Regional Support",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Public", "description": "Public endpoints"},
        {"name": "Charts", "description": "Music charts"},
        {"name": "Regions", "description": "Ugandan regional data"},
        {"name": "Trending", "description": "Trending songs"},
        {"name": "Ingestion", "description": "Data ingestion"},
        {"name": "Admin", "description": "Administrative functions"},
    ]
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Global state
app_start_time = datetime.utcnow()
request_count = 0
current_chart_week = datetime.utcnow().strftime("%Y-W%W")

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/", tags=["Public"], summary="Service information")
async def root():
    """Root endpoint"""
    global request_count
    request_count += 1
    
    window_info = TrendingService.get_current_trending_window()
    
    return {
        "service": "UG Board Engine",
        "version": "8.4.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "environment": config.ENVIRONMENT.value,
        "trending_window": window_info,
        "ugandan_regions": list(config.VALID_REGIONS),
        "endpoints": {
            "health": "/health",
            "charts": {
                "top100": "/charts/top100",
                "regions": "/charts/regions",
                "region_detail": "/charts/regions/{region}",
                "trending": "/charts/trending",
                "current_trending": "/charts/trending/now"
            },
            "docs": "/docs"
        }
    }

@app.get("/health", tags=["Public"], summary="Health check")
async def health():
    """Health check"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "database": {
            "songs": len(db.songs),
            "youtube_songs": len(db.youtube_songs),
            "regions": len(db.regions),
            "chart_history": len(db.chart_history)
        },
        "requests_served": request_count,
        "chart_week": current_chart_week,
        "environment": config.ENVIRONMENT.value
    }

@app.get("/charts/top100", tags=["Charts"], summary="Uganda Top 100")
async def get_top100(
    limit: int = Query(100, ge=1, le=200, description="Number of songs")
):
    """Get Uganda Top 100 chart"""
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

@app.get("/charts/regions", tags=["Charts", "Regions"], summary="All regions data")
async def get_all_regions():
    """Get data for all regions"""
    stats = db.get_region_stats()
    
    return {
        "regions": stats,
        "count": len(stats),
        "timestamp": datetime.utcnow().isoformat(),
        "week": current_chart_week
    }

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"], summary="Region detail")
async def get_region_detail(
    region: str = FPath(..., description="Ugandan region"),
    limit: int = Query(10, ge=1, le=50, description="Number of songs")
):
    """Get region details"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=404,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    songs = db.get_top_songs(limit, region)
    region_data = config.UGANDAN_REGIONS[region]
    
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "region": region,
        "region_name": region_data["name"],
        "songs": songs,
        "districts": region_data["districts"],
        "notable_musicians": region_data["musicians"],
        "week": current_chart_week,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending", tags=["Charts", "Trending"], summary="Trending songs")
async def get_trending(
    limit: int = Query(10, ge=1, le=50, description="Number of songs")
):
    """Get trending songs"""
    songs = db.get_trending_songs(limit)
    window_info = TrendingService.get_current_trending_window()
    
    for i, song in enumerate(songs, 1):
        song["trend_rank"] = i
    
    return {
        "chart": f"Trending Now - Uganda",
        "entries": songs,
        "count": len(songs),
        "trending_window": window_info,
        "rotation": f"{config.TRENDING_WINDOW_HOURS}-hour window rotation",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/trending/now", tags=["Charts", "Trending"], summary="Current trending song")
async def get_current_trending():
    """Get current trending song"""
    trending_songs = db.get_trending_songs(1)
    
    if not trending_songs:
        raise HTTPException(status_code=404, detail="No trending songs available")
    
    window_info = TrendingService.get_current_trending_window()
    
    return {
        "trending_song": trending_songs[0],
        "trending_window": window_info,
        "rotation": f"Changes every {config.TRENDING_WINDOW_HOURS} hours",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/youtube", tags=["Ingestion"], summary="Ingest YouTube data")
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube)
):
    """Ingest YouTube data"""
    try:
        valid_songs = []
        validation_errors = []
        
        for song in payload.items:
            is_valid, error = ValidationService.validate_song(song)
            if is_valid:
                valid_songs.append(song)
            else:
                validation_errors.append({
                    "song": f"{song.title} - {song.artist}",
                    "error": error
                })
        
        if not valid_songs:
            raise ValidationError("No valid Ugandan songs found")
        
        added_count = db.add_songs(valid_songs, f"youtube_{payload.source}")
        
        logger.info(f"YouTube ingestion: {added_count} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} Ugandan YouTube songs",
            "statistics": {
                "received": len(payload.items),
                "valid": len(valid_songs),
                "added": added_count,
                "duplicates": len(valid_songs) - added_count,
                "invalid": len(validation_errors)
            },
            "validation_errors": validation_errors[:5],
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}")
        raise HTTPException(status_code=500, detail=f"YouTube ingestion error: {str(e)}")

@app.post("/ingest/radio", tags=["Ingestion"], summary="Ingest radio data")
async def ingest_radio(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest radio data"""
    try:
        valid_songs = []
        for song in payload.items:
            is_valid, error = ValidationService.validate_song(song)
            if is_valid:
                valid_songs.append(song)
        
        if not valid_songs:
            raise ValidationError("No valid Ugandan songs found")
        
        added_count = db.add_songs(valid_songs, f"radio_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} radio songs",
            "added": added_count,
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat()
        }
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Radio ingestion error: {e}")
        raise HTTPException(status_code=500, detail=f"Radio ingestion error: {str(e)}")

@app.post("/ingest/tv", tags=["Ingestion"], summary="Ingest TV data")
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest TV data"""
    try:
        valid_songs = []
        for song in payload.items:
            is_valid, error = ValidationService.validate_song(song)
            if is_valid:
                valid_songs.append(song)
        
        if not valid_songs:
            raise ValidationError("No valid Ugandan songs found")
        
        added_count = db.add_songs(valid_songs, f"tv_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {added_count} TV songs",
            "added": added_count,
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat()
        }
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"TV ingestion error: {e}")
        raise HTTPException(status_code=500, detail=f"TV ingestion error: {str(e)}")

@app.get("/admin/health", tags=["Admin"], summary="Admin health check")
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Admin health check"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "environment": config.ENVIRONMENT.value
        },
        "database": {
            "total_songs": len(db.songs),
            "unique_artists": len(set(s.get("artist", "") for s in db.songs)),
            "regions": list(db.regions.keys()),
            "chart_history": len(db.chart_history)
        },
        "cache": {
            "redis_available": db._redis is not None,
            "cache_ttl": config.CACHE_TTL
        }
    }

@app.post("/admin/publish/weekly", tags=["Admin"], summary="Publish weekly chart")
async def publish_weekly(auth: bool = Depends(AuthService.verify_admin)):
    """Publish weekly chart"""
    try:
        result = db.publish_weekly_chart(current_chart_week)
        
        # Update chart week
        global current_chart_week
        current_chart_week = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-W%W")
        
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
        logger.error(f"Weekly publish error: {e}")
        raise HTTPException(status_code=500, detail=f"Publish error: {str(e)}")

@app.get("/admin/index", tags=["Admin"], summary="Admin publication index")
async def admin_index(auth: bool = Depends(AuthService.verify_admin)):
    """Admin publication index"""
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

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """HTTP exception handler"""
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
    """General exception handler"""
    logger.error(f"Unhandled exception at {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if config.ENVIRONMENT == Environment.DEVELOPMENT else "Contact support",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    print(f"""
UG Board Engine v8.4.0
Environment: {config.ENVIRONMENT.value}
Chart Week: {current_chart_week}
URL: http://localhost:{port}
Docs: http://localhost:{port}/docs
Database: {len(db.songs)} songs loaded
Regions: {', '.join(config.VALID_REGIONS)}
""")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
