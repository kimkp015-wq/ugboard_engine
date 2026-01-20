"""
UG Board Engine - Production Ready Architecture v8.3.1
World-Class Implementation with SOLID Principles and Clean Architecture
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
from typing import Optional, List, Dict, Any, Union, Tuple
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum

import redis
import aiofiles
from fastapi import (
    FastAPI, HTTPException, Header, Depends, BackgroundTasks, 
    Query, Path as FPath, Request, status, APIRouter, Response
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator, ConfigDict, SecretStr
from pydantic_settings import BaseSettings

# ============================================================================
# CONFIGURATION LAYER (12-Factor App Compliant)
# ============================================================================

class Environment(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

class Settings(BaseSettings):
    """Configuration management with validation"""
    
    # Required Environment Variables
    ADMIN_TOKEN: SecretStr = Field(..., env="ADMIN_TOKEN")
    INGEST_TOKEN: SecretStr = Field(..., env="INGEST_TOKEN")
    INTERNAL_TOKEN: SecretStr = Field(..., env="INTERNAL_TOKEN")
    YOUTUBE_TOKEN: SecretStr = Field(..., env="YOUTUBE_TOKEN")
    
    # Optional with defaults
    ENVIRONMENT: Environment = Field(Environment.PRODUCTION, env="ENVIRONMENT")
    DATABASE_URL: Optional[str] = Field(None, env="DATABASE_URL")
    REDIS_URL: Optional[str] = Field(None, env="REDIS_URL")
    CACHE_TTL: int = Field(300, env="CACHE_TTL")  # 5 minutes
    MAX_DB_SIZE: int = Field(10000, env="MAX_DB_SIZE")
    TRENDING_WINDOW_HOURS: int = Field(8, env="TRENDING_WINDOW_HOURS")
    
    # Performance
    MAX_WORKERS: int = Field(4, env="MAX_WORKERS")
    REQUEST_TIMEOUT: int = Field(30, env="REQUEST_TIMEOUT")
    
    # Security
    CORS_ORIGINS: List[str] = Field(
        default=["*"],  # Changed to wildcard for Render deployment
        env="CORS_ORIGINS"
    )
    RATE_LIMIT_REQUESTS: int = Field(100, env="RATE_LIMIT_REQUESTS")
    RATE_LIMIT_WINDOW: int = Field(60, env="RATE_LIMIT_WINDOW")  # seconds
    
    # Paths
    DATA_DIR: Path = Field(Path("data"), env="DATA_DIR")
    LOGS_DIR: Path = Field(Path("logs"), env="LOGS_DIR")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore"
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.validate_configuration()
    
    def validate_configuration(self):
        """Validate configuration at runtime"""
        # Create directories
        self.DATA_DIR.mkdir(exist_ok=True, parents=True)
        self.LOGS_DIR.mkdir(exist_ok=True, parents=True)

settings = Settings()

# ============================================================================
# LOGGING & MONITORING
# ============================================================================

def setup_structured_logging():
    """Configure structured logging for production"""
    logs_dir = settings.LOGS_DIR
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # File handler
    file_handler = logging.FileHandler(logs_dir / "ugboard_engine.log")
    file_handler.setFormatter(formatter)
    
    # Setup root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler]
    )
    
    return logging.getLogger(__name__)

logger = setup_structured_logging()

# ============================================================================
# EXCEPTIONS (Clean Error Hierarchy)
# ============================================================================

class UGBaseException(Exception):
    """Base exception for UG Board Engine"""
    def __init__(self, message: str, code: str = "INTERNAL_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)

class ValidationError(UGBaseException):
    """Validation-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")

class AuthenticationError(UGBaseException):
    """Authentication-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "AUTHENTICATION_ERROR")

class DatabaseError(UGBaseException):
    """Database-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "DATABASE_ERROR")

class RateLimitError(UGBaseException):
    """Rate limiting errors"""
    def __init__(self, message: str):
        super().__init__(message, "RATE_LIMIT_ERROR")

# ============================================================================
# MODELS (Domain Entities)
# ============================================================================

class Region(str, Enum):
    CENTRAL = "central"
    WESTERN = "western"
    EASTERN = "eastern"
    NORTHERN = "northern"

class District(str, Enum):
    # Central
    KAMPALA = "Kampala"
    WAKISO = "Wakiso"
    MUKONO = "Mukono"
    LUWERO = "Luwero"
    MASAKA = "Masaka"
    
    # Western
    MBARARA = "Mbarara"
    KASESE = "Kasese"
    NTUNGAMO = "Ntungamo"
    KABALE = "Kabale"
    FORT_PORTAL = "Fort Portal"
    HOIMA = "Hoima"
    
    # Eastern
    JINJA = "Jinja"
    MBALE = "Mbale"
    TORORO = "Tororo"
    MAYUGE = "Mayuge"
    SOROTI = "Soroti"
    IGANGA = "Iganga"
    
    # Northern
    GULU = "Gulu"
    LIRA = "Lira"
    ARUA = "Arua"
    KITGUM = "Kitgum"

class SongItem(BaseModel):
    """Domain model for songs with comprehensive validation"""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        use_enum_values=True,
        extra="forbid"
    )
    
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=100)
    plays: int = Field(default=0, ge=0)
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    station: Optional[str] = Field(None, max_length=50)
    region: Region = Field(default=Region.CENTRAL)
    district: Optional[District] = None
    timestamp: Optional[str] = Field(None)
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISO 8601 timestamp"""
        if v:
            try:
                # Normalize timestamp
                if v.endswith('Z'):
                    v = v[:-1] + '+00:00'
                datetime.fromisoformat(v)
            except (ValueError, AttributeError):
                raise ValidationError(f"Invalid ISO 8601 timestamp: {v}")
        return v

class IngestPayload(BaseModel):
    """Base ingestion payload with rate limiting support"""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000)
    source: str = Field(..., min_length=1, max_length=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    request_id: Optional[str] = Field(None)  # For request tracing

class YouTubeIngestPayload(IngestPayload):
    """YouTube-specific ingestion"""
    channel_id: Optional[str] = Field(None, max_length=50)
    video_id: Optional[str] = Field(None, max_length=20)
    category: str = Field(default="music", max_length=50)

# ============================================================================
# DATABASE ABSTRACTION LAYER
# ============================================================================

class BaseDatabase:
    """Abstract base class for database implementations"""
    
    async def add_songs(self, songs: List[SongItem], source: str) -> int:
        """Add songs to database"""
        raise NotImplementedError
    
    async def get_top_songs(self, limit: int = 100, region: Optional[Region] = None) -> List[Dict[str, Any]]:
        """Get top songs"""
        raise NotImplementedError
    
    async def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs"""
        raise NotImplementedError
    
    async def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get region statistics"""
        raise NotImplementedError
    
    async def publish_weekly_chart(self, week: str) -> Dict[str, Any]:
        """Publish weekly chart"""
        raise NotImplementedError

class HybridDatabase(BaseDatabase):
    """
    Hybrid database with Redis caching and async file persistence
    Implements Repository pattern
    """
    
    def __init__(self, data_dir: Path, redis_url: Optional[str] = None):
        self.data_dir = data_dir
        self._redis = None
        self._redis_url = redis_url
        
        # Initialize data structures
        self._songs: List[Dict[str, Any]] = []
        self._chart_history: List[Dict[str, Any]] = []
        self._regions: Dict[str, Dict[str, Any]] = self._initialize_regions()
        
        # Initialize connections
        asyncio.create_task(self._initialize())
    
    def _initialize_regions(self) -> Dict[str, Dict[str, Any]]:
        """Initialize Ugandan region structure"""
        return {
            Region.CENTRAL: {
                "name": "Central Region",
                "districts": [District.KAMPALA, District.WAKISO, District.MUKONO, 
                            District.LUWERO, District.MASAKA],
                "musicians": ["Alien Skin", "Azawi", "Sheebah Karungi", "Vinka", 
                            "Eddy Kenzo", "Bobi Wine", "Daddy Andre"],
                "songs": []
            },
            Region.WESTERN: {
                "name": "Western Region",
                "districts": [District.MBARARA, District.KASESE, District.NTUNGAMO, 
                            District.KABALE, District.FORT_PORTAL, District.HOIMA],
                "musicians": ["Ray G", "T-Paul", "Truth 256", "Sister Charity", 
                            "Omega 256", "Rema Namakula"],
                "songs": []
            },
            Region.EASTERN: {
                "name": "Eastern Region",
                "districts": [District.JINJA, District.MBALE, District.TORORO, 
                            District.MAYUGE, District.SOROTI, District.IGANGA],
                "musicians": ["Victor Ruz", "Davido Spider", "Temperature Touch", 
                            "Idi Amasaba", "Rexy", "Geosteady"],
                "songs": []
            },
            Region.NORTHERN: {
                "name": "Northern Region",
                "districts": [District.GULU, District.LIRA, District.ARUA, District.KITGUM],
                "musicians": ["Bosmic Otim", "Odongo Romeo", "Eezzy", 
                            "Jenneth Prischa", "Laxzy Mover", "Fik Fameica"],
                "songs": []
            }
        }
    
    async def _initialize(self):
        """Initialize database connections"""
        # Initialize Redis if URL provided
        if self._redis_url:
            try:
                self._redis = redis.from_url(
                    self._redis_url,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    decode_responses=True
                )
                self._redis.ping()
                logger.info("Redis cache initialized")
            except redis.ConnectionError as e:
                logger.warning(f"Redis connection failed: {e}. Continuing without cache.")
                self._redis = None
        
        # Load data from files
        await self._load_data()
    
    async def _load_data(self):
        """Load data asynchronously"""
        try:
            await asyncio.gather(
                self._load_file("songs.json", self._songs),
                self._load_file("chart_history.json", self._chart_history),
                self._load_regions()
            )
            logger.info(f"Loaded {len(self._songs)} songs")
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise DatabaseError(f"Failed to load database: {str(e)}")
    
    async def _load_file(self, filename: str, target_list: List):
        """Load JSON file asynchronously"""
        file_path = self.data_dir / filename
        if await self._file_exists(file_path):
            async with aiofiles.open(file_path, 'r') as f:
                content = await f.read()
                if content:
                    target_list.extend(json.loads(content))
    
    async def _file_exists(self, path: Path) -> bool:
        """Check if file exists asynchronously"""
        try:
            return await asyncio.to_thread(path.exists)
        except Exception:
            return False
    
    async def _load_regions(self):
        """Load regions data"""
        regions_file = self.data_dir / "regions.json"
        if await self._file_exists(regions_file):
            async with aiofiles.open(regions_file, 'r') as f:
                loaded_regions = json.loads(await f.read())
                for region_code, data in loaded_regions.items():
                    if region_code in self._regions:
                        self._regions[region_code].update(data)
    
    async def _save_data(self):
        """Save data asynchronously with atomic writes"""
        try:
            # Use temporary files for atomic writes
            tasks = [
                self._save_file_atomic("songs.json", self._songs),
                self._save_file_atomic("chart_history.json", self._chart_history),
                self._save_regions_atomic()
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise DatabaseError(f"Failed to save database: {str(e)}")
    
    async def _save_file_atomic(self, filename: str, data: List):
        """Save file atomically to prevent corruption"""
        temp_file = self.data_dir / f"{filename}.tmp"
        final_file = self.data_dir / filename
        
        # Write to temp file
        async with aiofiles.open(temp_file, 'w') as f:
            await f.write(json.dumps(data, indent=2, default=str))
        
        # Atomic rename (works on POSIX systems)
        await asyncio.to_thread(temp_file.replace, final_file)
    
    async def _save_regions_atomic(self):
        """Save regions data without songs to reduce file size"""
        regions_to_save = {
            region_code: {
                "name": data["name"],
                "districts": data["districts"],
                "musicians": data["musicians"]
            }
            for region_code, data in self._regions.items()
        }
        
        await self._save_file_atomic("regions.json", regions_to_save)
    
    def _generate_song_id(self, song: SongItem) -> str:
        """Generate deterministic ID for song"""
        content = f"{song.title}_{song.artist}_{song.region}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    async def add_songs(self, songs: List[SongItem], source: str) -> int:
        """Add songs with deduplication"""
        added_count = 0
        now = datetime.utcnow()
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["id"] = self._generate_song_id(song)
            song_dict["source"] = source
            song_dict["ingested_at"] = now.isoformat()
            
            # Check for duplicates (last 48 hours)
            is_duplicate = any(
                s.get("id") == song_dict["id"] and
                datetime.fromisoformat(s.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
                now - timedelta(hours=48)
                for s in self._songs[-500:]
            )
            
            if not is_duplicate:
                self._songs.append(song_dict)
                
                # Add to region
                region_songs = self._regions[song.region]["songs"]
                region_songs.append(song_dict)
                
                # Invalidate cache for this region
                if self._redis:
                    await asyncio.to_thread(
                        self._redis.delete,
                        f"top_songs:{song.region}",
                        f"trending_songs",
                        f"region_stats"
                    )
                
                added_count += 1
        
        # Enforce size limit
        if len(self._songs) > settings.MAX_DB_SIZE:
            self._songs = self._songs[-settings.MAX_DB_SIZE:]
            # Rebuild region songs from remaining songs
            for region_data in self._regions.values():
                region_data["songs"] = [
                    s for s in self._songs 
                    if s.get("region") == region_data["name"].replace(" Region", "").lower()
                ]
        
        # Save data asynchronously
        if added_count > 0:
            asyncio.create_task(self._save_data())
        
        return added_count
    
    async def get_top_songs(self, limit: int = 100, region: Optional[Region] = None) -> List[Dict[str, Any]]:
        """Get top songs with caching"""
        cache_key = f"top_songs:{region or 'all'}:{limit}"
        
        # Try cache first
        if self._redis:
            try:
                cached = await asyncio.to_thread(self._redis.get, cache_key)
                if cached:
                    return json.loads(cached)
            except redis.RedisError:
                pass
        
        # Get data
        source_list = (
            self._regions[region]["songs"] 
            if region and region in self._regions 
            else self._songs
        )
        
        sorted_songs = sorted(
            source_list,
            key=lambda x: (x.get("score", 0), x.get("plays", 0)),
            reverse=True
        )[:limit]
        
        # Cache result
        if self._redis and sorted_songs:
            try:
                await asyncio.to_thread(
                    self._redis.setex,
                    cache_key,
                    settings.CACHE_TTL,
                    json.dumps(sorted_songs)
                )
            except redis.RedisError:
                pass
        
        return sorted_songs
    
    async def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs with 8-hour window algorithm"""
        cache_key = f"trending_songs:{limit}"
        
        # Try cache first
        if self._redis:
            try:
                cached = await asyncio.to_thread(self._redis.get, cache_key)
                if cached:
                    return json.loads(cached)
            except redis.RedisError:
                pass
        
        if not self._songs:
            return []
        
        # 8-hour window algorithm
        hours_since_epoch = int(time.time() // 3600)
        window_number = hours_since_epoch // settings.TRENDING_WINDOW_HOURS
        
        # Get recent songs (last 7 days)
        recent_cutoff = datetime.utcnow() - timedelta(days=7)
        recent_songs = [
            song for song in self._songs
            if datetime.fromisoformat(song.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) > recent_cutoff
        ]
        
        if not recent_songs:
            return []
        
        # Deterministic shuffle based on window number
        def song_score(song: Dict[str, Any]) -> float:
            base_score = song.get("score", 0) * 0.6 + song.get("plays", 0) * 0.4
            # Add window-specific component for deterministic ordering
            window_hash = hash(f"{window_number}_{song.get('id', '')}") % 1000
            return base_score + (window_hash / 1000.0)
        
        sorted_songs = sorted(recent_songs, key=song_score, reverse=True)[:limit]
        
        # Cache result
        if self._redis and sorted_songs:
            try:
                await asyncio.to_thread(
                    self._redis.setex,
                    cache_key,
                    1800,  # 30 minutes cache for trending
                    json.dumps(sorted_songs)
                )
            except redis.RedisError:
                pass
        
        return sorted_songs
    
    async def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive region statistics"""
        cache_key = "region_stats"
        
        if self._redis:
            try:
                cached = await asyncio.to_thread(self._redis.get, cache_key)
                if cached:
                    return json.loads(cached)
            except redis.RedisError:
                pass
        
        stats = {}
        
        for region_code, region_data in self._regions.items():
            region_songs = region_data["songs"]
            
            if region_songs:
                # Calculate statistics
                total_plays = sum(s.get("plays", 0) for s in region_songs)
                avg_score = sum(s.get("score", 0) for s in region_songs) / len(region_songs)
                
                # Top song
                top_song = max(region_songs, key=lambda x: x.get("score", 0))
                
                # Artist distribution
                artist_counts = {}
                for song in region_songs:
                    artist = song.get("artist", "Unknown")
                    artist_counts[artist] = artist_counts.get(artist, 0) + 1
                
                top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                
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
        
        # Cache result
        if self._redis:
            try:
                await asyncio.to_thread(
                    self._redis.setex,
                    cache_key,
                    settings.CACHE_TTL,
                    json.dumps(stats)
                )
            except redis.RedisError:
                pass
        
        return stats
    
    async def publish_weekly_chart(self, week: str) -> Dict[str, Any]:
        """Publish weekly chart with comprehensive data"""
        try:
            snapshot = {
                "week": week,
                "published_at": datetime.utcnow().isoformat(),
                "top100": await self.get_top_songs(100),
                "regions": {
                    region_code: await self.get_top_songs(10, region_code)
                    for region_code in self._regions.keys()
                },
                "region_stats": await self.get_region_stats(),
                "trending": await self.get_trending_songs(10),
                "summary": {
                    "total_songs": len(self._songs),
                    "total_plays": sum(s.get("plays", 0) for s in self._songs),
                    "average_score": sum(s.get("score", 0) for s in self._songs) / max(1, len(self._songs))
                }
            }
            
            self._chart_history.append(snapshot)
            
            # Keep only last 52 weeks
            if len(self._chart_history) > 52:
                self._chart_history = self._chart_history[-52:]
            
            # Save asynchronously
            asyncio.create_task(self._save_data())
            
            # Invalidate all caches
            if self._redis:
                await asyncio.to_thread(self._redis.flushdb)
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Failed to publish weekly chart: {e}")
            raise DatabaseError(f"Failed to publish weekly chart: {str(e)}")

# ============================================================================
# SERVICES LAYER (Business Logic)
# ============================================================================

class AuthenticationService:
    """Authentication service with token validation"""
    
    @staticmethod
    async def verify_admin(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
    ) -> bool:
        """Verify admin token"""
        if not credentials:
            raise AuthenticationError("Missing authentication token")
        
        try:
            expected_token = settings.ADMIN_TOKEN.get_secret_value()
            if credentials.credentials != expected_token:
                raise AuthenticationError("Invalid admin token")
            
            logger.debug("Admin authentication successful")
            return True
            
        except Exception as e:
            logger.warning(f"Admin authentication failed: {e}")
            raise AuthenticationError("Authentication failed")
    
    @staticmethod
    async def verify_ingest(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
    ) -> bool:
        """Verify ingestion token"""
        if not credentials:
            raise AuthenticationError("Missing ingestion token")
        
        try:
            expected_token = settings.INGEST_TOKEN.get_secret_value()
            if credentials.credentials != expected_token:
                raise AuthenticationError("Invalid ingestion token")
            
            logger.debug("Ingest authentication successful")
            return True
            
        except Exception as e:
            logger.warning(f"Ingest authentication failed: {e}")
            raise AuthenticationError("Ingestion authentication failed")
    
    @staticmethod
    async def verify_youtube(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
    ) -> bool:
        """Verify YouTube ingestion token"""
        if not credentials:
            raise AuthenticationError("Missing YouTube token")
        
        try:
            expected_token = settings.YOUTUBE_TOKEN.get_secret_value()
            if credentials.credentials != expected_token:
                raise AuthenticationError("Invalid YouTube token")
            
            logger.debug("YouTube authentication successful")
            return True
            
        except Exception as e:
            logger.warning(f"YouTube authentication failed: {e}")
            raise AuthenticationError("YouTube authentication failed")

class ValidationService:
    """Business logic validation for Ugandan content"""
    
    # Comprehensive Ugandan artist database
    UGANDAN_ARTISTS = {
        # Central Region
        "bobi wine", "eddy kenzo", "sheebah", "daddy andre", "gravity",
        "vyroota", "geosteady", "feffe busi", "jose chameleone", "bebe cool",
        "alien skin", "azawi", "vinka", "cindy", "fille", "lyrical", "navio",
        
        # Western Region
        "ray g", "t-paul", "truth 256", "sister charity", "omega 256",
        "rema namakula", "irinah", "mickie wine",
        
        # Eastern Region
        "victor ruz", "davido spider", "temperature touch", "idi amasaba",
        "rexy", "kadongo kamu",
        
        # Northern Region
        "bosmic otim", "odongo romeo", "eezzy", "jenneth prischa",
        "laxzy mover", "fik fameica",
        
        # Cross-region
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
        
        # Check for Ugandan name patterns
        ugandan_patterns = [
            " omutujju", " omulangira", " kawalya", " kigozi",
            " nakimera", " nakitto", " namale", " nantale",
            " ssali", " ssebagala", " ssemakula", " ssempijja"
        ]
        
        return any(pattern in artist_lower for pattern in ugandan_patterns)
    
    @classmethod
    def validate_song(cls, song: SongItem) -> Tuple[bool, Optional[str]]:
        """Validate song content, return (is_valid, error_message)"""
        # Check artist
        if not cls.is_ugandan_artist(song.artist):
            return False, f"Artist '{song.artist}' is not recognized as Ugandan"
        
        return True, None

class RateLimitService:
    """Rate limiting service using Redis"""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self._redis = redis_client
    
    async def check_rate_limit(self, key: str, max_requests: int, window: int) -> bool:
        """Check if request is within rate limit"""
        if not self._redis:
            return True  # No rate limiting without Redis
        
        try:
            current = await asyncio.to_thread(self._redis.get, key)
            if current and int(current) >= max_requests:
                return False
            
            # Increment counter
            pipe = self._redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, window)
            await asyncio.to_thread(pipe.execute)
            
            return True
            
        except redis.RedisError:
            logger.warning("Rate limit check failed, allowing request")
            return True

# ============================================================================
# APPLICATION FACTORY
# ============================================================================

def create_application() -> FastAPI:
    """Application factory following Clean Architecture"""
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application lifecycle management"""
        # Startup
        logger.info("UG Board Engine starting up")
        logger.info(f"Environment: {settings.ENVIRONMENT}")
        
        # Initialize database
        global db, rate_limiter
        db = HybridDatabase(settings.DATA_DIR, settings.REDIS_URL)
        rate_limiter = RateLimitService(db._redis)
        
        # Wait for initialization
        await asyncio.sleep(0.1)  # Small delay for async init
        
        # Initial stats
        songs_count = len(db._songs)
        logger.info(f"Database initialized: {songs_count} songs loaded")
        
        if songs_count == 0:
            logger.info("Database is empty - ready for ingestion")
        
        yield
        
        # Shutdown
        logger.info("UG Board Engine shutting down")
        logger.info(f"Final stats: {len(db._songs)} songs processed")
        if db._redis:
            try:
                await asyncio.to_thread(db._redis.close)
            except:
                pass
    
    # Create FastAPI application
    app = FastAPI(
        title="UG Board Engine",
        version="8.3.1",
        description="Production-ready Ugandan Music Chart System with Regional Support",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
        openapi_tags=[
            {"name": "Public", "description": "Public endpoints"},
            {"name": "Charts", "description": "Music charts and trends"},
            {"name": "Regions", "description": "Ugandan regional data"},
            {"name": "Ingestion", "description": "Data ingestion endpoints"},
            {"name": "Admin", "description": "Administrative functions"},
        ]
    )
    
    # ========================================================================
    # MIDDLEWARE
    # ========================================================================
    
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )
    
    # Compression
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    
    # Request logging middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", "N/A")
        start_time = time.time()
        
        logger.info(f"Request {request_id}: {request.method} {request.url.path}")
        
        try:
            response = await call_next(request)
            process_time = (time.time() - start_time) * 1000
            
            logger.info(
                f"Response {request_id}: {response.status_code} "
                f"({process_time:.2f}ms)"
            )
            
            response.headers["X-Process-Time"] = str(process_time)
            response.headers["X-Request-ID"] = request_id
            
            return response
            
        except Exception as e:
            process_time = (time.time() - start_time) * 1000
            logger.error(
                f"Error {request_id}: {type(e).__name__} in {process_time:.2f}ms - {str(e)}"
            )
            raise
    
    # ========================================================================
    # GLOBAL STATE
    # ========================================================================
    
    app.state.start_time = datetime.utcnow()
    app.state.request_count = 0
    app.state.current_chart_week = datetime.utcnow().strftime("%Y-W%W")
    
    # ========================================================================
    # EXCEPTION HANDLERS
    # ========================================================================
    
    @app.exception_handler(UGBaseException)
    async def ug_exception_handler(request: Request, exc: UGBaseException):
        """Handle UG Board exceptions"""
        logger.warning(f"UG Exception at {request.url.path}: {exc.code} - {exc.message}")
        
        status_code = {
            "VALIDATION_ERROR": status.HTTP_422_UNPROCESSABLE_ENTITY,
            "AUTHENTICATION_ERROR": status.HTTP_401_UNAUTHORIZED,
            "DATABASE_ERROR": status.HTTP_503_SERVICE_UNAVAILABLE,
            "RATE_LIMIT_ERROR": status.HTTP_429_TOO_MANY_REQUESTS,
        }.get(exc.code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        return JSONResponse(
            status_code=status_code,
            content={
                "error": exc.code,
                "message": exc.message,
                "timestamp": datetime.utcnow().isoformat(),
                "path": request.url.path,
                "request_id": request.headers.get("X-Request-ID", "N/A")
            }
        )
    
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
                "path": request.url.path,
                "request_id": request.headers.get("X-Request-ID", "N/A")
            }
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle unexpected exceptions"""
        error_id = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        logger.error(
            f"Unhandled Exception {error_id} at {request.url.path}: "
            f"{type(exc).__name__} - {str(exc)}",
            exc_info=True
        )
        
        detail = "Internal server error" if settings.ENVIRONMENT == Environment.PRODUCTION else str(exc)
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "INTERNAL_SERVER_ERROR",
                "message": detail,
                "error_id": error_id,
                "timestamp": datetime.utcnow().isoformat(),
                "path": request.url.path,
                "request_id": request.headers.get("X-Request-ID", "N/A")
            }
        )
    
    # ========================================================================
    # PUBLIC ENDPOINTS
    # ========================================================================
    
    @app.get("/", tags=["Public"], summary="Service information")
    async def root(request: Request):
        """Root endpoint with comprehensive service information"""
        app.state.request_count += 1
        
        trending_window = time.time() // (settings.TRENDING_WINDOW_HOURS * 3600)
        
        return {
            "service": "UG Board Engine",
            "version": "8.3.1",
            "status": "online",
            "environment": settings.ENVIRONMENT,
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": int((datetime.utcnow() - app.state.start_time).total_seconds()),
            "request_count": app.state.request_count,
            "chart_week": app.state.current_chart_week,
            "features": {
                "trending_window_hours": settings.TRENDING_WINDOW_HOURS,
                "cache_enabled": db._redis is not None,
                "max_db_size": settings.MAX_DB_SIZE
            },
            "endpoints": {
                "health": "/health",
                "charts": {
                    "top100": "/charts/top100",
                    "regions": "/charts/regions",
                    "region_detail": "/charts/regions/{region}",
                    "trending": "/charts/trending",
                    "current_trending": "/charts/trending/now"
                },
                "docs": "/docs",
                "openapi": "/openapi.json"
            },
            "uganda": {
                "regions": list(Region.__members__.keys()),
                "trending_window": f"Every {settings.TRENDING_WINDOW_HOURS} hours",
                "current_window": int(trending_window)
            }
        }
    
    @app.get("/health", tags=["Public"], summary="Health check")
    async def health():
        """Comprehensive health check with dependency verification"""
        health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "8.3.1",
            "environment": settings.ENVIRONMENT,
            "checks": {}
        }
        
        # Database check
        try:
            songs_count = len(db._songs)
            health_status["checks"]["database"] = {
                "status": "healthy",
                "songs": songs_count,
                "regions": len(db._regions),
                "chart_history": len(db._chart_history)
            }
        except Exception as e:
            health_status["checks"]["database"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            health_status["status"] = "degraded"
        
        # Redis check
        if db._redis:
            try:
                await asyncio.to_thread(db._redis.ping)
                info = await asyncio.to_thread(db._redis.info)
                health_status["checks"]["redis"] = {
                    "status": "healthy",
                    "used_memory": info.get("used_memory_human", "N/A"),
                    "connected_clients": info.get("connected_clients", 0)
                }
            except Exception as e:
                health_status["checks"]["redis"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["status"] = "degraded"
        
        # System metrics
        health_status["system"] = {
            "uptime_seconds": int((datetime.utcnow() - app.state.start_time).total_seconds()),
            "requests_served": app.state.request_count,
            "current_chart_week": app.state.current_chart_week,
            "python_version": sys.version.split()[0]
        }
        
        return health_status
    
    # ========================================================================
    # CHARTS ENDPOINTS
    # ========================================================================
    
    @app.get("/charts/top100", tags=["Charts"], summary="Uganda Top 100")
    async def get_top100(
        limit: int = Query(100, ge=1, le=200, description="Number of songs"),
        request: Request = None
    ):
        """Get Uganda Top 100 chart"""
        try:
            songs = await db.get_top_songs(limit)
            
            for i, song in enumerate(songs, 1):
                song["rank"] = i
                song["rank_change"] = "new"  # Placeholder for rank change logic
            
            return {
                "chart": "Uganda Top 100",
                "week": app.state.current_chart_week,
                "entries": songs,
                "count": len(songs),
                "timestamp": datetime.utcnow().isoformat(),
                "cache_info": "Redis cached" if db._redis else "No cache"
            }
        except Exception as e:
            logger.error(f"Failed to get top100: {e}")
            raise DatabaseError("Failed to retrieve top 100 chart")
    
    @app.get("/charts/regions", tags=["Charts", "Regions"], summary="All regions data")
    async def get_all_regions():
        """Get comprehensive data for all Ugandan regions"""
        try:
            stats = await db.get_region_stats()
            
            return {
                "regions": stats,
                "count": len(stats),
                "timestamp": datetime.utcnow().isoformat(),
                "week": app.state.current_chart_week
            }
        except Exception as e:
            logger.error(f"Failed to get region stats: {e}")
            raise DatabaseError("Failed to retrieve region statistics")
    
    @app.get("/charts/regions/{region}", tags=["Charts", "Regions"], summary="Region detail")
    async def get_region_detail(
        region: Region = FPath(..., description="Ugandan region"),
        limit: int = Query(10, ge=1, le=50, description="Number of songs")
    ):
        """Get detailed information for a specific region"""
        try:
            # Get top songs for region
            songs = await db.get_top_songs(limit, region)
            
            for i, song in enumerate(songs, 1):
                song["rank"] = i
            
            # Get region stats
            stats = await db.get_region_stats()
            region_stats = stats.get(region.value, {})
            
            return {
                "region": region.value,
                "region_name": region_stats.get("name", region.value.capitalize()),
                "songs": songs,
                "stats": region_stats,
                "count": len(songs),
                "timestamp": datetime.utcnow().isoformat(),
                "week": app.state.current_chart_week
            }
        except Exception as e:
            logger.error(f"Failed to get region detail for {region}: {e}")
            raise DatabaseError(f"Failed to retrieve region data for {region}")
    
    @app.get("/charts/trending", tags=["Charts"], summary="Trending songs")
    async def get_trending(
        limit: int = Query(10, ge=1, le=50, description="Number of songs")
    ):
        """Get trending songs with 8-hour window rotation"""
        try:
            songs = await db.get_trending_songs(limit)
            
            for i, song in enumerate(songs, 1):
                song["trend_rank"] = i
            
            current_window = int(time.time() // (settings.TRENDING_WINDOW_HOURS * 3600))
            
            return {
                "chart": f"Trending Now - Uganda (Every {settings.TRENDING_WINDOW_HOURS}h)",
                "entries": songs,
                "count": len(songs),
                "window": {
                    "current": current_window,
                    "hours": settings.TRENDING_WINDOW_HOURS,
                    "next_change_seconds": (current_window + 1) * settings.TRENDING_WINDOW_HOURS * 3600 - time.time()
                },
                "timestamp": datetime.utcnow().isoformat(),
                "algorithm": "8-hour deterministic rotation"
            }
        except Exception as e:
            logger.error(f"Failed to get trending songs: {e}")
            raise DatabaseError("Failed to retrieve trending songs")
    
    @app.get("/charts/trending/now", tags=["Charts"], summary="Current trending song")
    async def get_current_trending():
        """Get the current trending song for the window"""
        try:
            trending_songs = await db.get_trending_songs(1)
            
            if not trending_songs:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No trending songs available"
                )
            
            current_window = int(time.time() // (settings.TRENDING_WINDOW_HOURS * 3600))
            
            return {
                "trending_song": trending_songs[0],
                "window": {
                    "current": current_window,
                    "hours": settings.TRENDING_WINDOW_HOURS,
                    "description": f"Changes every {settings.TRENDING_WINDOW_HOURS} hours"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get current trending: {e}")
            raise DatabaseError("Failed to retrieve current trending song")
    
    # ========================================================================
    # INGESTION ENDPOINTS
    # ========================================================================
    
    @app.post("/ingest/youtube", tags=["Ingestion"], summary="Ingest YouTube data")
    async def ingest_youtube(
        payload: YouTubeIngestPayload,
        auth: bool = Depends(AuthenticationService.verify_youtube)
    ):
        """Ingest YouTube data with Ugandan validation"""
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
                raise ValidationError(
                    f"No valid Ugandan songs found. "
                    f"Errors: {[e['error'] for e in validation_errors[:3]]}"
                )
            
            # Add to database
            added_count = await db.add_songs(valid_songs, f"youtube_{payload.source}")
            
            # Log ingestion
            logger.info(
                f"YouTube ingestion from {payload.source}: "
                f"{added_count} songs added, "
                f"{len(validation_errors)} failed validation"
            )
            
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
                "validation_errors": validation_errors[:10],  # Limit errors in response
                "source": payload.source,
                "category": payload.category,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"YouTube ingestion failed: {e}", exc_info=True)
            raise DatabaseError(f"YouTube ingestion failed: {str(e)}")
    
    @app.post("/ingest/radio", tags=["Ingestion"], summary="Ingest radio data")
    async def ingest_radio(
        payload: IngestPayload,
        auth: bool = Depends(AuthenticationService.verify_ingest)
    ):
        """Ingest radio data"""
        try:
            # Basic validation
            valid_songs = []
            for song in payload.items:
                is_valid, error = ValidationService.validate_song(song)
                if is_valid:
                    valid_songs.append(song)
            
            if not valid_songs:
                raise ValidationError("No valid Ugandan songs found")
            
            added_count = await db.add_songs(valid_songs, f"radio_{payload.source}")
            
            logger.info(f"Radio ingestion from {payload.source}: {added_count} songs added")
            
            return {
                "status": "success",
                "message": f"Ingested {added_count} radio songs",
                "added": added_count,
                "source": payload.source,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"Radio ingestion failed: {e}")
            raise DatabaseError(f"Radio ingestion failed: {str(e)}")
    
    @app.post("/ingest/tv", tags=["Ingestion"], summary="Ingest TV data")
    async def ingest_tv(
        payload: IngestPayload,
        auth: bool = Depends(AuthenticationService.verify_ingest)
    ):
        """Ingest TV data"""
        try:
            # Basic validation
            valid_songs = []
            for song in payload.items:
                is_valid, error = ValidationService.validate_song(song)
                if is_valid:
                    valid_songs.append(song)
            
            if not valid_songs:
                raise ValidationError("No valid Ugandan songs found")
            
            added_count = await db.add_songs(valid_songs, f"tv_{payload.source}")
            
            logger.info(f"TV ingestion from {payload.source}: {added_count} songs added")
            
            return {
                "status": "success",
                "message": f"Ingested {added_count} TV songs",
                "added": added_count,
                "source": payload.source,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"TV ingestion failed: {e}")
            raise DatabaseError(f"TV ingestion failed: {str(e)}")
    
    # ========================================================================
    # ADMIN ENDPOINTS
    # ========================================================================
    
    @app.get("/admin/health", tags=["Admin"], summary="Admin health check")
    async def admin_health(auth: bool = Depends(AuthenticationService.verify_admin)):
        """Admin-only detailed health check"""
        health_data = await health()  # Reuse public health endpoint
        
        # Add admin-specific data
        health_data["admin"] = {
            "authenticated": True,
            "environment": settings.ENVIRONMENT,
            "config": {
                "max_db_size": settings.MAX_DB_SIZE,
                "cache_ttl": settings.CACHE_TTL,
                "trending_window": settings.TRENDING_WINDOW_HOURS
            },
            "security": {
                "admin_token_set": bool(settings.ADMIN_TOKEN.get_secret_value()),
                "ingest_token_set": bool(settings.INGEST_TOKEN.get_secret_value()),
                "youtube_token_set": bool(settings.YOUTUBE_TOKEN.get_secret_value())
            }
        }
        
        return health_data
    
    @app.post("/admin/publish/weekly", tags=["Admin"], summary="Publish weekly chart")
    async def publish_weekly(auth: bool = Depends(AuthenticationService.verify_admin)):
        """Publish weekly chart for all regions"""
        try:
            snapshot = await db.publish_weekly_chart(app.state.current_chart_week)
            
            # Update chart week for next publication
            app.state.current_chart_week = (
                datetime.utcnow() + timedelta(days=7)
            ).strftime("%Y-W%W")
            
            logger.info(f"Weekly chart published for week {snapshot['week']}")
            
            return {
                "status": "success",
                "message": "Weekly chart published successfully",
                "week": snapshot["week"],
                "published_at": snapshot["published_at"],
                "summary": {
                    "top100_count": len(snapshot["top100"]),
                    "regions_published": len(snapshot["regions"]),
                    "total_songs": len(db._songs),
                    "total_plays": sum(s.get("plays", 0) for s in db._songs)
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Weekly publish failed: {e}", exc_info=True)
            raise DatabaseError(f"Failed to publish weekly chart: {str(e)}")
    
    @app.get("/admin/index", tags=["Admin"], summary="Admin publication index")
    async def admin_index(auth: bool = Depends(AuthenticationService.verify_admin)):
        """Admin-only detailed publication index"""
        try:
            return {
                "current_week": app.state.current_chart_week,
                "chart_history": db._chart_history[-10:],  # Last 10 weeks
                "statistics": {
                    "total_publications": len(db._chart_history),
                    "first_publication": db._chart_history[0]["week"] if db._chart_history else None,
                    "last_publication": db._chart_history[-1]["week"] if db._chart_history else None,
                    "songs_by_region": {
                        region: len(data["songs"])
                        for region, data in db._regions.items()
                    }
                },
                "database": {
                    "total_songs": len(db._songs),
                    "youtube_songs": len([s for s in db._songs if s.get("source", "").startswith("youtube_")]),
                    "radio_songs": len([s for s in db._songs if s.get("source", "").startswith("radio_")]),
                    "tv_songs": len([s for s in db._songs if s.get("source", "").startswith("tv_")])
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Admin index failed: {e}")
            raise DatabaseError(f"Failed to retrieve admin index: {str(e)}")
    
    # ========================================================================
    # METRICS ENDPOINT (Prometheus compatible)
    # ========================================================================
    
    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        """Prometheus metrics endpoint"""
        metrics_data = []
        
        # Application metrics
        metrics_data.append(f"ugboard_app_uptime_seconds {int((datetime.utcnow() - app.state.start_time).total_seconds())}")
        metrics_data.append(f"ugboard_app_requests_total {app.state.request_count}")
        metrics_data.append(f"ugboard_app_songs_total {len(db._songs)}")
        metrics_data.append(f"ugboard_app_regions_total {len(db._regions)}")
        
        # Region-specific metrics
        for region_code, region_data in db._regions.items():
            songs_count = len(region_data["songs"])
            total_plays = sum(s.get("plays", 0) for s in region_data["songs"])
            metrics_data.append(f'ugboard_region_songs_total{{region="{region_code}"}} {songs_count}')
            metrics_data.append(f'ugboard_region_plays_total{{region="{region_code}"}} {total_plays}')
        
        return Response(content="\n".join(metrics_data), media_type="text/plain")
    
    return app

# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

app = create_application()
db: Optional[HybridDatabase] = None
rate_limiter: Optional[RateLimitService] = None

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Configuration
    host = "0.0.0.0"
    port = int(os.getenv("PORT", 8000))
    
    # Startup message
    logger.info("UG Board Engine v8.3.1 starting...")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    logger.info(f"Chart Week: {app.state.current_chart_week}")
    logger.info(f"Data Directory: {settings.DATA_DIR}")
    logger.info(f"URL: http://{host}:{port}")
    logger.info(f"Docs: http://{host}:{port}/docs")
    logger.info(f"Health: http://{host}:{port}/health")
    
    # Run server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=True
    )
