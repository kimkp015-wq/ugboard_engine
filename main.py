"""
"""
UG Board Engine - Production Ready Architecture v8.3.0
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
    Query, Path as FPath, Request, status, APIRouter
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator, ConfigDict, SecretStr
from pydantic_settings import BaseSettings
from prometheus_fastapi_instrumentator import Instrumentator

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
        default=["https://ugboard-engine.onrender.com"],
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
        if self.ENVIRONMENT == Environment.PRODUCTION:
            if not self.ADMIN_TOKEN.get_secret_value():
                raise ValueError("ADMIN_TOKEN must be set in production")
            if not self.INGEST_TOKEN.get_secret_value():
                raise ValueError("INGEST_TOKEN must be set in production")
        
        # Create directories
        self.DATA_DIR.mkdir(exist_ok=True, parents=True)
        self.LOGS_DIR.mkdir(exist_ok=True, parents=True)
        
        logger.info(f"Configuration validated for {self.ENVIRONMENT} environment")

settings = Settings()

# ============================================================================
# LOGGING & MONITORING
# ============================================================================

def setup_structured_logging():
    """Configure structured JSON logging for production"""
    logs_dir = settings.LOGS_DIR
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # JSON formatter for production, simpler for development
    if settings.ENVIRONMENT == Environment.PRODUCTION:
        import structlog
        from pythonjsonlogger import jsonlogger
        
        formatter = jsonlogger.JsonFormatter(
            fmt='%(asctime)s %(name)s %(levelname)s %(message)s %(pathname)s %(lineno)d'
        )
        log_level = logging.INFO
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        log_level = logging.DEBUG
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # File handler
    file_handler = logging.FileHandler(logs_dir / "ugboard_engine.json")
    file_handler.setFormatter(formatter)
    
    # Setup root logger
    logging.basicConfig(
        level=log_level,
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
    
    @field_validator('artist')
    @classmethod
    def normalize_artist(cls, v: str) -> str:
        """Normalize artist name"""
        return ' '.join(word.capitalize() for word in v.split())
    
    @field_validator('title')
    @classmethod
    def normalize_title(cls, v: str) -> str:
        """Normalize song title"""
        return ' '.join(word.capitalize() for word in v.split())

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
                logger.info("✅ Redis cache initialized")
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
        
        # Check region
        if song.region not in [r.value for r in Region]:
            return False, f"Invalid region: {song.region}"
        
        # Check district belongs to region
        if song.district:
            region_districts = {
                Region.CENTRAL: [d.value for d in [
                    District.KAMPALA, District.WAKISO, District.MUKONO,
                    District.LUWERO, District.MASAKA
                ]],
                Region.WESTERN: [d.value for d in [
                    District.MBARARA, District.KASESE, District.NTUNGAMO,
                    District.KABALE, District.FORT_PORTAL, District.HOIMA
                ]],
                Region.EASTERN: [d.value for d in [
                    District.JINJA, District.MBALE, District.TORORO,
                    District.MAYUGE, District.SOROTI, District.IGANGA
                ]],
                Region.NORTHERN: [d.value for d in [
                    District.GULU, District.LIRA, District.ARUA, District.KITGUM
                ]]
            }
            
            if song.district.value not in region_districts[song.region]:
                return False, f"District {song.district} is not in {song.region.value} region"
        
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
        logger.info("🚀 UG Board Engine starting up")
        logger.info(f"🌍 Environment: {settings.ENVIRONMENT}")
        logger.info(f"⚙️ Configuration loaded: {len(settings.model_fields)} settings")
        
        # Initialize database
        global db, rate_limiter
        db = HybridDatabase(settings.DATA_DIR, settings.REDIS_URL)
        rate_limiter = RateLimitService(db._redis)
        
        # Wait for initialization
        await asyncio.sleep(0.1)  # Small delay for async init
        
        # Initial stats
        songs_count = len(db._songs)
        logger.info(f"📊 Database initialized: {songs_count} songs loaded")
        
        if songs_count == 0:
            logger.info("📝 Database is empty - ready for ingestion")
        
        yield
        
        # Shutdown
        logger.info("🛑 UG Board Engine shutting down")
        logger.info(f"📈 Final stats: {len(db._songs)} songs processed")
        if db._redis:
            try:
                await asyncio.to_thread(db._redis.close)
            except:
                pass
    
    # Create FastAPI application
    app = FastAPI(
        title="UG Board Engine",
        version="8.3.0",
        description="Production-ready Ugandan Music Chart System with Regional Support",
        docs_url="/docs" if settings.ENVIRONMENT != Environment.PRODUCTION else None,
        redoc_url="/redoc" if settings.ENVIRONMENT != Environment.PRODUCTION else None,
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
    
    # Trusted Hosts
    if settings.ENVIRONMENT == Environment.PRODUCTION:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=["ugboard-engine.onrender.com", "localhost", "127.0.0.1"]
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
    
    # Rate limiting middleware
    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/"]:
            return await call_next(request)
        
        client_ip = request.client.host if request.client else "unknown"
        endpoint = request.url.path
        
        rate_limit_key = f"rate_limit:{client_ip}:{endpoint}"
        
        if not await rate_limiter.check_rate_limit(
            rate_limit_key,
            settings.RATE_LIMIT_REQUESTS,
            settings.RATE_LIMIT_WINDOW
        ):
            raise RateLimitError(
                f"Rate limit exceeded. Maximum {settings.RATE_LIMIT_REQUESTS} "
                f"requests per {settings.RATE_LIMIT_WINDOW} seconds"
            )
        
        return await call_next(request)
    
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
            "version": "8.3.0",
            "status": "online",
            "environment": settings.ENVIRONMENT,
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": int((datetime.utcnow() - app.state.start_time).total_seconds()),
            "request_count": app.state.request_count,
            "chart_week": app.state.current_chart_week,
            "features": {
                "trending_window_hours": settings.TRENDING_WINDOW_HOURS,
                "cache_enabled": db._redis is not None,
                "max_db_size": settings.MAX_DB_SIZE,
                "rate_limiting": True
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
            "version": "8.3.0",
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
                "trending_window": settings.TRENDING_WINDOW_HOURS,
                "rate_limit": f"{settings.RATE_LIMIT_REQUESTS}/{settings.RATE_LIMIT_WINDOW}s"
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
    workers = settings.MAX_WORKERS if settings.ENVIRONMENT == Environment.PRODUCTION else 1
    
    # Startup banner
    logger.info(f"""
    {'='*60}
    🎵 UG Board Engine v8.3.0
    🌍 Environment: {settings.ENVIRONMENT}
    ⚙️  Workers: {workers}
    📅 Chart Week: {app.state.current_chart_week}
    🗄️  Data Directory: {settings.DATA_DIR}
    🌐 URL: http://{host}:{port}
    📚 Docs: http://{host}:{port}/docs
    🩺 Health: http://{host}:{port}/health
    📊 Metrics: http://{host}:{port}/metrics
    {'='*60}
    """)
    
    # Run server
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        workers=workers,
        log_level="info" if settings.ENVIRONMENT == Environment.PRODUCTION else "debug",
        timeout_keep_alive=settings.REQUEST_TIMEOUT,
        access_log=True if settings.ENVIRONMENT != Environment.PRODUCTION else False
    )UG Board Engine - Production Ready with Pydantic v2 Fix
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
