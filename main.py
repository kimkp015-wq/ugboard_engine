"""
UG Board Engine - Production Ready Ugandan Music Chart System
Version: 8.3.0
Architecture: Clean Architecture with FastAPI
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
from functools import wraps
import hashlib

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator, ConfigDict, validator
import redis
from redis.exceptions import RedisError

# ====== CONFIGURATION ======
class Config:
    """Centralized configuration management"""
    
    # Environment
    ENVIRONMENT = os.getenv("ENV", "production").lower()
    DEBUG = ENVIRONMENT != "production"
    
    # Security Tokens (must match .env.example)
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin_ug_board_secure_token_2025_replace_this")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "ug_board_ingest_secure_token_2025_replace_this")
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "ug_board_internal_secure_token_2025_replace_this")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)  # Default to ingest token
    
    # Deployment
    PORT = int(os.getenv("PORT", 8000))
    HOST = os.getenv("HOST", "0.0.0.0")
    
    # Redis Configuration (for caching and persistence on Render)
    REDIS_URL = os.getenv("REDIS_URL", None)
    USE_REDIS = bool(REDIS_URL)
    
    # Data Management
    DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
    MAX_SONGS = int(os.getenv("MAX_SONGS", 10000))
    BACKUP_INTERVAL = int(os.getenv("BACKUP_INTERVAL", 300))  # seconds
    
    # Performance
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))
    MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", 10485760))  # 10MB
    
    # Ugandan Regions Configuration
    UGANDAN_REGIONS = {
        "central": {
            "name": "Central Region",
            "districts": ["Kampala", "Wakiso", "Mukono", "Luwero", "Masaka"],
            "musicians": ["Alien Skin", "Azawi", "Sheebah Karungi", "Vinka", "Eddy Kenzo", "Bobi Wine", "Daddy Andre"]
        },
        "western": {
            "name": "Western Region",
            "districts": ["Mbarara", "Kasese", "Ntungamo", "Kabale", "Fort Portal", "Hoima"],
            "musicians": ["Ray G", "T-Paul", "Truth 256", "Sister Charity", "Omega 256", "Rema Namakula"]
        },
        "eastern": {
            "name": "Eastern Region",
            "districts": ["Jinja", "Mbale", "Tororo", "Mayuge", "Soroti", "Iganga"],
            "musicians": ["Victor Ruz", "Davido Spider", "Temperature Touch", "Idi Amasaba", "Rexy", "Geosteady"]
        },
        "northern": {
            "name": "Northern Region",
            "districts": ["Gulu", "Lira", "Arua", "Kitgum"],
            "musicians": ["Bosmic Otim", "Odongo Romeo", "Eezzy", "Jenneth Prischa", "Laxzy Mover", "Fik Fameica"]
        }
    }
    
    VALID_REGIONS = set(UGANDAN_REGIONS.keys())
    
    @classmethod
    def validate(cls):
        """Validate configuration and set defaults"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        
        if cls.ENVIRONMENT == "production":
            if not cls.ADMIN_TOKEN or "replace_this" in cls.ADMIN_TOKEN:
                raise ValueError("ADMIN_TOKEN must be set in production")
            if not cls.INGEST_TOKEN or "replace_this" in cls.INGEST_TOKEN:
                raise ValueError("INGEST_TOKEN must be set in production")
        
        return cls
    
    @classmethod
    def get_allowed_origins(cls):
        """Get CORS allowed origins based on environment"""
        if cls.DEBUG:
            return ["*"]
        return [
            "https://ugboard-engine.onrender.com",
            "https://ugboard.vercel.app",
            "http://localhost:3000"
        ]

config = Config.validate()

# ====== LOGGING ======
class StructuredLogger:
    """Production-ready structured logging"""
    
    @staticmethod
    def setup():
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        log_level = logging.DEBUG if config.DEBUG else logging.INFO
        
        # JSON formatter for production, simple for development
        if config.ENVIRONMENT == "production":
            import json_log_formatter
            formatter = json_log_formatter.JSONFormatter()
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
        
        logging.basicConfig(
            level=log_level,
            handlers=[handler, logging.FileHandler(logs_dir / "ugboard_engine.log")]
        )
        
        logger = logging.getLogger("ugboard_engine")
        logger.info(f"Logging initialized for {config.ENVIRONMENT} environment")
        return logger

logger = StructuredLogger.setup()

# ====== MODELS ======
class SongItem(BaseModel):
    """Song data model with comprehensive validation"""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra='forbid'
    )
    
    title: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Song title",
        examples=["Nalumansi", "Kyarenga"]
    )
    
    artist: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Artist name",
        examples=["Bobi Wine", "Eddy Kenzo"]
    )
    
    plays: int = Field(
        default=0,
        ge=0,
        le=10000000,
        description="Number of plays",
        examples=[1000, 50000]
    )
    
    score: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description="Chart score (0-100)",
        examples=[85.5, 92.3]
    )
    
    station: Optional[str] = Field(
        default=None,
        max_length=50,
        description="TV/Radio station",
        examples=["NBS TV", "Capital FM"]
    )
    
    region: str = Field(
        default="central",
        pattern="^(central|western|eastern|northern)$",
        description="Ugandan region",
        examples=["central", "western"]
    )
    
    district: Optional[str] = Field(
        default=None,
        max_length=50,
        description="District within region",
        examples=["Kampala", "Mbarara"]
    )
    
    timestamp: Optional[str] = Field(
        default=None,
        description="ISO 8601 timestamp",
        examples=["2024-01-20T12:30:45Z"]
    )
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISO 8601 timestamp format"""
        if not v:
            return v
        
        try:
            # Handle Z suffix
            if v.endswith('Z'):
                v = v[:-1] + '+00:00'
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('Invalid ISO 8601 timestamp format. Expected format: YYYY-MM-DDTHH:MM:SS[Z]')
    
    @field_validator('district')
    @classmethod
    def validate_district(cls, v: Optional[str], info) -> Optional[str]:
        """Validate district belongs to specified region"""
        if not v:
            return v
        
        region = info.data.get('region')
        if not region:
            return v
        
        valid_districts = config.UGANDAN_REGIONS.get(region, {}).get("districts", [])
        if v not in valid_districts:
            raise ValueError(
                f"District '{v}' is not in {region.capitalize()} Region. "
                f"Valid districts: {', '.join(valid_districts)}"
            )
        return v

class IngestPayload(BaseModel):
    """Base ingestion payload with rate limiting"""
    model_config = ConfigDict(extra='forbid')
    
    items: List[SongItem] = Field(
        ...,
        min_items=1,
        max_items=1000,
        description="List of songs to ingest"
    )
    
    source: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Data source identifier",
        examples=["youtube_scraper_v1", "radio_capital_fm"]
    )
    
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata"
    )
    
    @validator('items')
    def validate_item_count(cls, v):
        """Validate reasonable ingestion size"""
        if len(v) > 1000:
            raise ValueError("Maximum 1000 items per ingestion")
        return v

class YouTubeIngestPayload(IngestPayload):
    """YouTube-specific ingestion payload"""
    channel_id: Optional[str] = Field(
        default=None,
        max_length=50,
        description="YouTube channel ID",
        examples=["UC1234567890abcdef"]
    )
    
    video_id: Optional[str] = Field(
        default=None,
        max_length=20,
        description="YouTube video ID",
        examples=["dQw4w9WgXcQ"]
    )
    
    category: str = Field(
        default="music",
        max_length=50,
        description="Content category",
        examples=["music", "entertainment"]
    )

# ====== DATABASE LAYER ======
class CacheManager:
    """Redis-based cache manager with fallback to memory"""
    
    def __init__(self, redis_url: Optional[str] = None):
        self.redis = None
        self.memory_cache = {}
        self.cache_ttl = 300  # 5 minutes default
        
        if redis_url:
            try:
                self.redis = redis.from_url(redis_url, decode_responses=True)
                self.redis.ping()
                logger.info("Redis cache initialized successfully")
            except RedisError as e:
                logger.warning(f"Redis connection failed: {e}. Using memory cache.")
                self.redis = None
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            if self.redis:
                value = self.redis.get(key)
                if value:
                    return json.loads(value)
            return self.memory_cache.get(key)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache"""
        try:
            ttl = ttl or self.cache_ttl
            json_value = json.dumps(value, default=str)
            
            if self.redis:
                return self.redis.setex(key, ttl, json_value)
            else:
                self.memory_cache[key] = value
                # Simple TTL simulation for memory cache
                asyncio.create_task(self._clear_memory_key(key, ttl))
                return True
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    async def _clear_memory_key(self, key: str, ttl: int):
        """Clear memory cache key after TTL"""
        await asyncio.sleep(ttl)
        if key in self.memory_cache:
            del self.memory_cache[key]
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            if self.redis:
                return bool(self.redis.delete(key))
            if key in self.memory_cache:
                del self.memory_cache[key]
                return True
            return False
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache keys matching pattern"""
        try:
            if self.redis:
                keys = self.redis.keys(pattern)
                if keys:
                    return self.redis.delete(*keys)
            # For memory cache, we can't easily pattern match
            return 0
        except Exception as e:
            logger.error(f"Cache pattern invalidation error: {e}")
            return 0

class JSONDatabase:
    """Production-grade database with Redis caching and file persistence"""
    
    def __init__(self, data_dir: Path, cache_manager: Optional[CacheManager] = None):
        self.data_dir = data_dir
        self.cache = cache_manager or CacheManager(None)
        
        # Initialize data structures
        self._data = {
            "songs": [],
            "chart_history": [],
            "regions": {},
            "youtube_songs": [],
            "metadata": {
                "total_ingestions": 0,
                "last_backup": None,
                "created_at": datetime.utcnow().isoformat()
            }
        }
        
        # Initialize regions
        for region_code, region_data in config.UGANDAN_REGIONS.items():
            self._data["regions"][region_code] = {
                "name": region_data["name"],
                "districts": region_data["districts"],
                "musicians": region_data["musicians"],
                "songs": [],
                "stats": {
                    "total_songs": 0,
                    "total_plays": 0,
                    "avg_score": 0.0
                }
            }
        
        # Load existing data
        self._load_data()
        
        # Start periodic backup if in production
        if config.ENVIRONMENT == "production":
            asyncio.create_task(self._periodic_backup())
    
    def _load_data(self):
        """Load data from files with error handling"""
        try:
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self._data["songs"] = json.load(f)
                
                # Process loaded songs
                self._process_loaded_songs()
                
                logger.info(f"Loaded {len(self._data['songs'])} songs from disk")
            
            history_file = self.data_dir / "chart_history.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    self._data["chart_history"] = json.load(f)
            
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load data: {e}")
            # Continue with empty data structures
    
    def _process_loaded_songs(self):
        """Process loaded songs into regions and youtube cache"""
        self._data["youtube_songs"] = [
            song for song in self._data["songs"]
            if song.get("source", "").startswith("youtube_")
        ]
        
        # Reset region songs
        for region in self._data["regions"].values():
            region["songs"] = []
        
        # Reassign songs to regions
        for song in self._data["songs"]:
            region = song.get("region")
            if region in self._data["regions"]:
                self._data["regions"][region]["songs"].append(song)
    
    async def _periodic_backup(self):
        """Periodic backup to prevent data loss"""
        while True:
            await asyncio.sleep(config.BACKUP_INTERVAL)
            try:
                self._save_data()
                logger.debug("Periodic backup completed")
            except Exception as e:
                logger.error(f"Periodic backup failed: {e}")
    
    def _save_data(self):
        """Save data to files with atomic writes"""
        try:
            # Save songs
            temp_file = self.data_dir / "songs.json.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self._data["songs"], f, indent=2, default=str)
            
            # Atomic replace
            songs_file = self.data_dir / "songs.json"
            temp_file.replace(songs_file)
            
            # Save chart history
            temp_file = self.data_dir / "chart_history.json.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self._data["chart_history"], f, indent=2, default=str)
            
            history_file = self.data_dir / "chart_history.json"
            temp_file.replace(history_file)
            
            self._data["metadata"]["last_backup"] = datetime.utcnow().isoformat()
            
        except IOError as e:
            logger.error(f"Failed to save data: {e}")
            raise
    
    def _generate_song_id(self) -> str:
        """Generate unique song ID"""
        timestamp = int(time.time() * 1000)
        random_suffix = hashlib.md5(str(timestamp).encode()).hexdigest()[:8]
        return f"song_{timestamp}_{random_suffix}"
    
    def add_songs(self, songs: List[SongItem], source: str) -> Dict[str, Any]:
        """
        Add songs with deduplication and region assignment
        Returns detailed statistics
        """
        added = 0
        duplicates = 0
        invalid = 0
        added_songs = []
        
        for song in songs:
            # Validate region
            if song.region not in config.VALID_REGIONS:
                invalid += 1
                continue
            
            # Create song dict
            song_dict = song.model_dump()
            song_dict.update({
                "source": source,
                "ingested_at": datetime.utcnow().isoformat(),
                "id": self._generate_song_id()
            })
            
            # Deduplication: same title + artist within 24 hours
            is_duplicate = any(
                s.get("title", "").lower() == song.title.lower() and
                s.get("artist", "").lower() == song.artist.lower() and
                datetime.fromisoformat(s.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
                datetime.utcnow() - timedelta(hours=24)
                for s in self._data["songs"][-500:]  # Check last 500 songs
            )
            
            if is_duplicate:
                duplicates += 1
                continue
            
            # Add to main songs list
            self._data["songs"].append(song_dict)
            added_songs.append(song_dict)
            
            # Add to region
            self._data["regions"][song.region]["songs"].append(song_dict)
            
            # Track YouTube songs
            if source.startswith("youtube_"):
                self._data["youtube_songs"].append(song_dict)
            
            added += 1
        
        # Enforce size limits
        if len(self._data["songs"]) > config.MAX_SONGS:
            excess = len(self._data["songs"]) - config.MAX_SONGS
            self._data["songs"] = self._data["songs"][excess:]
            logger.info(f"Trimmed {excess} songs to maintain limit of {config.MAX_SONGS}")
        
        # Save if songs were added
        if added > 0:
            self._save_data()
            self._data["metadata"]["total_ingestions"] += 1
            
            # Invalidate relevant caches
            self.cache.invalidate_pattern("get_top_songs:*")
            self.cache.invalidate_pattern("get_trending_songs:*")
            self.cache.invalidate_pattern("get_region_stats:*")
        
        return {
            "added": added,
            "duplicates": duplicates,
            "invalid": invalid,
            "total_songs": len(self._data["songs"]),
            "added_songs": added_songs[:10]  # Return first 10 for reference
        }
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs with caching"""
        cache_key = f"get_top_songs:{limit}:{region}"
        
        # Try cache first
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        # Determine source list
        if region and region in self._data["regions"]:
            source_list = self._data["regions"][region]["songs"]
        else:
            source_list = self._data["songs"]
        
        # Sort by score
        sorted_songs = sorted(
            source_list,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:limit]
        
        # Add ranks
        for i, song in enumerate(sorted_songs, 1):
            song["rank"] = i
        
        # Cache result
        self.cache.set(cache_key, sorted_songs, ttl=60)  # 1 minute cache
        
        return sorted_songs
    
    def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs using 8-hour window algorithm"""
        cache_key = f"get_trending_songs:{limit}"
        
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        if not self._data["youtube_songs"]:
            return []
        
        # Calculate current 8-hour window
        hours_since_epoch = int(time.time() // 3600)
        window_number = hours_since_epoch // 8
        
        # Filter recent songs (last 72 hours)
        recent_songs = [
            song for song in self._data["youtube_songs"]
            if datetime.fromisoformat(song.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
            datetime.utcnow() - timedelta(hours=72)
        ]
        
        if not recent_songs:
            return []
        
        # Deterministic ordering based on window number
        def sorting_key(song: Dict[str, Any]) -> Tuple[int, float]:
            # Create deterministic hash from window + song id
            hash_value = hash(f"{window_number}_{song.get('id', '')}") % 1000
            # Calculate popularity score
            popularity = song.get("score", 0) * 0.6 + song.get("plays", 0) * 0.4
            return (hash_value, popularity)
        
        sorted_songs = sorted(recent_songs, key=sorting_key, reverse=True)[:limit]
        
        # Add trend ranks
        for i, song in enumerate(sorted_songs, 1):
            song["trend_rank"] = i
        
        # Cache with shorter TTL (trending changes frequently)
        self.cache.set(cache_key, sorted_songs, ttl=30)
        
        return sorted_songs
    
    def get_current_trending_song(self) -> Optional[Dict[str, Any]]:
        """Get single trending song for current 8-hour window"""
        trending_songs = self.get_trending_songs(1)
        return trending_songs[0] if trending_songs else None
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive statistics for all regions"""
        cache_key = "get_region_stats"
        
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        stats = {}
        
        for region_code, region_data in self._data["regions"].items():
            songs = region_data["songs"]
            
            if songs:
                # Calculate statistics
                total_plays = sum(s.get("plays", 0) for s in songs)
                avg_score = sum(s.get("score", 0) for s in songs) / len(songs)
                
                # Top song
                top_song = max(songs, key=lambda x: x.get("score", 0))
                
                # Top artists
                artist_counts = {}
                for song in songs:
                    artist = song.get("artist", "")
                    artist_counts[artist] = artist_counts.get(artist, 0) + 1
                
                top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": len(songs),
                    "total_plays": total_plays,
                    "average_score": round(avg_score, 2),
                    "top_song": {
                        "title": top_song.get("title"),
                        "artist": top_song.get("artist"),
                        "score": top_song.get("score"),
                        "plays": top_song.get("plays", 0)
                    },
                    "top_artists": [
                        {"artist": artist, "song_count": count}
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
                    "average_score": 0.0,
                    "top_song": None,
                    "top_artists": [],
                    "districts": region_data["districts"],
                    "notable_musicians": region_data["musicians"],
                    "last_updated": datetime.utcnow().isoformat()
                }
        
        self.cache.set(cache_key, stats, ttl=120)  # 2 minutes cache
        
        return stats
    
    def publish_weekly_chart(self, current_week: str) -> Dict[str, Any]:
        """Publish weekly chart with comprehensive data"""
        snapshot = {
            "week": current_week,
            "published_at": datetime.utcnow().isoformat(),
            "top100": self.get_top_songs(100),
            "regions": {
                region: self.get_top_songs(5, region)
                for region in config.VALID_REGIONS
            },
            "region_stats": self.get_region_stats(),
            "trending_song": self.get_current_trending_song(),
            "metadata": {
                "total_songs": len(self._data["songs"]),
                "youtube_songs": len(self._data["youtube_songs"]),
                "weeks_published": len(self._data["chart_history"]) + 1
            }
        }
        
        self._data["chart_history"].append(snapshot)
        
        # Keep only last 52 weeks
        if len(self._data["chart_history"]) > 52:
            self._data["chart_history"] = self._data["chart_history"][-52:]
        
        self._save_data()
        
        # Invalidate caches
        self.cache.invalidate_pattern("*")
        
        return snapshot
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        return {
            "songs": {
                "total": len(self._data["songs"]),
                "youtube": len(self._data["youtube_songs"]),
                "by_region": {
                    region: len(data["songs"])
                    for region, data in self._data["regions"].items()
                }
            },
            "history": {
                "weeks_stored": len(self._data["chart_history"]),
                "last_publication": (
                    self._data["chart_history"][-1]["published_at"]
                    if self._data["chart_history"] else None
                )
            },
            "metadata": self._data["metadata"],
            "cache": {
                "enabled": self.cache.redis is not None,
                "type": "redis" if self.cache.redis else "memory"
            }
        }

# Initialize database with Redis caching
cache_manager = CacheManager(config.REDIS_URL)
db = JSONDatabase(config.DATA_DIR, cache_manager)

# ====== AUTHENTICATION ======
security = HTTPBearer(auto_error=False)

class AuthService:
    """Production authentication service with rate limiting"""
    
    # Track failed attempts for rate limiting
    _failed_attempts: Dict[str, List[float]] = {}
    _MAX_ATTEMPTS = 5
    _WINDOW_SECONDS = 300  # 5 minutes
    
    @classmethod
    def _check_rate_limit(cls, identifier: str) -> bool:
        """Check if request is rate limited"""
        now = time.time()
        
        # Clean old attempts
        if identifier in cls._failed_attempts:
            cls._failed_attempts[identifier] = [
                attempt for attempt in cls._failed_attempts[identifier]
                if now - attempt < cls._WINDOW_SECONDS
            ]
        
        # Check if exceeded limit
        if len(cls._failed_attempts.get(identifier, [])) >= cls._MAX_ATTEMPTS:
            logger.warning(f"Rate limit exceeded for {identifier}")
            return False
        
        return True
    
    @classmethod
    def _record_failed_attempt(cls, identifier: str):
        """Record a failed authentication attempt"""
        now = time.time()
        if identifier not in cls._failed_attempts:
            cls._failed_attempts[identifier] = []
        cls._failed_attempts[identifier].append(now)
    
    @classmethod
    def verify_admin(
        cls,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
        request: Request = None
    ) -> bool:
        """Verify admin token with rate limiting"""
        client_ip = request.client.host if request else "unknown"
        
        if not cls._check_rate_limit(f"admin_{client_ip}"):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many authentication attempts. Please try again later."
            )
        
        if not config.ADMIN_TOKEN:
            logger.error("Admin token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Administration authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.ADMIN_TOKEN:
            cls._record_failed_attempt(f"admin_{client_ip}")
            logger.warning(f"Failed admin authentication attempt from {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing administration token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return True
    
    @classmethod
    def verify_ingest(
        cls,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
        request: Request = None
    ) -> bool:
        """Verify ingestion token with rate limiting"""
        client_ip = request.client.host if request else "unknown"
        
        if not cls._check_rate_limit(f"ingest_{client_ip}"):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many authentication attempts. Please try again later."
            )
        
        if not config.INGEST_TOKEN:
            logger.error("Ingest token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ingestion authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.INGEST_TOKEN:
            cls._record_failed_attempt(f"ingest_{client_ip}")
            logger.warning(f"Failed ingest authentication attempt from {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing ingestion token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return True
    
    @classmethod
    def verify_youtube(
        cls,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
        request: Request = None
    ) -> bool:
        """Verify YouTube ingestion token with rate limiting"""
        client_ip = request.client.host if request else "unknown"
        
        if not cls._check_rate_limit(f"youtube_{client_ip}"):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many authentication attempts. Please try again later."
            )
        
        if not config.YOUTUBE_TOKEN:
            logger.error("YouTube token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="YouTube authentication not configured"
            )
        
        if not credentials or credentials.credentials != config.YOUTUBE_TOKEN:
            cls._record_failed_attempt(f"youtube_{client_ip}")
            logger.warning(f"Failed YouTube authentication attempt from {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing YouTube ingestion token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return True

# ====== VALIDATION SERVICE ======
class ValidationService:
    """Comprehensive content validation for Ugandan music"""
    
    # Extended list of Ugandan artists
    UGANDAN_ARTISTS = {
        # Central Region
        "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
        "gravity", "vyroota", "geosteady", "feffe busi",
        "jose chameleone", "bebe cool", "radio and weasel",
        "alien skin", "azawi", "vinka", "rema", "cindy",
        "fille", "lyrical", "navio", "keko", "gnl zamba",
        "pallaso", "winnie nwagi", "spice diana", "john blaq",
        "zex bilangilangi", "pia pounds", "catherine kusasira",
        
        # Western Region
        "ray g", "t-paul", "truth 256", "sister charity", 
        "omega 256", "rema namakula", "irinah", "mickie wine",
        "rickman manrick", "queen of sheeba", "mesach semakula",
        
        # Eastern Region
        "victor ruz", "davido spider", "temperature touch", 
        "idi amasaba", "rexy", "kadongo kamu",
        "moses matasi", "betty kawala", "judith babirye",
        
        # Northern Region
        "bosmic otim", "odongo romeo", "eezzy", 
        "jenneth prischa", "laxzy mover", "fik fameica",
        "lilian mbabazi", "martha mukisa", "fille muwayira",
        
        # Groups and Bands
        "goodlyfe", "ghetto kids", "trio marino", "acholi blues",
        "eagles production", "team no sleep", "swangz avenue"
    }
    
    # Common Ugandan name patterns
    UGANDAN_PATTERNS = [
        " omutujju", " omulangira", " kawalya", " kigozi",
        " nakimera", " nakitto", " namale", " nantale",
        " namagembe", " namuli", " nakyanzi", " nanyonjo",
        " ssemakula", " ssematimba", " ssenyonga", " ssempebwa"
    ]
    
    @classmethod
    def is_ugandan_artist(cls, artist_name: str) -> bool:
        """Check if artist is Ugandan using multiple strategies"""
        if not artist_name or not isinstance(artist_name, str):
            return False
        
        artist_lower = artist_name.lower().strip()
        
        # 1. Check exact match in known artists
        if artist_lower in cls.UGANDAN_ARTISTS:
            return True
        
        # 2. Check for known artist within the name
        for known_artist in cls.UGANDAN_ARTISTS:
            if known_artist in artist_lower:
                return True
        
        # 3. Check for Ugandan name patterns
        for pattern in cls.UGANDAN_PATTERNS:
            if pattern in artist_lower:
                return True
        
        # 4. Check for common Ugandan prefixes/suffixes
        ugandan_indicators = [" ug ", " uganda", " kampala", " mbarara"]
        for indicator in ugandan_indicators:
            if indicator in artist_lower:
                return True
        
        # 5. Check for known music groups/labels
        ugandan_groups = ["team no sleep", "swangz", "muthaland", "kama"]
        for group in ugandan_groups:
            if group in artist_lower:
                return True
        
        return False
    
    @classmethod
    def validate_ugandan_content(cls, song: SongItem) -> Tuple[bool, List[str]]:
        """Validate song content and return reasons for any failures"""
        validation_errors = []
        
        # Check artist
        if not cls.is_ugandan_artist(song.artist):
            validation_errors.append(f"Artist '{song.artist}' is not recognized as Ugandan")
        
        # Check region
        if song.region not in config.VALID_REGIONS:
            validation_errors.append(f"Region '{song.region}' is not a valid Ugandan region")
        
        # Check district if provided
        if song.district:
            valid_districts = config.UGANDAN_REGIONS.get(song.region, {}).get("districts", [])
            if song.district not in valid_districts:
                validation_errors.append(
                    f"District '{song.district}' is not in {song.region.capitalize()} Region"
                )
        
        # Check score range
        if not 0 <= song.score <= 100:
            validation_errors.append(f"Score {song.score} is not between 0 and 100")
        
        # Check plays
        if song.plays < 0:
            validation_errors.append(f"Plays {song.plays} cannot be negative")
        
        return len(validation_errors) == 0, validation_errors

# ====== TRENDING SERVICE ======
class TrendingService:
    """Advanced trending algorithm service"""
    
    @staticmethod
    def get_current_trending_window() -> Dict[str, Any]:
        """Get information about current trending window"""
        current_time = time.time()
        hours_since_epoch = int(current_time // 3600)
        
        # Calculate 8-hour window
        window_number = hours_since_epoch // 8
        window_start_hour = (window_number * 8) % 24
        window_end_hour = (window_start_hour + 8) % 24
        
        # Calculate time until next window
        next_window_start = (window_number + 1) * 8 * 3600
        seconds_remaining = max(0, next_window_start - current_time)
        
        return {
            "window_number": window_number,
            "window_start_utc": f"{window_start_hour:02d}:00",
            "window_end_utc": f"{window_end_hour:02d}:00",
            "seconds_remaining": int(seconds_remaining),
            "current_utc_hour": datetime.utcnow().hour,
            "description": f"8-hour window {window_start_hour:02d}:00-{window_end_hour:02d}:00 UTC",
            "next_window_in": f"{int(seconds_remaining // 3600)}h {int((seconds_remaining % 3600) // 60)}m"
        }

# ====== LIFECYCLE MANAGEMENT ======
current_chart_week = datetime.utcnow().strftime("%Y-W%W")
app_start_time = datetime.utcnow()
request_count = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle with proper startup/shutdown"""
    # Startup
    startup_message = """
    UG Board Engine v8.3.0 - Ugandan Music Charts
    ---------------------------------------------
    Environment: {}
    Database: {} songs loaded
    Redis Cache: {}
    Regions: {}
    Chart Week: {}
    Trending Window: {}
    ---------------------------------------------
    """.format(
        config.ENVIRONMENT,
        len(db._data["songs"]),
        "Enabled" if cache_manager.redis else "Disabled",
        ", ".join(sorted(config.VALID_REGIONS)),
        current_chart_week,
        TrendingService.get_current_trending_window()["description"]
    )
    
    logger.info(startup_message)
    
    # Initialize background tasks
    background_tasks = set()
    
    # Health check task
    async def health_monitor():
        while True:
            await asyncio.sleep(60)
            db_stats = db.get_database_stats()
            logger.debug(
                f"Health check: {db_stats['songs']['total']} songs, "
                f"{db_stats['songs']['youtube']} YouTube songs"
            )
    
    if config.ENVIRONMENT == "production":
        task = asyncio.create_task(health_monitor())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    
    yield
    
    # Shutdown
    shutdown_message = """
    UG Board Engine shutting down
    ----------------------------
    Total requests served: {}
    Total songs in database: {}
    Uptime: {}
    ----------------------------
    """.format(
        request_count,
        len(db._data["songs"]),
        str(datetime.utcnow() - app_start_time).split('.')[0]
    )
    
    logger.info(shutdown_message)
    
    # Cancel background tasks
    for task in background_tasks:
        task.cancel()
    
    # Wait for tasks to complete
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    
    # Final save
    try:
        db._save_data()
        logger.info("Final database save completed")
    except Exception as e:
        logger.error(f"Final save failed: {e}")

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine - Ugandan Music Charts",
    version="8.3.0",
    description="""
    Production-ready Ugandan Music Chart System with:
    
    • Regional chart support for Central, Western, Eastern, Northern Uganda
    • 8-hour trending song rotation algorithm
    • YouTube, Radio, and TV ingestion endpoints
    • Redis caching for high performance
    • Comprehensive authentication and rate limiting
    
    **Environment:** {}
    **Chart Week:** {}
    **Regions:** {}
    """.format(config.ENVIRONMENT, current_chart_week, ", ".join(sorted(config.VALID_REGIONS))),
    docs_url="/docs" if config.DEBUG else None,
    redoc_url="/redoc" if config.DEBUG else None,
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "Root",
            "description": "Service information and health checks"
        },
        {
            "name": "Charts",
            "description": "Ugandan music chart endpoints"
        },
        {
            "name": "Regions",
            "description": "Regional chart data and statistics"
        },
        {
            "name": "Trending",
            "description": "Trending songs with 8-hour rotation"
        },
        {
            "name": "Ingestion",
            "description": "Data ingestion endpoints with authentication"
        },
        {
            "name": "Admin",
            "description": "Administrative functions"
        },
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Response-Time"]
)

app.add_middleware(
    GZipMiddleware,
    minimum_size=1000
)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Request ID middleware
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = hashlib.md5(f"{time.time()}{request.client.host}".encode()).hexdigest()[:8]
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response

# ====== API ENDPOINTS ======
@app.get("/", tags=["Root"], summary="Service information")
async def root(request: Request):
    """Root endpoint with comprehensive service information"""
    global request_count
    request_count += 1
    
    window_info = TrendingService.get_current_trending_window()
    db_stats = db.get_database_stats()
    
    return {
        "service": "UG Board Engine",
        "version": "8.3.0",
        "status": "online",
        "environment": config.ENVIRONMENT,
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "trending_window": window_info,
        "database": {
            "total_songs": db_stats["songs"]["total"],
            "youtube_songs": db_stats["songs"]["youtube"],
            "weeks_history": db_stats["history"]["weeks_stored"]
        },
        "regions": {
            "available": list(config.VALID_REGIONS),
            "count": len(config.VALID_REGIONS)
        },
        "cache": db_stats["cache"],
        "endpoints": {
            "documentation": "/docs" if config.DEBUG else "Disabled in production",
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
                "radio": "/ingest/radio",
                "tv": "/ingest/tv"
            },
            "admin": {
                "health": "/admin/health",
                "publish": "/admin/publish/weekly",
                "stats": "/admin/stats"
            }
        },
        "request_id": getattr(request.state, 'request_id', 'unknown')
    }

@app.get("/health", tags=["Root"], summary="Health check")
async def health():
    """Comprehensive health check endpoint"""
    uptime = datetime.utcnow() - app_start_time
    window_info = TrendingService.get_current_trending_window()
    db_stats = db.get_database_stats()
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "uptime_human": str(uptime).split('.')[0],
        "requests_served": request_count,
        "chart_week": current_chart_week,
        "trending_window": window_info,
        "database": {
            "status": "connected",
            "songs": db_stats["songs"]["total"],
            "regions": len(db_stats["songs"]["by_region"]),
            "last_backup": db_stats["metadata"].get("last_backup")
        },
        "cache": {
            "status": "connected" if cache_manager.redis else "memory_only",
            "type": "redis" if cache_manager.redis else "memory"
        },
        "environment": config.ENVIRONMENT,
        "version": "8.3.0"
    }
    
    return health_status

@app.get("/charts/top100", tags=["Charts"], summary="Uganda Top 100 chart")
async def get_top100(
    limit: int = Query(100, ge=1, le=200, description="Number of songs to return"),
    region: Optional[str] = Query(None, description="Filter by region")
):
    """
    Get Uganda Top 100 chart for current week.
    
    Optionally filter by region to get regional top songs.
    """
    try:
        songs = db.get_top_songs(limit, region)
        
        response_data = {
            "chart": "Uganda Top 100" + (f" - {region.capitalize()} Region" if region else ""),
            "week": current_chart_week,
            "entries": songs,
            "count": len(songs),
            "region": region if region else "all",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error fetching top songs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch chart data"
        )

@app.get("/charts/index", tags=["Charts"], summary="Chart publication index")
async def get_chart_index():
    """Get chart publication history index"""
    chart_history = db._data["chart_history"]
    
    return {
        "current_week": current_chart_week,
        "available_weeks": [h["week"] for h in chart_history[-10:]],
        "history_count": len(chart_history),
        "last_published": chart_history[-1]["published_at"] if chart_history else None,
        "first_published": chart_history[0]["published_at"] if chart_history else None,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/regions", tags=["Charts", "Regions"], summary="All region statistics")
async def get_all_regions():
    """Get comprehensive statistics for all Ugandan regions"""
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

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"], summary="Region detail")
async def get_region_detail(
    region: str = FPath(..., description="Ugandan region: central, western, eastern, northern")
):
    """Get detailed information and top songs for a specific region"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    try:
        # Get top songs for region
        songs = db.get_top_songs(10, region)
        
        # Get region statistics
        region_stats = db.get_region_stats().get(region, {})
        region_data = config.UGANDAN_REGIONS[region]
        
        # Add ranks to songs
        for i, song in enumerate(songs, 1):
            song["rank"] = i
        
        response_data = {
            "region": region,
            "region_name": region_data["name"],
            "chart_week": current_chart_week,
            "songs": songs,
            "statistics": region_stats,
            "districts": region_data["districts"],
            "notable_musicians": region_data["musicians"],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error fetching region data for {region}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch region data for {region}"
        )

@app.get("/charts/trending", tags=["Charts", "Trending"], summary="Trending songs")
async def get_trending(
    limit: int = Query(10, ge=1, le=50, description="Number of trending songs")
):
    """
    Get currently trending songs using 8-hour window rotation algorithm.
    
    Songs are selected based on a deterministic algorithm that changes every 8 hours,
    ensuring fair rotation of content.
    """
    try:
        songs = db.get_trending_songs(limit)
        window_info = TrendingService.get_current_trending_window()
        
        response_data = {
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
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error fetching trending songs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch trending songs"
        )

@app.get("/charts/trending/now", tags=["Charts", "Trending"], summary="Current trending song")
async def get_current_trending():
    """Get the single trending song for the current 8-hour window"""
    try:
        trending_song = db.get_current_trending_song()
        window_info = TrendingService.get_current_trending_window()
        
        if not trending_song:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No trending song available for current window"
            )
        
        return {
            "trending_song": trending_song,
            "trending_window": window_info,
            "next_change": window_info["next_window_in"],
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

@app.post("/ingest/youtube", tags=["Ingestion"], summary="Ingest YouTube data")
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube),
    request: Request = None
):
    """
    Ingest YouTube data with Ugandan content validation.
    
    Only songs by recognized Ugandan artists are processed.
    """
    try:
        valid_items = []
        validation_results = []
        
        for item in payload.items:
            is_valid, errors = ValidationService.validate_ugandan_content(item)
            
            if is_valid:
                item_dict = item.model_dump()
                item_dict["source"] = f"youtube_{payload.source}"
                item_dict["category"] = payload.category
                
                if payload.channel_id:
                    item_dict["channel_id"] = payload.channel_id
                if payload.video_id:
                    item_dict["video_id"] = payload.video_id
                
                valid_items.append(item_dict)
                validation_results.append({
                    "song": f"{item.title} - {item.artist}",
                    "status": "accepted",
                    "region": item.region
                })
            else:
                validation_results.append({
                    "song": f"{item.title} - {item.artist}",
                    "status": "rejected",
                    "errors": errors
                })
        
        # Add to database
        result = db.add_songs([SongItem(**item) for item in valid_items], f"youtube_{payload.source}")
        
        # Log ingestion
        logger.info(
            f"YouTube ingestion from {payload.source}: "
            f"{result['added']} added, {result['duplicates']} duplicates, "
            f"{len(validation_results) - len(valid_items)} rejected"
        )
        
        # Prepare response
        response_data = {
            "status": "success",
            "message": f"Processed {len(payload.items)} YouTube songs",
            "source": payload.source,
            "results": {
                "total_received": len(payload.items),
                "valid_ugandan": len(valid_items),
                "added_to_database": result["added"],
                "duplicates_skipped": result["duplicates"],
                "rejected_non_ugandan": len(payload.items) - len(valid_items)
            },
            "validation_summary": validation_results[:10],  # First 10 for reference
            "region_breakdown": {
                region: sum(1 for item in valid_items if item.get("region") == region)
                for region in config.VALID_REGIONS
            },
            "timestamp": datetime.utcnow().isoformat(),
            "request_id": getattr(request.state, 'request_id', 'unknown') if request else None
        }
        
        return response_data
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"YouTube ingestion failed: {str(e)}"
        )

@app.post("/ingest/radio", tags=["Ingestion"], summary="Ingest radio data")
async def ingest_radio(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest radio station data"""
    try:
        result = db.add_songs(payload.items, f"radio_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Processed {len(payload.items)} radio songs",
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

@app.post("/ingest/tv", tags=["Ingestion"], summary="Ingest TV data")
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest TV station data"""
    try:
        result = db.add_songs(payload.items, f"tv_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Processed {len(payload.items)} TV songs",
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

@app.get("/admin/health", tags=["Admin"], summary="Admin health check")
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Detailed health check with system information (admin only)"""
    uptime = datetime.utcnow() - app_start_time
    db_stats = db.get_database_stats()
    window_info = TrendingService.get_current_trending_window()
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "environment": config.ENVIRONMENT,
            "uptime": str(uptime).split('.')[0],
            "uptime_seconds": int(uptime.total_seconds()),
            "requests_served": request_count,
            "python_version": sys.version,
            "host": config.HOST,
            "port": config.PORT
        },
        "database": db_stats,
        "trending": window_info,
        "cache": {
            "enabled": cache_manager.redis is not None,
            "type": "redis" if cache_manager.redis else "memory",
            "redis_connected": cache_manager.redis is not None
        },
        "authentication": {
            "admin_configured": bool(config.ADMIN_TOKEN),
            "ingest_configured": bool(config.INGEST_TOKEN),
            "youtube_configured": bool(config.YOUTUBE_TOKEN),
            "tokens_secure": all(
                "replace_this" not in token for token in [
                    config.ADMIN_TOKEN, config.INGEST_TOKEN, config.YOUTUBE_TOKEN
                ] if token
            )
        },
        "limits": {
            "max_songs": config.MAX_SONGS,
            "backup_interval": config.BACKUP_INTERVAL,
            "max_upload_size": config.MAX_UPLOAD_SIZE
        }
    }

@app.post("/admin/publish/weekly", tags=["Admin"], summary="Publish weekly chart")
async def publish_weekly(auth: bool = Depends(AuthService.verify_admin)):
    """Publish weekly chart for all regions (admin only)"""
    try:
        result = db.publish_weekly_chart(current_chart_week)
        
        logger.info(f"Weekly chart published for week {current_chart_week}")
        
        return {
            "status": "success",
            "message": "Weekly chart published successfully",
            "week": result["week"],
            "published_at": result["published_at"],
            "summary": {
                "top100_songs": len(result["top100"]),
                "regions_published": len(result["regions"]),
                "total_songs": len(db._data["songs"]),
                "youtube_songs": len(db._data["youtube_songs"])
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Weekly publish error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish weekly chart: {str(e)}"
        )

@app.get("/admin/stats", tags=["Admin"], summary="Admin statistics")
async def admin_stats(auth: bool = Depends(AuthService.verify_admin)):
    """Detailed system statistics (admin only)"""
    db_stats = db.get_database_stats()
    region_stats = db.get_region_stats()
    
    # Calculate some analytics
    total_plays = sum(stats["total_plays"] for stats in region_stats.values())
    avg_score = (
        sum(stats["average_score"] * stats["total_songs"] for stats in region_stats.values()) /
        max(1, sum(stats["total_songs"] for stats in region_stats.values()))
    )
    
    return {
        "statistics": {
            "overall": {
                "total_songs": db_stats["songs"]["total"],
                "total_plays": total_plays,
                "average_score": round(avg_score, 2),
                "youtube_songs": db_stats["songs"]["youtube"],
                "weeks_published": db_stats["history"]["weeks_stored"],
                "total_ingestions": db_stats["metadata"].get("total_ingestions", 0)
            },
            "by_region": db_stats["songs"]["by_region"],
            "region_details": region_stats
        },
        "performance": {
            "requests_served": request_count,
            "uptime_seconds": int((datetime.utcnow() - app_start_time).total_seconds()),
            "chart_week": current_chart_week,
            "current_trending_window": TrendingService.get_current_trending_window()
        },
        "metadata": db_stats["metadata"],
        "timestamp": datetime.utcnow().isoformat()
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with structured logging"""
    logger.warning(
        f"HTTP {exc.status_code} at {request.url.path}: {exc.detail}",
        extra={
            "status_code": exc.status_code,
            "path": request.url.path,
            "method": request.method,
            "client_ip": request.client.host if request.client else "unknown",
            "request_id": getattr(request.state, 'request_id', 'unknown')
        }
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": getattr(request.state, 'request_id', 'unknown')
        },
        headers=exc.headers
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    logger.error(
        f"Unhandled exception at {request.url.path}: {exc}",
        exc_info=True,
        extra={
            "path": request.url.path,
            "method": request.method,
            "client_ip": request.client.host if request.client else "unknown",
            "request_id": getattr(request.state, 'request_id', 'unknown')
        }
    )
    
    error_detail = str(exc) if config.DEBUG else "Internal server error"
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": error_detail,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": getattr(request.state, 'request_id', 'unknown')
        }
    )

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    
    # Startup banner (without emojis for production compatibility)
    banner = """
    ============================================
    UG Board Engine v8.3.0 - Ugandan Music Charts
    ============================================
    Environment: {}
    Database: {} songs loaded
    Redis Cache: {}
    Regions: {}
    Chart Week: {}
    ============================================
    Server: http://{}:{}
    Docs: http://{}:{}/docs
    Health: http://{}:{}/health
    ============================================
    """.format(
        config.ENVIRONMENT,
        len(db._data["songs"]),
        "Enabled" if cache_manager.redis else "Disabled",
        ", ".join(sorted(config.VALID_REGIONS)),
        current_chart_week,
        config.HOST, config.PORT,
        config.HOST, config.PORT,
        config.HOST, config.PORT
    )
    
    print(banner)
    
    # Run server
    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_level="info" if config.ENVIRONMENT == "production" else "debug",
        timeout_keep_alive=config.REQUEST_TIMEOUT,
        access_log=True if config.DEBUG else False
    )
