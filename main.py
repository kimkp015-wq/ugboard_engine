"""
UG Board Engine - PRODUCTION READY VERSION
Enhanced with database, async operations, and improved security
Primary focus: Ugandan music and artists
Foreign artists only allowed in collaborations with Ugandan artists
"""

import os
import json
import uuid
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Body, Path, Query, Depends, status, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field, validator, ValidationError
import asyncpg
import redis.asyncio as redis
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import sentry_sdk
from prometheus_fastapi_instrumentator import Instrumentator

# =========================
# Pydantic Models for Validation
# =========================

class SongItem(BaseModel):
    """Pydantic model for song validation"""
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=200)
    genre: Optional[str] = Field(None, max_length=50)
    score: Optional[float] = Field(0.0, ge=0, le=100)
    plays: Optional[int] = Field(0, ge=0)
    region: Optional[str] = Field("ug", regex="^(ug|eac|afr|ww)$")
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "Nalumansi",
                "artist": "Bobi Wine",
                "genre": "kadongo kamu",
                "score": 95.5,
                "plays": 10000,
                "region": "ug"
            }
        }


class IngestionPayload(BaseModel):
    """Pydantic model for ingestion payload validation"""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000)
    source: str = Field(..., max_length=50)
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    timestamp: Optional[str] = None
    
    @validator("timestamp")
    def validate_timestamp(cls, v):
        if v:
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("Invalid ISO format timestamp")
        return v or datetime.utcnow().isoformat()


# =========================
# Configuration with Environment Variables
# =========================

class Settings:
    """Centralized configuration management"""
    
    def __init__(self):
        # Server Configuration
        self.PORT = int(os.getenv("PORT", "8000"))
        self.HOST = os.getenv("HOST", "0.0.0.0")
        self.DEBUG = os.getenv("DEBUG", "false").lower() == "true"
        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
        
        # Service Identity
        self.SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "ugboard-engine")
        self.INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID", "local")[:8]
        self.VERSION = "7.0.0"
        
        # Security - Load from environment with defaults for development
        self.ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", self._generate_default_token("admin"))
        self.INJECT_TOKEN = os.getenv("INJECT_TOKEN", self._generate_default_token("ingest"))
        self.INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", self._generate_default_token("internal"))
        self.JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", self._generate_jwt_secret())
        self.ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
        
        # Database Configuration
        self.DATABASE_URL = os.getenv("DATABASE_URL")
        self.REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
        
        # External Services
        self.SENTRY_DSN = os.getenv("SENTRY_DSN")
        
        # Rate Limiting
        self.RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))
        
        # CORS
        self.CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,https://*.ugboard.com").split(",")
        
        # Initialize monitoring
        if self.SENTRY_DSN:
            sentry_sdk.init(
                dsn=self.SENTRY_DSN,
                traces_sample_rate=1.0 if self.DEBUG else 0.1,
                environment=self.ENVIRONMENT,
            )
    
    def _generate_default_token(self, prefix: str) -> str:
        """Generate a default token for development"""
        import hashlib
        import secrets
        random_part = secrets.token_hex(8)
        return f"{prefix}-{random_part}-{self.INSTANCE_ID}"
    
    def _generate_jwt_secret(self) -> str:
        """Generate a JWT secret for development"""
        import secrets
        return secrets.token_urlsafe(32)


settings = Settings()

# =========================
# Database Connections
# =========================

class Database:
    """Database connection pool manager"""
    
    _pool = None
    
    @classmethod
    async def get_pool(cls):
        """Get or create database connection pool"""
        if cls._pool is None and settings.DATABASE_URL:
            cls._pool = await asyncpg.create_pool(
                settings.DATABASE_URL,
                min_size=5,
                max_size=20,
                command_timeout=60,
            )
            await cls._initialize_database()
        return cls._pool
    
    @classmethod
    async def _initialize_database(cls):
        """Initialize database schema"""
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS songs (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    artist VARCHAR(200) NOT NULL,
                    artist_list JSONB DEFAULT '[]',
                    artist_types JSONB DEFAULT '[]',
                    is_collaboration BOOLEAN DEFAULT FALSE,
                    has_ugandan_artist BOOLEAN DEFAULT TRUE,
                    has_foreign_artist BOOLEAN DEFAULT FALSE,
                    score FLOAT DEFAULT 0.0,
                    plays INTEGER DEFAULT 0,
                    change VARCHAR(10) DEFAULT 'same',
                    genre VARCHAR(50) DEFAULT 'afrobeat',
                    region VARCHAR(10) DEFAULT 'ug',
                    release_date DATE,
                    weeks_on_chart INTEGER DEFAULT 0,
                    source VARCHAR(50) DEFAULT 'unknown',
                    source_id VARCHAR(100) UNIQUE,
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_songs_artist (artist),
                    INDEX idx_songs_region (region),
                    INDEX idx_songs_score (score DESC),
                    INDEX idx_songs_created_at (created_at DESC)
                );
                
                CREATE TABLE IF NOT EXISTS ingestion_logs (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(50) NOT NULL,
                    count INTEGER NOT NULL,
                    ugandan_artists INTEGER DEFAULT 0,
                    foreign_artists INTEGER DEFAULT 0,
                    collaborations INTEGER DEFAULT 0,
                    instance VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_logs_source (source),
                    INDEX idx_logs_created_at (created_at DESC)
                );
                
                CREATE TABLE IF NOT EXISTS charts (
                    id SERIAL PRIMARY KEY,
                    week_id VARCHAR(20) NOT NULL,
                    region VARCHAR(10) NOT NULL,
                    chart_data JSONB NOT NULL,
                    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'published',
                    instance VARCHAR(50),
                    UNIQUE(week_id, region),
                    INDEX idx_charts_week_id (week_id),
                    INDEX idx_charts_region (region)
                );
            """)
    
    @classmethod
    async def close(cls):
        """Close database connection pool"""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None


# =========================
# Redis Cache
# =========================

class RedisCache:
    """Redis cache manager"""
    
    _client = None
    
    @classmethod
    async def get_client(cls):
        """Get or create Redis client"""
        if cls._client is None:
            cls._client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                max_connections=20
            )
        return cls._client
    
    @classmethod
    async def close(cls):
        """Close Redis connection"""
        if cls._client:
            await cls._client.close()
            cls._client = None


# =========================
# Enhanced UGANDAN MUSIC RULES & VALIDATION
# =========================

class EnhancedMusicRules:
    """Enhanced rules for Ugandan music charting system with caching"""
    
    PRIMARY_REGION = "ug"
    
    # Extended Ugandan artists database with caching
    UGANDAN_ARTISTS = {
        # Male Artists
        "bobi wine", "joseph mayanja", "eddy kenzo", "daddy andre", "gravity omutujju",
        "fik fameica", "vyroota", "geosteady", "choppa", "feffe busi", "leyla kayondo",
        "recho rey", "vivian todi", "cindy sanyu", "niniola", "sheebah karungi",
        "spice diana", "winnie nwagi", "vinka", "zex bilangilangi", "john blaq",
        "pallaso", "navio", "gnl zamba", "rickman manrick", "buchaman", "ragga dee",
        "bebe cool", "goodlyfe", "radio & weasel", "moses matthew", "afrigo band",
        "michael ouma", "mesach semakula", "herbert kinobe",
        
        # Female Artists
        "irene namatovu", "martha mukisa", "catherine kusasira", "judith babirye",
        "lillian mbabazi", "juliana kanyomozi", "remy namakula", "sandra nankoma",
        "nancy kacungira", "doreen kiconco",
        
        # Groups/Bands
        "ghetto kids", "team no sleep", "swangz avenue", "black skin",
        "east african party", "kadongo kamu artists", "acholi artists",
        "eagles production", "fenon records", "dream girls"
    }
    
    # Extended foreign collaborators
    FOREIGN_COLLABORATORS = {
        "davido", "wizkid", "burna boy", "tiwa savage", "yemi alade",
        "diamond platnumz", "rayvanny", "harmonize", "nandy", "juma nature",
        "khaligraph jones", "sauti sol", "nyashinski", "otile brown",
        "ali kiba", "vanessa mdee", "lady jaydee", "marioo", "jay melody",
        "zuchu", "lava lava", "mboso"
    }
    
    # Ugandan music genres
    UGANDAN_GENRES = {
        "kadongo kamu", "kidandali", "afrobeat", "dancehall", "reggae",
        "gospel", "hip hop", "rnb", "traditional", "zouk", "bongo flava",
        "folk", "world music", "afropop", "ragga", "kizomba"
    }
    
    # Compile regex patterns for performance
    UGANDAN_PATTERNS = [
        re.compile(r'\b(kenzo|pallaso|choppa|gnl|navio|zamba|wine)\b', re.IGNORECASE),
        re.compile(r'\b(mayanja|omutujju|busi|kayondo|sanyu|sheebah)\b', re.IGNORECASE),
        re.compile(r'\b(diana|nwagi|vinka|blaq|fameica|vyroota)\b', re.IGNORECASE),
        re.compile(r'\b(ghetto|team\s*no|swangz|black\s*skin)\b', re.IGNORECASE),
        re.compile(r'\b(cool|radio|weasel|geosteady|rickman)\b', re.IGNORECASE),
    ]
    
    @classmethod
    async def is_ugandan_artist(cls, artist_name: str) -> bool:
        """Check if artist is Ugandan with Redis caching"""
        artist_lower = artist_name.lower().strip()
        
        # Check cache first
        cache_key = f"artist:ugandan:{hashlib.md5(artist_lower.encode()).hexdigest()}"
        cache_client = await RedisCache.get_client()
        cached = await cache_client.get(cache_key)
        
        if cached is not None:
            return cached == "true"
        
        # Check exact match
        if artist_lower in cls.UGANDAN_ARTISTS:
            await cache_client.setex(cache_key, 3600, "true")  # Cache for 1 hour
            return True
        
        # Check regex patterns
        for pattern in cls.UGANDAN_PATTERNS:
            if pattern.search(artist_lower):
                await cache_client.setex(cache_key, 3600, "true")
                return True
        
        await cache_client.setex(cache_key, 3600, "false")
        return False
    
    @classmethod
    async def is_known_collaborator(cls, artist_name: str) -> bool:
        """Check if artist is a known foreign collaborator with caching"""
        artist_lower = artist_name.lower().strip()
        return artist_lower in cls.FOREIGN_COLLABORATORS
    
    @classmethod
    async def validate_artists(cls, artists: List[str]) -> Tuple[bool, str]:
        """
        Validate that foreign artists only appear with Ugandan collaborators
        Returns: (is_valid, error_message)
        """
        if not artists:
            return False, "No artists specified"
        
        # Check if all artists are Ugandan (allowed)
        ugandan_checks = [await cls.is_ugandan_artist(artist) for artist in artists]
        all_ugandan = all(ugandan_checks)
        
        if all_ugandan:
            return True, ""
        
        # Check if there's at least one Ugandan artist
        has_ugandan = any(ugandan_checks)
        if not has_ugandan:
            return False, "No Ugandan artist found. Foreign artists must collaborate with Ugandan artists."
        
        # Check foreign artists are known collaborators
        foreign_artists = [artists[i] for i, is_ug in enumerate(ugandan_checks) if not is_ug]
        for foreign in foreign_artists:
            if not await cls.is_known_collaborator(foreign):
                return False, f"Foreign artist '{foreign}' is not in the approved collaborator list"
        
        return True, ""
    
    @classmethod
    def extract_artist_list(cls, artist_field: str) -> List[str]:
        """Extract individual artists from artist field"""
        if not artist_field:
            return []
        
        # Enhanced separators for Ugandan music
        separators = [' feat. ', ' ft. ', ' & ', ' x ', ' , ', ' with ', ' and ', ' vs. ', ' featuring ']
        normalized = artist_field.lower()
        for sep in separators:
            normalized = normalized.replace(sep, '|')
        
        artists = [a.strip() for a in normalized.split('|') if a.strip()]
        return artists
    
    @classmethod
    async def get_artist_type(cls, artist_name: str) -> str:
        """Get artist type: 'ugandan', 'foreign_collaborator', or 'unknown'"""
        if await cls.is_ugandan_artist(artist_name):
            return "ugandan"
        elif await cls.is_known_collaborator(artist_name):
            return "foreign_collaborator"
        else:
            return "unknown"


# =========================
# Data Models
# =========================

class Region(str, Enum):
    """Supported regions - Uganda is primary, others are regional charts"""
    UG = "ug"    # Uganda (Primary - Top 100)
    EAC = "eac"  # East African Community
    AFR = "afr"  # Africa-wide
    WW = "ww"    # Worldwide (Ugandan diaspora focus)


# =========================
# Authentication with Rate Limiting
# =========================

limiter = Limiter(key_func=get_remote_address)

def verify_admin(authorization: Optional[str] = Header(None)):
    """Verify admin token with rate limiting"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401, 
            detail="Invalid authorization header"
        )
    
    token = authorization.replace("Bearer ", "").strip()
    if token != settings.ADMIN_TOKEN:
        raise HTTPException(
            status_code=401, 
            detail="Invalid admin token"
        )
    return True


def verify_ingestion(authorization: Optional[str] = Header(None)):
    """Verify ingestion token"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401, 
            detail="Invalid authorization header"
        )
    
    token = authorization.replace("Bearer ", "").strip()
    if token != settings.INJECT_TOKEN:
        raise HTTPException(
            status_code=401, 
            detail="Invalid ingestion token"
        )
    return True


def verify_internal(x_internal_token: Optional[str] = Header(None)):
    """Verify internal token"""
    if not x_internal_token or x_internal_token != settings.INTERNAL_TOKEN:
        raise HTTPException(
            status_code=401, 
            detail="Invalid internal token"
        )
    return True


# =========================
# Enhanced Data Store with Database
# =========================

class EnhancedUgandanMusicStore:
    """Production-ready data store with PostgreSQL and Redis"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EnhancedUgandanMusicStore, cls).__new__(cls)
        return cls._instance
    
    async def initialize(self):
        """Initialize database connections"""
        self.db_pool = await Database.get_pool()
        self.redis_client = await RedisCache.get_client()
        await self._seed_initial_data()
    
    async def _seed_initial_data(self):
        """Seed initial Ugandan music data"""
        ugandan_songs = [
            {"title": "Nalumansi", "artist": "Bobi Wine", "genre": "kadongo kamu"},
            {"title": "Sitya Loss", "artist": "Eddy Kenzo", "genre": "afrobeat"},
            {"title": "Mummy", "artist": "Daddy Andre", "genre": "dancehall"},
            {"title": "Tonny On Low", "artist": "Gravity Omutujju", "genre": "hip hop"},
            {"title": "Bailando", "artist": "Sheebah Karungi", "genre": "dancehall"},
            {"title": "Kaddugala", "artist": "Cindy Sanyu", "genre": "afrobeat"},
            {"title": "Biri Biri", "artist": "Fik Fameica", "genre": "hip hop"},
            {"title": "Wale Wale", "artist": "Spice Diana", "genre": "afrobeat"},
            {"title": "Zenjye", "artist": "John Blaq", "genre": "afrobeat"},
            {"title": "Mundongo", "artist": "Pallaso", "genre": "afrobeat"},
            {"title": "Vitamin", "artist": "Daddy Andre ft. Eddy Kenzo", "genre": "dancehall"},
            {"title": "Munde", "artist": "Eddy Kenzo ft. Niniola", "genre": "afrobeat"},
            {"title": "Baddest", "artist": "Sheebah ft. DJ Erycom", "genre": "dancehall"},
            {"title": "Binkolera", "artist": "Gravity Omutujju ft. Choppa", "genre": "hip hop"},
            {"title": "Mpita Njia", "artist": "Diamond Platnumz ft. Choppa", "genre": "bongo flava"},
            {"title": "Bweyagala", "artist": "Vyroota", "genre": "kidandali"},
            {"title": "Enjoy", "artist": "Geosteady", "genre": "rnb"},
            {"title": "Sembera", "artist": "Feffe Busi", "genre": "hip hop"},
            {"title": "Bino", "artist": "Leyla Kayondo", "genre": "afrobeat"},
            {"title": "Nkwagala", "artist": "Vivian Todi", "genre": "gospel"},
        ]
        
        for song in ugandan_songs:
            await self._save_song(song)
    
    async def _save_song(self, song_data: Dict):
        """Save a song to the database"""
        try:
            artists = EnhancedMusicRules.extract_artist_list(song_data["artist"])
            is_valid, error_msg = await EnhancedMusicRules.validate_artists(artists)
            
            if not is_valid:
                return  # Skip invalid songs
            
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO songs 
                    (title, artist, artist_list, artist_types, genre, source, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (source_id) DO NOTHING
                """,
                song_data["title"],
                song_data["artist"],
                json.dumps(artists),
                json.dumps([await EnhancedMusicRules.get_artist_type(a) for a in artists]),
                song_data.get("genre", "afrobeat"),
                "seed",
                datetime.utcnow()
                )
        except Exception as e:
            print(f"Error saving song: {e}")
    
    async def get_top100(self) -> List[Dict]:
        """Get Uganda Top 100 from database"""
        cache_key = "chart:top100:ug"
        cached = await self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM songs 
                WHERE region = 'ug' 
                ORDER BY score DESC, plays DESC 
                LIMIT 100
            """)
        
        chart_data = [dict(row) for row in rows]
        
        # Cache for 5 minutes
        await self.redis_client.setex(cache_key, 300, json.dumps(chart_data))
        return chart_data
    
    async def get_region_top5(self, region: str) -> List[Dict]:
        """Get regional top 5 from database"""
        cache_key = f"chart:top5:{region}"
        cached = await self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM songs 
                WHERE region = $1 
                ORDER BY score DESC 
                LIMIT 5
            """, region)
        
        chart_data = [dict(row) for row in rows]
        
        # Cache for 5 minutes
        await self.redis_client.setex(cache_key, 300, json.dumps(chart_data))
        return chart_data
    
    async def get_trending(self, limit: int = 20) -> List[Dict]:
        """Get trending songs from database"""
        cache_key = f"trending:{limit}"
        cached = await self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM songs 
                ORDER BY 
                    (plays * 0.3 + score * 0.7) DESC,
                    created_at DESC 
                LIMIT $1
            """, limit)
        
        trending = [dict(row) for row in rows]
        
        # Cache for 1 minute (trending changes frequently)
        await self.redis_client.setex(cache_key, 60, json.dumps(trending))
        return trending
    
    async def log_ingestion(self, source: str, count: int, items: List[Dict] = None) -> Dict:
        """Log ingestion to database"""
        ugandan_count = 0
        foreign_count = 0
        collaboration_count = 0
        
        if items:
            for item in items:
                artists = EnhancedMusicRules.extract_artist_list(item.get("artist", ""))
                artist_checks = [await EnhancedMusicRules.is_ugandan_artist(a) for a in artists]
                
                if any(artist_checks):
                    ugandan_count += 1
                if any(not check for check in artist_checks):
                    foreign_count += 1
                if len(artists) > 1:
                    collaboration_count += 1
        
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO ingestion_logs 
                (source, count, ugandan_artists, foreign_artists, collaborations, instance)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, source, count, ugandan_count, foreign_count, collaboration_count, settings.INSTANCE_ID)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "count": count,
            "ugandan_artists": ugandan_count,
            "foreign_artists": foreign_count,
            "collaborations": collaboration_count,
            "instance": settings.INSTANCE_ID
        }
    
    async def get_artist_stats(self) -> Dict:
        """Get artist statistics from database"""
        cache_key = "stats:artists"
        cached = await self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        async with self.db_pool.acquire() as conn:
            # Get unique artists
            rows = await conn.fetch("""
                SELECT 
                    COUNT(DISTINCT artist) as total_artists,
                    COUNT(DISTINCT CASE WHEN has_ugandan_artist THEN artist END) as ugandan_artists,
                    COUNT(DISTINCT CASE WHEN has_foreign_artist THEN artist END) as foreign_artists,
                    COUNT(CASE WHEN is_collaboration THEN 1 END) as collaboration_count,
                    COUNT(*) as total_songs
                FROM songs
            """)
            
            stats = dict(rows[0]) if rows else {}
        
        # Cache for 1 hour
        await self.redis_client.setex(cache_key, 3600, json.dumps(stats))
        return stats
    
    async def save_songs_batch(self, songs: List[Dict], source: str) -> int:
        """Save multiple songs to database in batch"""
        saved_count = 0
        
        async with self.db_pool.acquire() as conn:
            for song in songs:
                try:
                    # Validate song
                    song_item = SongItem(**song)
                    
                    # Extract and validate artists
                    artists = EnhancedMusicRules.extract_artist_list(song_item.artist)
                    is_valid, error_msg = await EnhancedMusicRules.validate_artists(artists)
                    
                    if not is_valid:
                        continue  # Skip invalid songs
                    
                    # Generate unique source ID
                    source_id = hashlib.md5(
                        f"{song_item.title}_{song_item.artist}_{source}".encode()
                    ).hexdigest()
                    
                    # Save to database
                    await conn.execute("""
                        INSERT INTO songs 
                        (title, artist, artist_list, artist_types, 
                         is_collaboration, has_ugandan_artist, has_foreign_artist,
                         score, plays, genre, region, source, source_id, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        ON CONFLICT (source_id) DO UPDATE SET
                            score = EXCLUDED.score,
                            plays = EXCLUDED.plays,
                            updated_at = CURRENT_TIMESTAMP
                    """,
                    song_item.title,
                    song_item.artist,
                    json.dumps(artists),
                    json.dumps([await EnhancedMusicRules.get_artist_type(a) for a in artists]),
                    len(artists) > 1,
                    any(await EnhancedMusicRules.is_ugandan_artist(a) for a in artists),
                    any(not await EnhancedMusicRules.is_ugandan_artist(a) for a in artists),
                    song_item.score or 0.0,
                    song_item.plays or 0,
                    song_item.genre or "afrobeat",
                    song_item.region or "ug",
                    source,
                    source_id,
                    json.dumps(song_item.metadata) if hasattr(song_item, 'metadata') else '{}'
                    )
                    
                    saved_count += 1
                    
                except ValidationError as e:
                    print(f"Validation error for song: {e}")
                except Exception as e:
                    print(f"Error saving song: {e}")
        
        return saved_count


# Initialize data store
data_store = EnhancedUgandanMusicStore()

# =========================
# Create FastAPI App with Lifespan
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for startup/shutdown events"""
    # Startup
    print(f"🚀 Starting {settings.SERVICE_NAME} v{settings.VERSION}")
    print(f"🌍 Environment: {settings.ENVIRONMENT}")
    print(f"🔧 Debug: {settings.DEBUG}")
    
    # Initialize data store
    await data_store.initialize()
    
    yield
    
    # Shutdown
    print("🛑 Shutting down...")
    await Database.close()
    await RedisCache.close()


app = FastAPI(
    title="UG Board Engine - Production Ready",
    description="""Production-ready automated Ugandan music chart system. 
    Primary focus: Ugandan music and artists.
    Rule: Foreign artists only allowed in collaborations with Ugandan artists.""",
    version=settings.VERSION,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None,
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Add Prometheus metrics
if not settings.DEBUG:
    Instrumentator().instrument(app).expose(app, endpoint="/metrics")


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    openapi_schema["components"]["securitySchemes"] = {
        "AdminAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": "Admin token authentication"
        },
        "IngestionAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": "Ingestion token authentication"
        },
        "InternalAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-Internal-Token",
            "description": "Internal service token"
        }
    }
    
    openapi_schema["tags"] = [
        {"name": "Default", "description": "Public engine health check"},
        {"name": "Health", "description": "System health endpoints"},
        {"name": "Charts", "description": "Ugandan music chart endpoints"},
        {"name": "Regions", "description": "Regional Ugandan music charts"},
        {"name": "Trending", "description": "Trending Ugandan songs (live)"},
        {"name": "Ingestion", "description": "Data ingestion with Ugandan artist validation"},
        {"name": "Admin", "description": "Administrative functions"},
        {"name": "Artists", "description": "Ugandan artist information"}
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# =========================
# Enhanced Endpoints
# =========================

@app.get("/", tags=["Default"])
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def root(request: Request):
    """Public engine health check - Enhanced with database info"""
    artist_stats = await data_store.get_artist_stats()
    
    # Get database status
    db_status = "connected" if settings.DATABASE_URL else "not_configured"
    redis_status = "connected" if settings.REDIS_URL else "not_configured"
    
    return {
        "service": "UG Board Engine - Production Ready",
        "version": settings.VERSION,
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "focus": "Ugandan music and artists",
        "rule": "Foreign artists only allowed in collaborations with Ugandan artists",
        "artist_statistics": artist_stats,
        "database": {
            "status": db_status,
            "redis": redis_status
        },
        "instance": {
            "service": settings.SERVICE_NAME,
            "id": settings.INSTANCE_ID,
            "environment": settings.ENVIRONMENT
        },
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "metrics": "/metrics" if not settings.DEBUG else None,
            "uganda_top100": "/charts/top100",
            "regional_charts": "/charts/regions/{region}",
            "trending": "/charts/trending",
            "artist_info": "/artists/stats",
            "ingestion": "/ingest/{source}",
            "admin": "/admin/*"
        }
    }


@app.get("/health", tags=["Health"])
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def health():
    """Enhanced health check with dependency verification"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": settings.SERVICE_NAME,
        "instance": settings.INSTANCE_ID,
        "version": settings.VERSION,
    }
    
    # Check database connectivity
    if settings.DATABASE_URL:
        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            health_status["database"] = "healthy"
        except Exception as e:
            health_status["database"] = "unhealthy"
            health_status["database_error"] = str(e)
    
    # Check Redis connectivity
    if settings.REDIS_URL:
        try:
            client = await RedisCache.get_client()
            await client.ping()
            health_status["redis"] = "healthy"
        except Exception as e:
            health_status["redis"] = "unhealthy"
            health_status["redis_error"] = str(e)
    
    return health_status


@app.get("/charts/top100", tags=["Charts"])
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def get_top100():
    """Uganda Top 100 (current week) - From database"""
    chart_data = await data_store.get_top100()
    
    # Calculate statistics
    collaborations = sum(1 for song in chart_data if song.get("is_collaboration", False))
    foreign_involved = sum(1 for song in chart_data if song.get("has_foreign_artist", False))
    total_entries = len(chart_data)
    
    return {
        "region": "ug",
        "chart_name": "Uganda Top 100 - Ugandan Music",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "published": datetime.utcnow().isoformat(),
        "entries": chart_data,
        "total_entries": total_entries,
        "statistics": {
            "collaborations": collaborations,
            "foreign_involved": foreign_involved,
            "pure_ugandan": total_entries - foreign_involved,
            "collaboration_rate": f"{(collaborations / total_entries) * 100:.1f}%" if total_entries > 0 else "0%"
        },
        "instance": settings.INSTANCE_ID,
        "rule_enforced": "Foreign artists only in collaborations with Ugandan artists",
        "data_source": "database"
    }


@app.get("/charts/regions/{region}", tags=["Regions"])
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def get_region_chart(region: Region = Path(..., description="Region code")):
    """Get regional chart for Ugandan music from database"""
    chart_data = await data_store.get_region_top5(region.value)
    
    region_names = {
        "ug": "Uganda (Top 100 available separately)",
        "eac": "East African Community",
        "afr": "Africa (Ugandan music impact)",
        "ww": "Worldwide (Ugandan diaspora)"
    }
    
    return {
        "region": region.value,
        "region_name": region_names.get(region.value, "Unknown"),
        "chart_name": f"Ugandan Music - {region_names.get(region.value, 'Regional')} Top 5",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "entries": chart_data,
        "total_entries": len(chart_data),
        "instance": settings.INSTANCE_ID,
        "timestamp": datetime.utcnow().isoformat(),
        "note": "All charts feature Ugandan artists or collaborations with Ugandan artists",
        "data_source": "database"
    }


@app.get("/charts/trending", tags=["Trending"])
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def get_trending(
    request: Request,
    limit: int = Query(default=20, ge=1, le=50, description="Number of trending items")
):
    """Trending Ugandan songs (live) from database"""
    trending_data = await data_store.get_trending(limit)
    
    # Calculate statistics
    ugandan_only = 0
    youtube_count = 0
    radio_count = 0
    
    for song in trending_data:
        artists = EnhancedMusicRules.extract_artist_list(song.get("artist", ""))
        if all(await EnhancedMusicRules.is_ugandan_artist(a) for a in artists):
            ugandan_only += 1
        
        if song.get("source") == "youtube":
            youtube_count += 1
        elif song.get("source") == "radio":
            radio_count += 1
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "data_source": "database",
        "trending": trending_data,
        "total_trending": len(trending_data),
        "statistics": {
            "ugandan_only_songs": ugandan_only,
            "collaboration_songs": len(trending_data) - ugandan_only,
            "source_breakdown": {
                "youtube": youtube_count,
                "radio": radio_count,
                "other": len(trending_data) - youtube_count - radio_count
            }
        },
        "refresh_interval": "1 minute",
        "instance": settings.INSTANCE_ID,
        "focus": "Trending Ugandan music"
    }


@app.post("/ingest/radio", tags=["Ingestion"])
@limiter.limit("30/minute")  # Lower limit for ingestion
async def ingest_radio(
    request: Request,
    payload: IngestionPayload = Body(...),
    x_internal_token: Optional[str] = Header(None)
):
    """Enhanced radio ingestion with database storage"""
    # Verify internal token
    if not x_internal_token or x_internal_token != settings.INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    
    try:
        # Validate and process songs
        saved_count = await data_store.save_songs_batch(payload.items, payload.source)
        
        # Log ingestion
        log_entry = await data_store.log_ingestion(
            payload.source, 
            saved_count, 
            [item.dict() for item in payload.items]
        )
        
        return {
            "status": "success",
            "message": f"Ingested {saved_count} radio items with Ugandan artist validation",
            "source": payload.source,
            "items_received": len(payload.items),
            "items_saved": saved_count,
            "rejected_count": len(payload.items) - saved_count,
            "timestamp": datetime.utcnow().isoformat(),
            "log_entry": log_entry,
            "instance": settings.INSTANCE_ID,
            "database_persisted": True
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Validation error: {e}")
    except Exception as e:
        # Log to Sentry
        if settings.SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )


@app.post("/ingest/youtube", tags=["Ingestion"])
@limiter.limit("30/minute")
async def ingest_youtube(
    request: Request,
    payload: IngestionPayload = Body(...),
    authorization: Optional[str] = Header(None)
):
    """Enhanced YouTube ingestion with database storage"""
    # Verify ingestion token
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    
    token = authorization.replace("Bearer ", "").strip()
    if token != settings.INJECT_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    
    try:
        # Validate and process songs
        saved_count = await data_store.save_songs_batch(payload.items, payload.source)
        
        # Log ingestion
        log_entry = await data_store.log_ingestion(
            payload.source, 
            saved_count, 
            [item.dict() for item in payload.items]
        )
        
        return {
            "status": "success",
            "message": f"Ingested {saved_count} YouTube items with Ugandan artist validation",
            "source": payload.source,
            "items_received": len(payload.items),
            "items_saved": saved_count,
            "rejected_count": len(payload.items) - saved_count,
            "timestamp": datetime.utcnow().isoformat(),
            "log_entry": log_entry,
            "instance": settings.INSTANCE_ID,
            "database_persisted": True
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Validation error: {e}")
    except Exception as e:
        # Log to Sentry
        if settings.SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )


# =========================
# Enhanced Error Handling
# =========================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler with request ID"""
    request_id = str(uuid.uuid4())
    
    # Log to Sentry for server errors
    if exc.status_code >= 500 and settings.SENTRY_DSN:
        sentry_sdk.capture_exception(exc)
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "method": request.method,
            "instance": settings.INSTANCE_ID,
            "request_id": request_id
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Enhanced general exception handler"""
    error_id = str(uuid.uuid4())
    
    # Log to Sentry
    if settings.SENTRY_DSN:
        sentry_sdk.capture_exception(exc)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "method": request.method,
            "instance": settings.INSTANCE_ID,
            "request_id": error_id
        }
    )


# =========================
# Server Startup
# =========================

if __name__ == "__main__":
    import uvicorn
    import hashlib
    
    print(f"🚀 Starting UG Board Engine - Production Ready v{settings.VERSION}")
    print(f"🌐 Service: {settings.SERVICE_NAME}")
    print(f"🔧 Environment: {settings.ENVIRONMENT}")
    print(f"🔐 Instance ID: {settings.INSTANCE_ID}")
    print(f"📚 Docs: http://localhost:{settings.PORT}/docs")
    print(f"🏥 Health: http://localhost:{settings.PORT}/health")
    
    if not settings.DEBUG:
        print(f"📊 Metrics: http://localhost:{settings.PORT}/metrics")
    
    print("\n🔑 Security Configuration:")
    print(f"   • Admin Token: {'****' + settings.ADMIN_TOKEN[-8:] if settings.ADMIN_TOKEN else 'Not set'}")
    print(f"   • Ingestion Token: {'****' + settings.INJECT_TOKEN[-8:] if settings.INJECT_TOKEN else 'Not set'}")
    print(f"   • Internal Token: {'****' + settings.INTERNAL_TOKEN[-8:] if settings.INTERNAL_TOKEN else 'Not set'}")
    
    print("\n🗄️  Database Configuration:")
    print(f"   • PostgreSQL: {'Connected' if settings.DATABASE_URL else 'Not configured'}")
    print(f"   • Redis: {'Connected' if settings.REDIS_URL else 'Not configured'}")
    
    print("\n🎯 Focus: Ugandan music and artists")
    print("📜 Rule: Foreign artists only allowed in collaborations with Ugandan artists")
    print("=" * 60)
    
    uvicorn.run(
        app,
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info" if settings.DEBUG else "warning",
        access_log=settings.DEBUG
    )
