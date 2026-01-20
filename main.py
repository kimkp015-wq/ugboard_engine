"""
UG Board Engine - Complete Production System v9.0.0
Architecture: Clean Architecture + FastAPI + SQLite + Redis + YouTube Worker Integration
"""

import os
import sys
import json
import time
import asyncio
import logging
import sqlite3
import aiosqlite
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple, AsyncGenerator
from contextlib import asynccontextmanager
from enum import Enum
from functools import wraps
import hashlib

import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
import redis.asyncio as redis
from pydantic import BaseModel, Field, field_validator, ConfigDict, validator, EmailStr
import aiofiles
from aiofiles import os as aiofiles_os

# ====== CONFIGURATION ======
class Environment(str, Enum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TESTING = "testing"

class Config:
    """Complete configuration management"""
    
    # Environment
    ENV = Environment(os.getenv("ENV", "production").lower())
    DEBUG = ENV == Environment.DEVELOPMENT
    TESTING = ENV == Environment.TESTING
    
    # Security Tokens (from .env.example)
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
    INGEST_TOKEN = os.getenv("INGEST_TOKEN", "")
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
    YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)
    
    # YouTube Worker Integration
    YOUTUBE_WORKER_URL = os.getenv("YOUTUBE_WORKER_URL", "https://ugboard-youtube-puller.kimkp015.workers.dev")
    YOUTUBE_WORKER_TOKEN = os.getenv("YOUTUBE_WORKER_TOKEN", YOUTUBE_TOKEN)
    
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./ugboard.db")
    SQLITE_PATH = Path(os.getenv("SQLITE_PATH", "data/ugboard.db"))
    
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    REDIS_PREFIX = os.getenv("REDIS_PREFIX", "ugboard:")
    
    # Deployment
    PORT = int(os.getenv("PORT", 8000))
    HOST = os.getenv("HOST", "0.0.0.0")
    
    # Chart Rules
    CHART_WEEK_START = os.getenv("CHART_WEEK_START", "MONDAY")  # Monday = start of chart week
    PUBLISH_TIME = os.getenv("PUBLISH_TIME", "12:00")  # 12:00 UTC weekly publish
    TRENDING_WINDOW_HOURS = int(os.getenv("TRENDING_WINDOW_HOURS", 8))
    
    # Ugandan Music Rules
    UGANDAN_MUSIC_RULES = {
        "minimum_ugandan_content_percentage": 70,  # 70% of chart must be Ugandan
        "allow_collaborations": True,
        "foreign_artists_allowed": True,
        "minimum_ugandan_artist_percentage": 60,  # In collaborations
        "regional_quotas": {
            "central": 40,  # Percentage of chart
            "western": 25,
            "eastern": 20,
            "northern": 15
        }
    }
    
    # Ugandan Regions (Enhanced)
    UGANDAN_REGIONS = {
        "central": {
            "name": "Central Region",
            "capital": "Kampala",
            "districts": ["Kampala", "Wakiso", "Mukono", "Luwero", "Masaka", "Mpigi", "Kalangala", "Kalungu", "Bukomansimbi", "Gomba"],
            "dialects": ["Luganda", "English"],
            "musical_styles": ["Kadongo Kamu", "Kidandali", "Afrobeat", "Dancehall"],
            "iconic_venues": ["National Theatre", "Lugogo Cricket Oval", "Kololo Airstrip"],
            "famous_musicians": [
                {"name": "Bobi Wine", "genre": "Kadongo Kamu", "era": "2000s-present"},
                {"name": "Eddy Kenzo", "genre": "Afrobeat", "era": "2010s-present"},
                {"name": "Sheebah Karungi", "genre": "Dancehall", "era": "2010s-present"},
                {"name": "Daddy Andre", "genre": "R&B/Dancehall", "era": "2010s-present"}
            ],
            "recording_studios": ["Swangz Avenue", "Muthaland", "Dream Studios"],
            "radio_stations": ["Capital FM", "Sanyu FM", "Radio One"],
            "tv_stations": ["NBS TV", "NTV Uganda", "Bukedde TV"]
        },
        "western": {
            "name": "Western Region",
            "capital": "Mbarara",
            "districts": ["Mbarara", "Kasese", "Ntungamo", "Kabale", "Fort Portal", "Hoima", "Kibaale", "Kyenjojo", "Bushenyi", "Ibanda"],
            "dialects": ["Runyankole", "Rukiga", "Rutooro", "Lhukonzo"],
            "musical_styles": ["Runyankole Folk", "Ekitaguriro", "Modern Afro-fusion"],
            "iconic_venues": ["Lake Bunyonyi", "Queen Elizabeth National Park"],
            "famous_musicians": [
                {"name": "Rema Namakula", "genre": "Afrobeat", "era": "2010s-present"},
                {"name": "Mickie Wine", "genre": "Dancehall", "era": "2010s-present"},
                {"name": "Ray G", "genre": "R&B", "era": "2000s-present"},
                {"name": "Mesach Semakula", "genre": "Gospel", "era": "1990s-present"}
            ],
            "recording_studios": ["Satellite Studios", "Dreamland Studios"],
            "radio_stations": ["Voice of Kigezi", "Rhino Radio", "Better FM"],
            "tv_stations": ["Voice of Toro TV", "KTV"]
        },
        "eastern": {
            "name": "Eastern Region",
            "capital": "Jinja",
            "districts": ["Jinja", "Mbale", "Tororo", "Mayuge", "Soroti", "Iganga", "Kamuli", "Bugiri", "Busia", "Pallisa"],
            "dialects": ["Lusoga", "Lugisu", "Ateso", "Kumam"],
            "musical_styles": ["Kadongo Kamu", "Laragga", "Bengila"],
            "iconic_venues": ["Source of the Nile", "Sipi Falls"],
            "famous_musicians": [
                {"name": "Geosteady", "genre": "R&B", "era": "2010s-present"},
                {"name": "Victor Ruz", "genre": "Afrobeat", "era": "2010s-present"},
                {"name": "Judith Babirye", "genre": "Gospel", "era": "2000s-present"},
                {"name": "Moses Matasi", "genre": "Kadongo Kamu", "era": "1980s-2000s"}
            ],
            "recording_studios": ["Little P Studios", "Homeboyz Studios"],
            "radio_stations": ["Radio Wa", "Busoga Radio", "Voice of Tororo"],
            "tv_stations": ["NTV Eastern", "Urban TV"]
        },
        "northern": {
            "name": "Northern Region",
            "capital": "Gulu",
            "districts": ["Gulu", "Lira", "Arua", "Kitgum", "Adjumani", "Nebbi", "Apac", "Oyam", "Kole", "Dokolo"],
            "dialects": ["Acholi", "Lango", "Lugbara", "Alur"],
            "musical_styles": ["Larakaraka", "Bwola", "Otole"],
            "iconic_venues": ["Gulu Independence Park", "Murchison Falls"],
            "famous_musicians": [
                {"name": "Fik Fameica", "genre": "Hip Hop", "era": "2010s-present"},
                {"name": "Bosmic Otim", "genre": "Afrobeat", "era": "2010s-present"},
                {"name": "Eezzy", "genre": "Dancehall", "era": "2010s-present"},
                {"name": "Lillian Mbabazi", "genre": "R&B", "era": "2000s-present"}
            ],
            "recording_studios": ["Gulu Independent Studios", "Lira Sound Studio"],
            "radio_stations": ["Radio Rupiny", "Radio Unity", "Mega FM"],
            "tv_stations": ["NTV Northern", "Arua One TV"]
        }
    }
    
    VALID_REGIONS = set(UGANDAN_REGIONS.keys())
    
    # Chart Configuration
    CHART_CONFIG = {
        "top100_size": 100,
        "regional_top_size": 10,
        "trending_size": 20,
        "minimum_plays_for_chart": 100,
        "minimum_score_for_chart": 30.0,
        "chart_week_duration_days": 7,
        "historical_charts_to_keep": 52  # 1 year
    }
    
    # Rate Limiting
    RATE_LIMITS = {
        "public": "100/minute",
        "ingestion": "50/minute",
        "admin": "30/minute",
        "worker": "200/minute"
    }
    
    @classmethod
    def validate_config(cls):
        """Validate and setup configuration"""
        # Create directories
        directories = ["data", "logs", "backups", "exports"]
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)
        
        # Validate tokens in production
        if cls.ENV == Environment.PRODUCTION:
            required_tokens = ["ADMIN_TOKEN", "INGEST_TOKEN", "INTERNAL_TOKEN"]
            missing = [token for token in required_tokens if not getattr(cls, token)]
            if missing:
                raise ValueError(f"Missing required tokens in production: {', '.join(missing)}")
        
        # Setup SQLite database path
        cls.SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        return cls
    
    @classmethod
    def get_allowed_origins(cls):
        """Get CORS allowed origins"""
        if cls.DEBUG:
            return ["*"]
        return [
            "https://ugboard-engine.onrender.com",
            "https://ugboard.vercel.app",
            "https://ugboard-youtube-puller.kimkp015.workers.dev",
            "http://localhost:3000",
            "http://127.0.0.1:3000"
        ]

config = Config.validate_config()

# ====== LOGGING ======
def setup_structured_logging():
    """Setup comprehensive logging"""
    logger = logging.getLogger("ugboard")
    logger.setLevel(logging.DEBUG if config.DEBUG else logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler('logs/ugboard_engine.log')
    file_format = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s | %(filename)s:%(lineno)d'
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Suppress noisy loggers
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    
    logger.info(f"Logging initialized for {config.ENV.value} environment")
    return logger

logger = setup_structured_logging()

# ====== DATABASE MODELS ======
class ChartWeek(BaseModel):
    """Chart week model"""
    week_id: str = Field(..., description="Chart week ID in format YYYY-WW")
    start_date: datetime
    end_date: datetime
    status: str = Field("active", pattern="^(active|published|archived)$")
    published_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class Song(BaseModel):
    """Song database model"""
    id: Optional[int] = Field(None, description="Database ID")
    external_id: str = Field(..., description="External song ID")
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=100)
    plays: int = Field(0, ge=0)
    score: float = Field(0.0, ge=0.0, le=100.0)
    region: str = Field(..., pattern="^(central|western|eastern|northern)$")
    district: Optional[str] = Field(None, max_length=50)
    source: str = Field(..., max_length=100)
    source_type: str = Field(..., pattern="^(youtube|radio|tv)$")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    chart_week: Optional[str] = Field(None, description="Associated chart week")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class ChartEntry(BaseModel):
    """Chart entry model"""
    chart_week: str
    region: str
    song_id: int
    rank: int
    previous_rank: Optional[int] = None
    change: Optional[str] = Field(None, pattern="^(up|down|new|same)$")
    weeks_on_chart: int = Field(1, ge=1)
    peak_position: int = Field(1, ge=1)

# ====== DATABASE LAYER ======
class DatabaseManager:
    """SQLite database manager with async operations"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None
    
    async def connect(self):
        """Connect to database"""
        self.conn = await aiosqlite.connect(str(self.db_path))
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA foreign_keys=ON")
        await self.conn.execute("PRAGMA busy_timeout=5000")
        await self._init_tables()
    
    async def _init_tables(self):
        """Initialize database tables"""
        # Songs table
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                artist TEXT NOT NULL,
                plays INTEGER DEFAULT 0 CHECK(plays >= 0),
                score REAL DEFAULT 0.0 CHECK(score >= 0.0 AND score <= 100.0),
                region TEXT NOT NULL CHECK(region IN ('central', 'western', 'eastern', 'northern')),
                district TEXT,
                source TEXT NOT NULL,
                source_type TEXT NOT NULL CHECK(source_type IN ('youtube', 'radio', 'tv')),
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                chart_week TEXT,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_region (region),
                INDEX idx_source_type (source_type),
                INDEX idx_chart_week (chart_week),
                INDEX idx_score (score DESC),
                INDEX idx_artist (artist)
            )
        """)
        
        # Chart weeks table
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS chart_weeks (
                week_id TEXT PRIMARY KEY,
                start_date TIMESTAMP NOT NULL,
                end_date TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active' CHECK(status IN ('active', 'published', 'archived')),
                published_at TIMESTAMP,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Chart entries table
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS chart_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chart_week TEXT NOT NULL,
                region TEXT NOT NULL,
                song_id INTEGER NOT NULL,
                rank INTEGER NOT NULL CHECK(rank >= 1),
                previous_rank INTEGER,
                change TEXT CHECK(change IN ('up', 'down', 'new', 'same')),
                weeks_on_chart INTEGER DEFAULT 1 CHECK(weeks_on_chart >= 1),
                peak_position INTEGER DEFAULT 1 CHECK(peak_position >= 1),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chart_week) REFERENCES chart_weeks(week_id) ON DELETE CASCADE,
                FOREIGN KEY (song_id) REFERENCES songs(id) ON DELETE CASCADE,
                FOREIGN KEY (region) REFERENCES songs(region),
                UNIQUE(chart_week, region, song_id),
                INDEX idx_chart_week_region (chart_week, region),
                INDEX idx_rank (rank)
            )
        """)
        
        # Trending songs table (for 8-hour window algorithm)
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trending_songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_id INTEGER NOT NULL,
                window_number INTEGER NOT NULL,
                trending_score REAL NOT NULL,
                rank INTEGER NOT NULL,
                region TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (song_id) REFERENCES songs(id) ON DELETE CASCADE,
                INDEX idx_window_rank (window_number, rank),
                INDEX idx_created (created_at),
                UNIQUE(window_number, song_id)
            )
        """)
        
        # YouTube worker logs
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS youtube_worker_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id TEXT NOT NULL,
                channel_id TEXT,
                video_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                status TEXT DEFAULT 'processing' CHECK(status IN ('processing', 'completed', 'failed')),
                error_message TEXT,
                metadata JSON,
                INDEX idx_worker (worker_id),
                INDEX idx_status (status),
                INDEX idx_created (started_at)
            )
        """)
        
        await self.conn.commit()
    
    async def add_songs(self, songs: List[Song]) -> Dict[str, Any]:
        """Add songs to database with conflict resolution"""
        added = 0
        updated = 0
        skipped = 0
        added_ids = []
        
        for song in songs:
            try:
                # Check for existing song (same title + artist within 7 days)
                cursor = await self.conn.execute("""
                    SELECT id, plays, score FROM songs 
                    WHERE title = ? AND artist = ? 
                    AND ingested_at > datetime('now', '-7 days')
                    LIMIT 1
                """, (song.title, song.artist))
                
                existing = await cursor.fetchone()
                
                if existing:
                    # Update existing song
                    song_id, existing_plays, existing_score = existing
                    
                    # Weighted update: 70% new, 30% old
                    new_plays = int((song.plays * 0.7) + (existing_plays * 0.3))
                    new_score = (song.score * 0.7) + (existing_score * 0.3)
                    
                    await self.conn.execute("""
                        UPDATE songs 
                        SET plays = ?, score = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (new_plays, new_score, song_id))
                    
                    updated += 1
                else:
                    # Insert new song
                    cursor = await self.conn.execute("""
                        INSERT INTO songs (
                            external_id, title, artist, plays, score, 
                            region, district, source, source_type,
                            ingested_at, metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        song.external_id, song.title, song.artist, song.plays, song.score,
                        song.region, song.district, song.source, song.source_type,
                        song.ingested_at.isoformat(), json.dumps(song.metadata)
                    ))
                    
                    added_ids.append(cursor.lastrowid)
                    added += 1
                    
            except Exception as e:
                logger.error(f"Error processing song {song.title}: {e}")
                skipped += 1
        
        await self.conn.commit()
        
        return {
            "added": added,
            "updated": updated,
            "skipped": skipped,
            "total_processed": len(songs),
            "added_ids": added_ids
        }
    
    async def get_top_songs(self, limit: int = 100, region: Optional[str] = None, 
                           chart_week: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs with advanced filtering"""
        query = """
            SELECT s.*, 
                   (SELECT COUNT(*) FROM songs s2 WHERE s2.score > s.score) + 1 as rank
            FROM songs s
            WHERE 1=1
        """
        params = []
        
        if region:
            query += " AND s.region = ?"
            params.append(region)
        
        if chart_week:
            query += " AND s.chart_week = ?"
            params.append(chart_week)
        
        query += " ORDER BY s.score DESC LIMIT ?"
        params.append(limit)
        
        cursor = await self.conn.execute(query, params)
        rows = await cursor.fetchall()
        
        # Convert to dict
        songs = []
        for row in rows:
            song_dict = dict(row)
            # Convert JSON metadata
            if song_dict.get("metadata"):
                song_dict["metadata"] = json.loads(song_dict["metadata"])
            songs.append(song_dict)
        
        return songs
    
    async def get_chart_entries(self, chart_week: str, region: str, 
                               limit: int = 100) -> List[Dict[str, Any]]:
        """Get chart entries for specific week and region"""
        cursor = await self.conn.execute("""
            SELECT ce.*, s.title, s.artist, s.plays, s.score, s.source_type
            FROM chart_entries ce
            JOIN songs s ON ce.song_id = s.id
            WHERE ce.chart_week = ? AND ce.region = ?
            ORDER BY ce.rank
            LIMIT ?
        """, (chart_week, region, limit))
        
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    async def publish_chart_week(self, week_id: str, 
                                region_songs: Dict[str, List[Dict[str, Any]]]) -> bool:
        """Publish chart week with region-specific rankings"""
        try:
            await self.conn.execute("BEGIN")
            
            # Update chart week status
            await self.conn.execute("""
                UPDATE chart_weeks 
                SET status = 'published', published_at = CURRENT_TIMESTAMP
                WHERE week_id = ?
            """, (week_id,))
            
            # Clear existing entries for this week
            await self.conn.execute("""
                DELETE FROM chart_entries WHERE chart_week = ?
            """, (week_id,))
            
            # Insert new chart entries
            for region, songs in region_songs.items():
                for rank, song in enumerate(songs, 1):
                    # Get previous rank
                    cursor = await self.conn.execute("""
                        SELECT rank FROM chart_entries 
                        WHERE song_id = ? AND region = ?
                        ORDER BY created_at DESC LIMIT 1
                    """, (song["id"], region))
                    
                    prev_row = await cursor.fetchone()
                    previous_rank = prev_row[0] if prev_row else None
                    
                    # Determine change
                    change = None
                    if previous_rank:
                        if rank < previous_rank:
                            change = "up"
                        elif rank > previous_rank:
                            change = "down"
                        else:
                            change = "same"
                    else:
                        change = "new"
                    
                    # Get weeks on chart and peak position
                    cursor = await self.conn.execute("""
                        SELECT COUNT(DISTINCT chart_week) as weeks_count,
                               MIN(rank) as best_rank
                        FROM chart_entries 
                        WHERE song_id = ? AND region = ?
                    """, (song["id"], region))
                    
                    stats_row = await cursor.fetchone()
                    weeks_on_chart = (stats_row[0] if stats_row[0] else 0) + 1
                    peak_position = min(rank, stats_row[1] if stats_row[1] else rank)
                    
                    # Insert chart entry
                    await self.conn.execute("""
                        INSERT INTO chart_entries (
                            chart_week, region, song_id, rank, 
                            previous_rank, change, weeks_on_chart, peak_position
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        week_id, region, song["id"], rank,
                        previous_rank, change, weeks_on_chart, peak_position
                    ))
            
            await self.conn.commit()
            return True
            
        except Exception as e:
            await self.conn.execute("ROLLBACK")
            logger.error(f"Error publishing chart week {week_id}: {e}")
            return False
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()

# ====== REDIS CACHE ======
class RedisCache:
    """Redis cache manager with connection pooling"""
    
    def __init__(self, redis_url: str, prefix: str = "ugboard:"):
        self.redis_url = redis_url
        self.prefix = prefix
        self.redis = None
    
    async def connect(self):
        """Connect to Redis"""
        self.redis = await redis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        # Test connection
        await self.redis.ping()
        logger.info(f"Connected to Redis at {self.redis_url}")
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            value = await self.redis.get(f"{self.prefix}{key}")
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Set value in cache with TTL"""
        try:
            await self.redis.setex(
                f"{self.prefix}{key}",
                ttl,
                json.dumps(value, default=str)
            )
            return True
        except Exception as e:
            logger.error(f"Redis set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            return bool(await self.redis.delete(f"{self.prefix}{key}"))
        except Exception as e:
            logger.error(f"Redis delete error for key {key}: {e}")
            return False
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate keys matching pattern"""
        try:
            keys = await self.redis.keys(f"{self.prefix}{pattern}")
            if keys:
                return await self.redis.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Redis pattern invalidation error: {e}")
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics"""
        try:
            info = await self.redis.info()
            return {
                "connected": True,
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": (
                    info.get("keyspace_hits", 0) / 
                    max(1, info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0))
                )
            }
        except Exception as e:
            logger.error(f"Redis stats error: {e}")
            return {"connected": False, "error": str(e)}
    
    async def close(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()

# ====== YOUTUBE WORKER INTEGRATION ======
class YouTubeWorkerClient:
    """Client for YouTube Worker integration"""
    
    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.client = None
    
    async def connect(self):
        """Create HTTP client"""
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.auth_token}",
                "User-Agent": "UG-Board-Engine/9.0.0",
                "Content-Type": "application/json"
            },
            timeout=30.0
        )
    
    async def trigger_pull(self, channels: Optional[List[str]] = None) -> Dict[str, Any]:
        """Trigger YouTube worker to pull data"""
        try:
            payload = {
                "channels": channels or ["all"],
                "triggered_at": datetime.utcnow().isoformat(),
                "max_videos": 50,
                "priority": "high"
            }
            
            response = await self.client.post("/pull", json=payload)
            response.raise_for_status()
            
            return response.json()
            
        except httpx.RequestError as e:
            logger.error(f"YouTube worker request error: {e}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="YouTube worker is temporarily unavailable"
            )
        except httpx.HTTPStatusError as e:
            logger.error(f"YouTube worker HTTP error: {e.response.status_code} - {e.response.text}")
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"YouTube worker error: {e.response.text}"
            )
    
    async def get_status(self) -> Dict[str, Any]:
        """Get YouTube worker status"""
        try:
            response = await self.client.get("/status")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"YouTube worker status error: {e}")
            return {"status": "unavailable", "error": str(e)}
    
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()

# ====== CHART RULES ENGINE ======
class ChartRulesEngine:
    """Business rules engine for Ugandan music charts"""
    
    @staticmethod
    def validate_song_for_chart(song: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate if song meets chart requirements"""
        errors = []
        
        # 1. Ugandan artist check
        ugandan_artists = {
            "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
            "gravity", "vyroota", "geosteady", "feffe busi",
            "alien skin", "azawi", "vinka", "rema", "cindy"
        }
        
        artist_lower = song.get("artist", "").lower()
        is_ugandan = any(ug_artist in artist_lower for ug_artist in ugandan_artists)
        
        if not is_ugandan:
            # Check for Ugandan name patterns
            ugandan_patterns = [" omutujju", " omulangira", " ssemakula", " nantale"]
            if not any(pattern in artist_lower for pattern in ugandan_patterns):
                errors.append("Artist is not recognized as Ugandan")
        
        # 2. Minimum plays threshold
        if song.get("plays", 0) < config.CHART_CONFIG["minimum_plays_for_chart"]:
            errors.append(f"Plays ({song.get('plays')}) below minimum ({config.CHART_CONFIG['minimum_plays_for_chart']})")
        
        # 3. Minimum score threshold
        if song.get("score", 0) < config.CHART_CONFIG["minimum_score_for_chart"]:
            errors.append(f"Score ({song.get('score')}) below minimum ({config.CHART_CONFIG['minimum_score_for_chart']})")
        
        # 4. Valid region
        if song.get("region") not in config.VALID_REGIONS:
            errors.append(f"Invalid region: {song.get('region')}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def apply_regional_quotas(songs: List[Dict[str, Any]], 
                             region: str, 
                             total_slots: int = 100) -> List[Dict[str, Any]]:
        """Apply regional quotas to chart"""
        quota_percentage = config.UGANDAN_MUSIC_RULES["regional_quotas"].get(region, 0)
        regional_slots = int(total_slots * (quota_percentage / 100))
        
        # Filter songs by region
        regional_songs = [s for s in songs if s.get("region") == region]
        
        # Apply regional slots
        return regional_songs[:regional_slots]
    
    @staticmethod
    def calculate_chart_week() -> Tuple[str, datetime, datetime]:
        """Calculate current chart week based on configuration"""
        now = datetime.utcnow()
        
        # Determine start day (0=Monday, 6=Sunday)
        start_day_map = {
            "MONDAY": 0,
            "TUESDAY": 1,
            "WEDNESDAY": 2,
            "THURSDAY": 3,
            "FRIDAY": 4,
            "SATURDAY": 5,
            "SUNDAY": 6
        }
        
        start_day = start_day_map.get(config.CHART_WEEK_START.upper(), 0)
        
        # Calculate start of week
        current_weekday = now.weekday()
        days_since_start = (current_weekday - start_day) % 7
        week_start = now - timedelta(days=days_since_start)
@staticmethod
    def calculate_chart_week() -> Tuple[str, datetime, datetime]:
        """Calculate current chart week based on configuration"""
        now = datetime.utcnow()
        
        # Determine start day (0=Monday, 6=Sunday)
        start_day_map = {
            "MONDAY": 0,
            "TUESDAY": 1,
            "WEDNESDAY": 2,
            "THURSDAY": 3,
            "FRIDAY": 4,
            "SATURDAY": 5,
            "SUNDAY": 6
        }
        
        start_day = start_day_map.get(config.CHART_WEEK_START.upper(), 0)
        
        # Calculate start of week
        current_weekday = now.weekday()
        days_since_start = (current_weekday - start_day) % 7
        week_start = now - timedelta(days=days_since_start)
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate end of week
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        
        # Format week ID: YYYY-WW
        year = week_start.year
        week_number = int(week_start.strftime("%W"))
        week_id = f"{year}-W{week_number:02d}"
        
        return week_id, week_start, week_end
    
    @staticmethod
    async def generate_chart_rankings(songs: List[Dict[str, Any]], 
                                      region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Generate chart rankings with advanced scoring algorithm"""
        if not songs:
            return []
        
        # Enhanced scoring algorithm
        def calculate_ranking_score(song: Dict[str, Any]) -> float:
            base_score = song.get("score", 0) * 0.5  # 50% base score
            play_score = min(song.get("plays", 0) / 1000, 10) * 0.3  # 30% plays (max 10 points)
            
            # Recency bonus (last 24 hours get 20% boost)
            ingested_at = song.get("ingested_at")
            recency_bonus = 0
            if ingested_at:
                try:
                    ingest_time = datetime.fromisoformat(ingested_at.replace('Z', '+00:00'))
                    hours_old = (datetime.utcnow() - ingest_time).total_seconds() / 3600
                    if hours_old < 24:
                        recency_bonus = 0.2 * (1 - (hours_old / 24))
                except:
                    pass
            
            # Source type weighting
            source_type = song.get("source_type", "")
            source_multiplier = {
                "youtube": 1.0,
                "radio": 0.9,
                "tv": 0.8
            }.get(source_type, 0.5)
            
            total_score = (base_score + play_score) * (1 + recency_bonus) * source_multiplier
            
            # Cap at 100
            return min(total_score, 100)
        
        # Calculate scores for all songs
        scored_songs = []
        for song in songs:
            song_copy = song.copy()
            song_copy["ranking_score"] = calculate_ranking_score(song)
            scored_songs.append(song_copy)
        
        # Sort by ranking score
        ranked_songs = sorted(scored_songs, key=lambda x: x["ranking_score"], reverse=True)
        
        # Apply regional quotas if specified
        if region:
            regional_quota = config.UGANDAN_MUSIC_RULES["regional_quotas"].get(region, 0)
            max_regional_songs = int(config.CHART_CONFIG["top100_size"] * (regional_quota / 100))
            ranked_songs = ranked_songs[:max_regional_songs]
        
        # Add ranks
        for i, song in enumerate(ranked_songs, 1):
            song["rank"] = i
        
        return ranked_songs

# ====== TRENDING ENGINE ======
class TrendingEngine:
    """8-hour trending window engine"""
    
    @staticmethod
    def get_current_window_info() -> Dict[str, Any]:
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
            "current_utc_hour": datetime.utcnow().hour,
            "description": f"{config.TRENDING_WINDOW_HOURS}-hour window {window_start_hour:02d}:00-{window_end_hour:02d}:00 UTC",
            "next_window_in": f"{int(seconds_remaining // 3600)}h {int((seconds_remaining % 3600) // 60)}m"
        }
    
    @staticmethod
    async def calculate_trending_songs(songs: List[Dict[str, Any]], 
                                      limit: int = 20,
                                      db_manager: Optional[DatabaseManager] = None) -> List[Dict[str, Any]]:
        """Calculate trending songs using 8-hour window algorithm"""
        if not songs:
            return []
        
        window_info = TrendingEngine.get_current_window_info()
        window_number = window_info["window_number"]
        
        # Filter for recent songs (last 72 hours)
        recent_songs = []
        for song in songs:
            ingested_at = song.get("ingested_at")
            if ingested_at:
                try:
                    ingest_time = datetime.fromisoformat(ingested_at.replace('Z', '+00:00'))
                    if datetime.utcnow() - ingest_time < timedelta(hours=72):
                        recent_songs.append(song)
                except:
                    continue
        
        if not recent_songs:
            return []
        
        # Calculate trending score
        def trending_score(song: Dict[str, Any]) -> float:
            base_score = song.get("score", 0) * 0.6
            velocity_score = min(song.get("plays", 0) / 500, 15) * 0.4
            
            # Recency bonus (exponential decay)
            ingested_at = song.get("ingested_at")
            if ingested_at:
                try:
                    ingest_time = datetime.fromisoformat(ingested_at.replace('Z', '+00:00'))
                    hours_old = (datetime.utcnow() - ingest_time).total_seconds() / 3600
                    recency_bonus = 10 * (0.5 ** (hours_old / 24))  # Halves every 24 hours
                except:
                    recency_bonus = 0
            else:
                recency_bonus = 0
            
            return base_score + velocity_score + recency_bonus
        
        # Add trending scores
        for song in recent_songs:
            song["trending_score"] = trending_score(song)
        
        # Deterministic shuffle based on window number
        def shuffle_key(song: Dict[str, Any]) -> int:
            return hash(f"{window_number}_{song.get('id', '')}") % 1000
        
        # Sort by shuffle key first, then trending score
        sorted_songs = sorted(recent_songs, key=lambda x: (shuffle_key(x), x["trending_score"]), reverse=True)
        
        # Store trending data in database if db_manager provided
        if db_manager and db_manager.conn:
            try:
                for i, song in enumerate(sorted_songs[:limit], 1):
                    await db_manager.conn.execute("""
                        INSERT OR REPLACE INTO trending_songs 
                        (song_id, window_number, trending_score, rank, region, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        song.get("id"),
                        window_number,
                        song["trending_score"],
                        i,
                        song.get("region", "central"),
                        datetime.utcnow().isoformat()
                    ))
                await db_manager.conn.commit()
            except Exception as e:
                logger.error(f"Error storing trending data: {e}")
        
        return sorted_songs[:limit]
    
    @staticmethod
    async def get_current_trending_song(songs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get single trending song for current window"""
        trending_songs = await TrendingEngine.calculate_trending_songs(songs, limit=1)
        return trending_songs[0] if trending_songs else None

# ====== AUTHENTICATION SERVICE ======
class AuthService:
    """Comprehensive authentication and authorization service"""
    
    # Rate limiting storage
    _failed_attempts: Dict[str, List[float]] = {}
    _MAX_ATTEMPTS = 10
    _WINDOW_SECONDS = 300
    
    @staticmethod
    def _check_rate_limit(identifier: str) -> bool:
        """Check rate limiting"""
        now = time.time()
        
        if identifier in AuthService._failed_attempts:
            AuthService._failed_attempts[identifier] = [
                attempt for attempt in AuthService._failed_attempts[identifier]
                if now - attempt < AuthService._WINDOW_SECONDS
            ]
        
        if len(AuthService._failed_attempts.get(identifier, [])) >= AuthService._MAX_ATTEMPTS:
            return False
        
        return True
    
    @staticmethod
    def _record_failed_attempt(identifier: str):
        """Record failed authentication attempt"""
        now = time.time()
        if identifier not in AuthService._failed_attempts:
            AuthService._failed_attempts[identifier] = []
        AuthService._failed_attempts[identifier].append(now)
    
    @classmethod
    async def verify_admin(
        cls,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
        request: Request = None
    ) -> bool:
        """Verify admin token with rate limiting"""
        client_ip = request.client.host if request else "unknown"
        identifier = f"admin_{client_ip}"
        
        if not cls._check_rate_limit(identifier):
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
            cls._record_failed_attempt(identifier)
            logger.warning(f"Failed admin authentication attempt from {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing administration token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return True
    
    @classmethod
    async def verify_ingest(
        cls,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
        request: Request = None
    ) -> bool:
        """Verify ingestion token"""
        client_ip = request.client.host if request else "unknown"
        identifier = f"ingest_{client_ip}"
        
        if not cls._check_rate_limit(identifier):
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
            cls._record_failed_attempt(identifier)
            logger.warning(f"Failed ingest authentication attempt from {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing ingestion token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return True
    
    @classmethod
    async def verify_youtube(
        cls,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
        request: Request = None
    ) -> bool:
        """Verify YouTube ingestion token"""
        client_ip = request.client.host if request else "unknown"
        identifier = f"youtube_{client_ip}"
        
        if not cls._check_rate_limit(identifier):
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
            cls._record_failed_attempt(identifier)
            logger.warning(f"Failed YouTube authentication attempt from {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing YouTube ingestion token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return True
    
    @classmethod
    async def verify_internal(
        cls,
        x_internal_token: Optional[str] = Header(None, alias="X-Internal-Token")
    ) -> bool:
        """Verify internal service token"""
        if not config.INTERNAL_TOKEN:
            logger.error("Internal token not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal authentication not configured"
            )
        
        if not x_internal_token or x_internal_token != config.INTERNAL_TOKEN:
            logger.warning("Failed internal authentication attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing internal token"
            )
        
        return True

# ====== APPLICATION STATE ======
class ApplicationState:
    """Global application state manager"""
    
    def __init__(self):
        self.db_manager = DatabaseManager(config.SQLITE_PATH)
        self.redis_cache = RedisCache(config.REDIS_URL, config.REDIS_PREFIX)
        self.youtube_worker = YouTubeWorkerClient(
            config.YOUTUBE_WORKER_URL,
            config.YOUTUBE_WORKER_TOKEN
        )
        self.startup_time = datetime.utcnow()
        self.request_count = 0
        self.current_chart_week = ChartRulesEngine.calculate_chart_week()[0]
        self.active_connections = set()
        self.background_tasks = set()
    
    async def initialize(self):
        """Initialize all connections"""
        logger.info("Initializing application state...")
        
        # Connect to database
        await self.db_manager.connect()
        logger.info(f"Connected to database: {config.SQLITE_PATH}")
        
        # Connect to Redis
        try:
            await self.redis_cache.connect()
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Continuing without cache.")
            self.redis_cache = None
        
        # Connect to YouTube worker
        try:
            await self.youtube_worker.connect()
            status = await self.youtube_worker.get_status()
            logger.info(f"YouTube worker status: {status.get('status', 'unknown')}")
        except Exception as e:
            logger.warning(f"YouTube worker connection failed: {e}")
            self.youtube_worker = None
        
        # Initialize current chart week
        week_id, week_start, week_end = ChartRulesEngine.calculate_chart_week()
        self.current_chart_week = week_id
        
        # Ensure chart week exists in database
        try:
            cursor = await self.db_manager.conn.execute(
                "SELECT week_id FROM chart_weeks WHERE week_id = ?",
                (week_id,)
            )
            if not await cursor.fetchone():
                await self.db_manager.conn.execute("""
                    INSERT INTO chart_weeks (week_id, start_date, end_date, status)
                    VALUES (?, ?, ?, 'active')
                """, (week_id, week_start.isoformat(), week_end.isoformat()))
                await self.db_manager.conn.commit()
                logger.info(f"Created new chart week: {week_id}")
        except Exception as e:
            logger.error(f"Error initializing chart week: {e}")
        
        logger.info(f"Application state initialized. Chart week: {self.current_chart_week}")
    
    async def shutdown(self):
        """Shutdown all connections"""
        logger.info("Shutting down application state...")
        
        # Close database
        await self.db_manager.close()
        
        # Close Redis
        if self.redis_cache:
            await self.redis_cache.close()
        
        # Close YouTube worker
        if self.youtube_worker:
            await self.youtube_worker.close()
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        logger.info("Application state shutdown complete")

# ====== PYDANTIC MODELS FOR API ======
class SongCreate(BaseModel):
    """Song creation model"""
    title: str = Field(..., min_length=1, max_length=200)
    artist: str = Field(..., min_length=1, max_length=100)
    plays: int = Field(0, ge=0)
    score: float = Field(0.0, ge=0.0, le=100.0)
    region: str = Field("central", pattern="^(central|western|eastern|northern)$")
    district: Optional[str] = Field(None, max_length=50)
    source: str = Field(..., max_length=100)
    source_type: str = Field(..., pattern="^(youtube|radio|tv)$")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('title', 'artist')
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()
    
    @field_validator('district')
    @classmethod
    def validate_district(cls, v: Optional[str], info) -> Optional[str]:
        if v:
            region = info.data.get('region')
            valid_districts = config.UGANDAN_REGIONS.get(region, {}).get("districts", [])
            if v not in valid_districts:
                raise ValueError(f"District '{v}' is not in {region.capitalize()} Region")
        return v

class IngestRequest(BaseModel):
    """Ingestion request model"""
    songs: List[SongCreate] = Field(..., min_items=1, max_items=1000)
    source: str = Field(..., min_length=1, max_length=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('songs')
    def validate_song_count(cls, v):
        if len(v) > 1000:
            raise ValueError("Maximum 1000 songs per ingestion")
        return v

class YouTubeIngestRequest(IngestRequest):
    """YouTube ingestion request model"""
    channel_id: Optional[str] = Field(None, max_length=50)
    video_id: Optional[str] = Field(None, max_length=20)
    category: str = Field("music", max_length=50)

class ChartResponse(BaseModel):
    """Chart response model"""
    chart: str
    week: str
    entries: List[Dict[str, Any]]
    count: int
    region: Optional[str] = None
    timestamp: datetime
    
    model_config = ConfigDict(json_encoders={datetime: lambda dt: dt.isoformat()})

class TrendingResponse(BaseModel):
    """Trending response model"""
    chart: str
    entries: List[Dict[str, Any]]
    count: int
    trending_window: Dict[str, Any]
    timestamp: datetime
    
    model_config = ConfigDict(json_encoders={datetime: lambda dt: dt.isoformat()})

# ====== APPLICATION LIFECYCLE ======
app_state = ApplicationState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    # Startup
    try:
        await app_state.initialize()
        
        # Startup banner
        banner = f"""
        ================================================
        UG Board Engine v9.0.0 - Production System
        ================================================
        Environment: {config.ENV.value}
        Database: {config.SQLITE_PATH}
        Redis Cache: {'Connected' if app_state.redis_cache else 'Disabled'}
        YouTube Worker: {'Connected' if app_state.youtube_worker else 'Disabled'}
        Chart Week: {app_state.current_chart_week}
        Ugandan Regions: {', '.join(sorted(config.VALID_REGIONS))}
        ================================================
        Server: http://{config.HOST}:{config.PORT}
        Docs: http://{config.HOST}:{config.PORT}/docs
        Health: http://{config.HOST}:{config.PORT}/health
        ================================================
        """
        
        print(banner)
        logger.info(banner)
        
        # Start background tasks
        if config.ENV == Environment.PRODUCTION:
            # Periodic health check
            async def health_monitor():
                while True:
                    await asyncio.sleep(300)  # Every 5 minutes
                    try:
                        # Check database
                        cursor = await app_state.db_manager.conn.execute("SELECT COUNT(*) FROM songs")
                        song_count = (await cursor.fetchone())[0]
                        
                        # Check Redis
                        redis_stats = {}
                        if app_state.redis_cache:
                            redis_stats = await app_state.redis_cache.get_stats()
                        
                        logger.info(
                            f"Health check - Songs: {song_count}, "
                            f"Redis: {redis_stats.get('connected', False)}, "
                            f"Requests: {app_state.request_count}"
                        )
                    except Exception as e:
                        logger.error(f"Health monitor error: {e}")
            
            task = asyncio.create_task(health_monitor())
            app_state.background_tasks.add(task)
            task.add_done_callback(app_state.background_tasks.discard)
        
        yield
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    finally:
        # Shutdown
        await app_state.shutdown()

# ====== FASTAPI APPLICATION ======
app = FastAPI(
    title="UG Board Engine",
    version="9.0.0",
    description="""
    ## Complete Ugandan Music Chart System
    
    ### Features:
    • Real-time Ugandan music charts for 4 regions (Central, Western, Eastern, Northern)
    • 8-hour trending window algorithm
    • SQLite database with Redis caching
    • YouTube worker integration for automated content ingestion
    • Comprehensive authentication and rate limiting
    • Chart rules engine with regional quotas
    
    ### Environments:
    • **Production**: Full features with security
    • **Development**: Debug features enabled
    • **Testing**: Test mode with mock data
    
    ### Authentication:
    All endpoints except health and root require Bearer tokens:
    - `admin`: Administrative functions
    - `ingest`: Data ingestion endpoints
    - `youtube`: YouTube-specific ingestion
    - `internal`: Internal service communication
    
    ### Chart Rules:
    • Minimum 70% Ugandan content
    • Regional quotas: Central 40%, Western 25%, Eastern 20%, Northern 15%
    • Minimum 100 plays for chart eligibility
    • 8-hour trending rotation windows
    """,
    contact={
        "name": "UG Board Engineering Team",
        "url": "https://github.com/kimkp015-wq/ugboard_engine",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    docs_url="/docs" if config.DEBUG else None,
    redoc_url="/redoc" if config.DEBUG else None,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Root", "description": "Service information and health checks"},
        {"name": "Charts", "description": "Music chart endpoints"},
        {"name": "Regions", "description": "Ugandan regional data and charts"},
        {"name": "Trending", "description": "Trending songs with 8-hour rotation"},
        {"name": "Ingestion", "description": "Data ingestion endpoints"},
        {"name": "YouTube", "description": "YouTube-specific endpoints and worker integration"},
        {"name": "Admin", "description": "Administrative functions"},
        {"name": "System", "description": "System monitoring and diagnostics"},
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["X-Process-Time", "X-Request-ID", "X-Chart-Week"]
)

app.add_middleware(
    GZipMiddleware,
    minimum_size=1000
)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    app_state.request_count += 1
    request_id = hashlib.md5(f"{time.time()}{request.client.host}".encode()).hexdigest()[:8]
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.3f}"
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Chart-Week"] = app_state.current_chart_week
    
    # Log request
    logger.info(
        f"Request {request_id}: {request.method} {request.url.path} "
        f"{response.status_code} - {process_time:.3f}s"
    )
    
    return response

# ====== DEPENDENCY INJECTION ======
def get_db_manager() -> DatabaseManager:
    """Database manager dependency"""
    return app_state.db_manager

def get_redis_cache() -> Optional[RedisCache]:
    """Redis cache dependency"""
    return app_state.redis_cache

def get_youtube_worker() -> Optional[YouTubeWorkerClient]:
    """YouTube worker dependency"""
    return app_state.youtube_worker

# ====== API ENDPOINTS ======
@app.get("/", tags=["Root"], summary="Service information")
async def root():
    """Root endpoint with comprehensive service information"""
    trending_window = TrendingEngine.get_current_window_info()
    redis_stats = await app_state.redis_cache.get_stats() if app_state.redis_cache else {"connected": False}
    
    # Get basic stats
    cursor = await app_state.db_manager.conn.execute("SELECT COUNT(*) FROM songs")
    song_count = (await cursor.fetchone())[0]
    
    cursor = await app_state.db_manager.conn.execute("SELECT COUNT(DISTINCT artist) FROM songs")
    artist_count = (await cursor.fetchone())[0]
    
    return {
        "service": "UG Board Engine",
        "version": "9.0.0",
        "status": "online",
        "environment": config.ENV.value,
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": str(datetime.utcnow() - app_state.startup_time).split('.')[0],
        "chart_week": app_state.current_chart_week,
        "trending_window": trending_window,
        "statistics": {
            "songs": song_count,
            "artists": artist_count,
            "requests": app_state.request_count
        },
        "connections": {
            "database": "connected",
            "redis": redis_stats.get("connected", False),
            "youtube_worker": app_state.youtube_worker is not None
        },
        "ugandan_regions": {
            "available": list(config.VALID_REGIONS),
            "count": len(config.VALID_REGIONS)
        },
        "documentation": {
            "openapi": "/docs",
            "redoc": "/redoc",
            "schema": "/openapi.json"
        }
    }

@app.get("/health", tags=["Root"], summary="Health check")
async def health():
    """Comprehensive health check endpoint"""
    # Check database
    try:
        cursor = await app_state.db_manager.conn.execute("SELECT 1")
        await cursor.fetchone()
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    # Check Redis
    redis_status = "not_configured"
    if app_state.redis_cache:
        try:
            stats = await app_state.redis_cache.get_stats()
            redis_status = "healthy" if stats.get("connected") else "unhealthy"
        except:
            redis_status = "unhealthy"
    
    # Check YouTube worker
    youtube_status = "not_configured"
    if app_state.youtube_worker:
        try:
            status = await app_state.youtube_worker.get_status()
            youtube_status = status.get("status", "unknown")
        except:
            youtube_status = "unhealthy"
    
    # Calculate uptime
    uptime = datetime.utcnow() - app_state.startup_time
    
    return {
        "status": "overall_healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "database": db_status,
            "redis_cache": redis_status,
            "youtube_worker": youtube_status,
            "api": "healthy"
        },
        "system": {
            "uptime_seconds": int(uptime.total_seconds()),
            "uptime_human": str(uptime).split('.')[0],
            "python_version": sys.version,
            "memory_usage": "N/A",  # Could add psutil
            "chart_week": app_state.current_chart_week
        },
        "requests": {
            "total": app_state.request_count,
            "active_connections": len(app_state.active_connections)
        }
    }

# ====== CHART ENDPOINTS ======
@app.get("/charts/top100", tags=["Charts"], summary="Uganda Top 100 chart")
async def get_top100(
    limit: int = Query(100, ge=1, le=200, description="Number of songs to return"),
    region: Optional[str] = Query(None, description="Filter by region"),
    cache: bool = Query(True, description="Use Redis cache"),
    db_manager: DatabaseManager = Depends(get_db_manager),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """
    Get Uganda Top 100 chart for current week.
    
    Can be filtered by region and optionally cached.
    """
    cache_key = f"top100:{limit}:{region}:{app_state.current_chart_week}"
    
    # Try cache first
    if cache and redis_cache:
        cached = await redis_cache.get(cache_key)
        if cached is not None:
            return ChartResponse(
                chart=f"Uganda Top 100 - {region.capitalize() if region else 'All Regions'}",
                week=app_state.current_chart_week,
                entries=cached,
                count=len(cached),
                region=region,
                timestamp=datetime.utcnow()
            )
    
    # Get from database
    songs = await db_manager.get_top_songs(limit, region, app_state.current_chart_week)
    
    # Apply chart rules
    ranked_songs = await ChartRulesEngine.generate_chart_rankings(songs, region)
    
    # Cache result
    if cache and redis_cache and ranked_songs:
        await redis_cache.set(cache_key, ranked_songs, ttl=60)  # 1 minute cache
    
    return ChartResponse(
        chart=f"Uganda Top 100 - {region.capitalize() if region else 'All Regions'}",
        week=app_state.current_chart_week,
        entries=ranked_songs,
        count=len(ranked_songs),
        region=region,
        timestamp=datetime.utcnow()
    )

@app.get("/charts/regions", tags=["Charts", "Regions"], summary="All region statistics")
async def get_all_regions(
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get comprehensive statistics for all Ugandan regions"""
    region_stats = {}
    
    for region_code in config.VALID_REGIONS:
        # Get top songs for region
        songs = await db_manager.get_top_songs(10, region_code, app_state.current_chart_week)
        
        # Calculate statistics
        total_plays = sum(s.get("plays", 0) for s in songs)
        avg_score = sum(s.get("score", 0) for s in songs) / len(songs) if songs else 0
        top_song = max(songs, key=lambda x: x.get("score", 0)) if songs else None
        
        # Artist diversity
        artists = [s.get("artist") for s in songs]
        unique_artists = len(set(artists))
        
        region_stats[region_code] = {
            "name": config.UGANDAN_REGIONS[region_code]["name"],
            "capital": config.UGANDAN_REGIONS[region_code]["capital"],
            "statistics": {
                "total_songs": len(songs),
                "total_plays": total_plays,
                "average_score": round(avg_score, 2),
                "unique_artists": unique_artists,
                "artist_diversity": round(unique_artists / len(songs), 2) if songs else 0
            },
            "top_song": top_song,
            "cultural_info": {
                "dialects": config.UGANDAN_REGIONS[region_code]["dialects"],
                "musical_styles": config.UGANDAN_REGIONS[region_code]["musical_styles"],
                "iconic_venues": config.UGANDAN_REGIONS[region_code]["iconic_venues"]
            }
        }
    
    return {
        "regions": region_stats,
        "count": len(region_stats),
        "chart_week": app_state.current_chart_week,
        "timestamp": datetime.utcnow().isoformat(),
        "regional_quotas": config.UGANDAN_MUSIC_RULES["regional_quotas"]
    }

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"], summary="Region detail")
async def get_region_detail(
    region: str = FPath(..., description="Ugandan region: central, western, eastern, northern"),
    limit: int = Query(10, ge=1, le=50, description="Number of songs to return"),
    include_stats: bool = Query(True, description="Include regional statistics"),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get detailed information and top songs for a specific region"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    # Get songs for region
    songs = await db_manager.get_top_songs(limit, region, app_state.current_chart_week)
    ranked_songs = await ChartRulesEngine.generate_chart_rankings(songs, region)
    
    # Get region data
    region_data = config.UGANDAN_REGIONS[region]
    
    response_data = {
        "region": region,
        "region_name": region_data["name"],
        "capital": region_data["capital"],
        "chart_week": app_state.current_chart_week,
        "songs": ranked_songs,
        "count": len(ranked_songs),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if include_stats:
        # Get chart entries for this region
        chart_entries = await db_manager.get_chart_entries(app_state.current_chart_week, region, 20)
        
        # Calculate statistics
        total_plays = sum(s.get("plays", 0) for s in songs)
        avg_score = sum(s.get("score", 0) for s in songs) / len(songs) if songs else 0
        
        response_data["statistics"] = {
            "total_songs_in_region": len(songs),
            "total_plays": total_plays,
            "average_score": round(avg_score, 2),
            "current_chart_entries": len(chart_entries),
            "regional_quota_percentage": config.UGANDAN_MUSIC_RULES["regional_quotas"].get(region, 0)
        }
        
        response_data["cultural_info"] = {
            "districts": region_data["districts"],
            "dialects": region_data["dialects"],
            "musical_styles": region_data["musical_styles"],
            "iconic_venues": region_data["iconic_venues"],
            "famous_musicians": region_data["famous_musicians"],
            "recording_studios": region_data["recording_studios"],
            "media_outlets": {
                "radio_stations": region_data["radio_stations"],
                "tv_stations": region_data["tv_stations"]
            }
        }
    
    return response_data

# ====== TRENDING ENDPOINTS ======
@app.get("/charts/trending", tags=["Charts", "Trending"], summary="Trending songs")
async def get_trending(
    limit: int = Query(20, ge=1, le=50, description="Number of trending songs"),
    cache: bool = Query(True, description="Use Redis cache"),
    db_manager: DatabaseManager = Depends(get_db_manager),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """
    Get currently trending songs using 8-hour window rotation algorithm.
    
    Trending songs are calculated based on a deterministic algorithm that
    changes every 8 hours, ensuring fair rotation of content.
    """
    cache_key = f"trending:{limit}:{TrendingEngine.get_current_window_info()['window_number']}"
    
    # Try cache first
    if cache and redis_cache:
        cached = await redis_cache.get(cache_key)
        if cached is not None:
            window_info = TrendingEngine.get_current_window_info()
            return TrendingResponse(
                chart="Trending Now - Uganda",
                entries=cached,
                count=len(cached),
                trending_window=window_info,
                timestamp=datetime.utcnow()
            )
    
    # Get recent songs (last 72 hours)
    cursor = await db_manager.conn.execute("""
        SELECT * FROM songs 
        WHERE ingested_at > datetime('now', '-3 days')
        AND source_type = 'youtube'
        ORDER BY ingested_at DESC
        LIMIT 1000
    """)
    
    rows = await cursor.fetchall()
    recent_songs = [dict(row) for row in rows]
    
    # Calculate trending songs
    trending_songs = await TrendingEngine.calculate_trending_songs(recent_songs, limit, db_manager)
    
    # Cache result (shorter TTL for trending)
    if cache and redis_cache and trending_songs:
        await redis_cache.set(cache_key, trending_songs, ttl=30)  # 30 seconds cache
    
    window_info = TrendingEngine.get_current_window_info()
    
    return TrendingResponse(
        chart="Trending Now - Uganda",
        entries=trending_songs,
        count=len(trending_songs),
        trending_window=window_info,
        timestamp=datetime.utcnow()
    )

@app.get("/charts/trending/now", tags=["Charts", "Trending"], summary="Current trending song")
async def get_current_trending(
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get the single trending song for current 8-hour window"""
    # Get recent songs
    cursor = await db_manager.conn.execute("""
        SELECT * FROM songs 
        WHERE ingested_at > datetime('now', '-3 days')
        AND source_type = 'youtube'
        ORDER BY ingested_at DESC
        LIMIT 500
    """)
    
    rows = await cursor.fetchall()
    recent_songs = [dict(row) for row in rows]
    
    # Get trending song
    trending_song = await TrendingEngine.get_current_trending_song(recent_songs)
    
    if not trending_song:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No trending song available for current window"
        )
    
    window_info = TrendingEngine.get_current_window_info()
    
    return {
        "trending_song": trending_song,
        "trending_window": window_info,
        "next_change": window_info["next_window_in"],
        "timestamp": datetime.utcnow().isoformat()
    }

# ====== INGESTION ENDPOINTS ======
@app.post("/ingest/youtube", tags=["Ingestion", "YouTube"], summary="Ingest YouTube data")
async def ingest_youtube(
    request: YouTubeIngestRequest,
    auth: bool = Depends(AuthService.verify_youtube),
    db_manager: DatabaseManager = Depends(get_db_manager),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """
    Ingest YouTube data with Ugandan content validation.
    
    Only songs by recognized Ugandan artists are processed.
    All songs are validated against chart rules before ingestion.
    """
    try:
        added_count = 0
        rejected_count = 0
        valid_songs = []
        validation_results = []
        
        # Process each song
        for song_data in request.songs:
            # Create Song object
            song = Song(
                external_id=f"youtube_{hashlib.md5(f'{song_data.title}{song_data.artist}'.encode()).hexdigest()[:12]}",
                title=song_data.title,
                artist=song_data.artist,
                plays=song_data.plays,
                score=song_data.score,
                region=song_data.region,
                district=song_data.district,
                source=song_data.source,
                source_type="youtube",
                ingested_at=datetime.utcnow(),
                metadata={
                    **song_data.metadata,
                    "category": request.category,
                    "channel_id": request.channel_id,
                    "video_id": request.video_id,
                    "ingestion_batch": request.source
                }
            )
            
            # Validate against chart rules
            song_dict = song.model_dump()
            is_valid, errors = ChartRulesEngine.validate_song_for_chart(song_dict)
            
            if is_valid:
                valid_songs.append(song)
                validation_results.append({
                    "song": f"{song.title} - {song.artist}",
                    "status": "accepted",
                    "region": song.region,
                    "validation_score": song.score
                })
            else:
                rejected_count += 1
                validation_results.append({
                    "song": f"{song.title} - {song.artist}",
                    "status": "rejected",
                    "errors": errors,
                    "region": song.region
                })
        
        # Add valid songs to database
        if valid_songs:
            result = await db_manager.add_songs(valid_songs)
            added_count = result["added"]
            
            # Invalidate relevant caches
            if redis_cache:
                await redis_cache.invalidate_pattern("top100:*")
                await redis_cache.invalidate_pattern("trending:*")
                await redis_cache.invalidate_pattern(f"region:{request.songs[0].region}:*")
        
        # Log ingestion
        logger.info(
            f"YouTube ingestion from {request.source}: "
            f"{added_count} added, {rejected_count} rejected, "
            f"{len(request.songs)} total"
        )
        
        return {
            "status": "success",
            "message": f"Processed {len(request.songs)} YouTube songs",
            "source": request.source,
            "results": {
                "total_received": len(request.songs),
                "valid_accepted": len(valid_songs),
                "added_to_database": added_count,
                "rejected": rejected_count,
                "validation_results": validation_results[:10]  # First 10 for reference
            },
            "region_breakdown": {
                region: sum(1 for s in valid_songs if s.region == region)
                for region in config.VALID_REGIONS
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"YouTube ingestion failed: {str(e)}"
        )

@app.post("/ingest/tv", tags=["Ingestion"], summary="Ingest TV data")
async def ingest_tv(
    request: IngestRequest,
    auth: bool = Depends(AuthService.verify_ingest),
    db_manager: DatabaseManager = Depends(get_db_manager),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """Ingest TV station data"""
    try:
        # Convert to Song objects
        songs = []
        for song_data in request.songs:
            song = Song(
                external_id=f"tv_{hashlib.md5(f'{song_data.title}{song_data.artist}'.encode()).hexdigest()[:12]}",
                title=song_data.title,
                artist=song_data.artist,
                plays=song_data.plays,
                score=song_data.score,
                region=song_data.region,
                district=song_data.district,
                source=song_data.source,
                source_type="tv",
                ingested_at=datetime.utcnow(),
                metadata={
                    **song_data.metadata,
                    "ingestion_batch": request.source
                }
            )
            songs.append(song)
        
        # Add to database
        result = await db_manager.add_songs(songs)
        
        # Invalidate caches
        if redis_cache:
            await redis_cache.invalidate_pattern("top100:*")
            await redis_cache.invalidate_pattern("trending:*")
        
        return {
            "status": "success",
            "message": f"Processed {len(songs)} TV songs",
            "source": request.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"TV ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"TV ingestion failed: {str(e)}"
        )

# ====== YOUTUBE WORKER ENDPOINTS ======
@app.post("/worker/youtube/pull", tags=["YouTube"], summary="Trigger YouTube worker pull")
async def trigger_youtube_pull(
    channels: Optional[List[str]] = Query(None, description="Specific channels to pull"),
    max_videos: int = Query(50, ge=1, le=200, description="Maximum videos to pull"),
    background_tasks: BackgroundTasks = None,
    auth: bool = Depends(AuthService.verify_internal),
    youtube_worker: Optional[YouTubeWorkerClient] = Depends(get_youtube_worker)
):
    """Trigger YouTube worker to pull data"""
    if not youtube_worker:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="YouTube worker not configured"
        )
    
    try:
        # Trigger pull in background
        async def pull_task():
            try:
                result = await youtube_worker.trigger_pull(channels)
                logger.info(f"YouTube worker pull completed: {result}")
                
                # Log in database
                await app_state.db_manager.conn.execute("""
                    INSERT INTO youtube_worker_logs 
                    (worker_id, channel_id, video_count, success_count, error_count, status, completed_at)
                    VALUES (?, ?, ?, ?, ?, 'completed', ?)
                """, (
                    result.get("worker_id", "unknown"),
                    ",".join(channels) if channels else "all",
                    result.get("total_videos", 0),
                    result.get("processed", 0),
                    result.get("errors", 0),
                    datetime.utcnow().isoformat()
                ))
                await app_state.db_manager.conn.commit()
                
            except Exception as e:
                logger.error(f"YouTube worker pull failed: {e}")
                # Log error in database
                try:
                    await app_state.db_manager.conn.execute("""
                        INSERT INTO youtube_worker_logs 
                        (worker_id, status, error_message, completed_at)
                        VALUES (?, 'failed', ?, ?)
                    """, ("unknown", str(e), datetime.utcnow().isoformat()))
                    await app_state.db_manager.conn.commit()
                except:
                    pass
        
        if background_tasks:
            background_tasks.add_task(pull_task)
        else:
            # Run immediately if no background tasks
            await pull_task()
        
        return {
            "status": "triggered",
            "message": "YouTube worker pull initiated",
            "channels": channels or ["all"],
            "max_videos": max_videos,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"YouTube worker trigger error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger YouTube worker: {str(e)}"
        )

@app.get("/worker/youtube/status", tags=["YouTube"], summary="YouTube worker status")
async def get_youtube_worker_status(
    auth: bool = Depends(AuthService.verify_internal),
    youtube_worker: Optional[YouTubeWorkerClient] = Depends(get_youtube_worker)
):
    """Get YouTube worker status"""
    if not youtube_worker:
        return {"status": "not_configured"}
    
    try:
        status = await youtube_worker.get_status()
        
        # Get recent logs from database
        cursor = await app_state.db_manager.conn.execute("""
            SELECT * FROM youtube_worker_logs 
            ORDER BY started_at DESC 
            LIMIT 10
        """)
        logs = await cursor.fetchall()
        
        return {
            "worker_status": status,
            "recent_logs": [dict(log) for log in logs],
            "connected": True
        }
    except Exception as e:
        return {
            "worker_status": {"status": "error", "error": str(e)},
            "connected": False
        }

# ====== ADMIN ENDPOINTS ======
@app.get("/admin/health", tags=["Admin"], summary="Admin health check")
async def admin_health(
    auth: bool = Depends(AuthService.verify_admin),
    db_manager: DatabaseManager = Depends(get_db_manager),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """Detailed health check with system information (admin only)"""
    uptime = datetime.utcnow() - app_state.startup_time
    
    # Get database statistics
    cursor = await db_manager.conn.execute("SELECT COUNT(*) FROM songs")
    song_count = (await cursor.fetchone())[0]
    
    cursor = await db_manager.conn.execute("SELECT COUNT(DISTINCT artist) FROM songs")
    artist_count = (await cursor.fetchone())[0]
    
    cursor = await db_manager.conn.execute("SELECT COUNT(*) FROM chart_weeks")
    week_count = (await cursor.fetchone())[0]
    
    # Get Redis statistics
    redis_stats = await redis_cache.get_stats() if redis_cache else {"connected": False}
    
    # Get trending window info
    window_info = TrendingEngine.get_current_window_info()
    
    # Check for pending ingestion
    cursor = await db_manager.conn.execute("""
        SELECT COUNT(*) FROM songs 
        WHERE chart_week IS NULL
    """)
    pending_count = (await cursor.fetchone())[0]
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "environment": config.ENV.value,
            "uptime": str(uptime).split('.')[0],
            "uptime_seconds": int(uptime.total_seconds()),
            "requests_served": app_state.request_count,
            "python_version": sys.version,
            "host": config.HOST,
            "port": config.PORT
        },
        "database": {
            "total_songs": song_count,
            "unique_artists": artist_count,
            "chart_weeks": week_count,
            "pending_for_chart": pending_count,
            "path": str(config.SQLITE_PATH)
        },
        "cache": redis_stats,
        "trending": window_info,
        "chart_week": app_state.current_chart_week,
        "authentication": {
            "admin_configured": bool(config.ADMIN_TOKEN),
            "ingest_configured": bool(config.INGEST_TOKEN),
            "youtube_configured": bool(config.YOUTUBE_TOKEN),
            "internal_configured": bool(config.INTERNAL_TOKEN)
        },
        "ugandan_rules": config.UGANDAN_MUSIC_RULES,
        "connections": {
            "active": len(app_state.active_connections),
            "background_tasks": len(app_state.background_tasks)
        }
    }

@app.post("/admin/publish/weekly", tags=["Admin"], summary="Publish weekly chart")
async def publish_weekly(
    auth: bool = Depends(AuthService.verify_admin),
    db_manager: DatabaseManager = Depends(get_db_manager),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """Publish weekly chart for all regions (admin only)"""
    try:
        # Get current chart week
        week_id = app_state.current_chart_week
        
        # Get top songs for each region
        region_songs = {}
        for region in config.VALID_REGIONS:
            songs = await db_manager.get_top_songs(
                config.CHART_CONFIG["top100_size"], 
                region, 
                week_id
            )
            ranked_songs = await ChartRulesEngine.generate_chart_rankings(songs, region)
            region_songs[region] = ranked_songs
        
        # Publish to database
        success = await db_manager.publish_chart_week(week_id, region_songs)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to publish chart week"
            )
        
        # Invalidate all caches
        if redis_cache:
            await redis_cache.invalidate_pattern("*")
        
        # Calculate new chart week
        new_week_id, new_week_start, new_week_end = ChartRulesEngine.calculate_chart_week()
        app_state.current_chart_week = new_week_id
        
        # Create new chart week in database
        await db_manager.conn.execute("""
            INSERT OR IGNORE INTO chart_weeks (week_id, start_date, end_date, status)
            VALUES (?, ?, ?, 'active')
        """, (new_week_id, new_week_start.isoformat(), new_week_end.isoformat()))
        await db_manager.conn.commit()
        
        logger.info(f"Chart week published: {week_id} -> {new_week_id}")
        
        return {
            "status": "success",
            "message": "Weekly chart published successfully",
            "published_week": week_id,
            "new_week": new_week_id,
            "published_at": datetime.utcnow().isoformat(),
            "summary": {
                "regions_published": len(region_songs),
                "total_songs_published": sum(len(songs) for songs in region_songs.values()),
                "regional_breakdown": {
                    region: len(songs) for region, songs in region_songs.items()
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Weekly publish error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish weekly chart: {str(e)}"
        )

@app.get("/admin/stats", tags=["Admin"], summary="Admin statistics")
async def admin_stats(
    auth: bool = Depends(AuthService.verify_admin),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Detailed system statistics (admin only)"""
    # Get comprehensive statistics
    stats = {}
    
    # Song statistics by source
    cursor = await db_manager.conn.execute("""
        SELECT source_type, COUNT(*) as count, 
               SUM(plays) as total_plays, AVG(score) as avg_score
        FROM songs 
        GROUP BY source_type
    """)
    source_stats = await cursor.fetchall()
    
    stats["by_source"] = [
        {
            "source_type": row[0],
            "count": row[1],
            "total_plays": row[2],
            "average_score": round(row[3], 2)
        }
        for row in source_stats
    ]
    
    # Regional statistics
    cursor = await db_manager.conn.execute("""
        SELECT region, COUNT(*) as count, 
               SUM(plays) as total_plays, AVG(score) as avg_score,
               COUNT(DISTINCT artist) as unique_artists
        FROM songs 
        GROUP BY region
    """)
    region_stats = await cursor.fetchall()
    
    stats["by_region"] = [
        {
            "region": row[0],
            "name": config.UGANDAN_REGIONS[row[0]]["name"],
            "count": row[1],
            "total_plays": row[2],
            "average_score": round(row[3], 2),
            "unique_artists": row[4],
            "quota_percentage": config.UGANDAN_MUSIC_RULES["regional_quotas"].get(row[0], 0)
        }
        for row in region_stats
    ]
    
    # Chart statistics
    cursor = await db_manager.conn.execute("""
        SELECT COUNT(*) as total_weeks,
               MIN(start_date) as first_week,
               MAX(start_date) as last_week,
               SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END) as published_weeks
        FROM chart_weeks
    """)
    chart_stats = await cursor.fetchone()
    
    stats["charts"] = {
        "total_weeks": chart_stats[0],
        "first_week": chart_stats[1],
        "last_week": chart_stats[2],
        "published_weeks": chart_stats[3]
    }
    
    # Top artists
    cursor = await db_manager.conn.execute("""
        SELECT artist, COUNT(*) as song_count, 
               SUM(plays) as total_plays, AVG(score) as avg_score
        FROM songs 
        GROUP BY artist 
        ORDER BY total_plays DESC 
        LIMIT 20
    """)
    top_artists = await cursor.fetchall()
    
    stats["top_artists"] = [
        {
            "artist": row[0],
            "song_count": row[1],
            "total_plays": row[2],
            "average_score": round(row[3], 2)
        }
        for row in top_artists
    ]
    
    # Recent activity
    cursor = await db_manager.conn.execute("""
        SELECT DATE(ingested_at) as date, 
               COUNT(*) as song_count,
               SUM(plays) as total_plays
        FROM songs 
        WHERE ingested_at > datetime('now', '-30 days')
        GROUP BY DATE(ingested_at)
        ORDER BY date DESC
        LIMIT 30
    """)
    recent_activity = await cursor.fetchall()
    
    stats["recent_activity"] = [
        {
            "date": row[0],
            "song_count": row[1],
            "total_plays": row[2]
        }
        for row in recent_activity
    ]
    
    return {
        "statistics": stats,
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": app_state.current_chart_week,
        "trending_window": TrendingEngine.get_current_window_info()
    }

# ====== SYSTEM ENDPOINTS ======
@app.get("/system/cache/stats", tags=["System"], summary="Cache statistics")
async def cache_stats(
    auth: bool = Depends(AuthService.verify_admin),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """Get Redis cache statistics"""
    if not redis_cache:
        return {"cache": "not_configured"}
    
    stats = await redis_cache.get_stats()
    return {
        "cache": stats,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/system/cache/clear", tags=["System"], summary="Clear cache")
async def clear_cache(
    pattern: str = Query("*", description="Cache key pattern to clear"),
    auth: bool = Depends(AuthService.verify_admin),
    redis_cache: Optional[RedisCache] = Depends(get_redis_cache)
):
    """Clear Redis cache entries"""
    if not redis_cache:
        return {"status": "cache_not_configured"}
    
    cleared = await redis_cache.invalidate_pattern(pattern)
    
    return {
        "status": "success",
        "message": f"Cleared {cleared} cache entries",
        "pattern": pattern,
        "cleared_count": cleared,
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
            "query_params": dict(request.query_params)
        }
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": request.headers.get("X-Request-ID", "unknown")
        },
        headers=exc.headers
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    error_id = hashlib.md5(f"{time.time()}{exc}".encode()).hexdigest()[:8]
    
    logger.error(
        f"Unhandled exception {error_id} at {request.url.path}: {exc}",
        exc_info=True,
        extra={
            "error_id": error_id,
            "path": request.url.path,
            "method": request.method,
            "client_ip": request.client.host if request.client else "unknown",
            "user_agent": request.headers.get("user-agent")
        }
    )
    
    error_detail = str(exc) if config.DEBUG else f"Internal server error (ID: {error_id})"
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "detail": error_detail,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": request.headers.get("X-Request-ID", "unknown")
        }
    )

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    
    # Enhanced startup banner
    banner = f"""
    {'='*60}
    UG Board Engine v9.0.0 - Production System
    {'='*60}
    Environment:      {config.ENV.value}
    Database:         {config.SQLITE_PATH}
    Redis:            {'Connected' if config.REDIS_URL else 'Disabled'}
    YouTube Worker:   {config.YOUTUBE_WORKER_URL}
    Regions:          {', '.join(sorted(config.VALID_REGIONS))}
    Chart Week:       {app_state.current_chart_week}
    Trending Window:  {config.TRENDING_WINDOW_HOURS} hours
    {'='*60}
    Server:           http://{config.HOST}:{config.PORT}
    Documentation:    http://{config.HOST}:{config.PORT}/docs
    Health Check:     http://{config.HOST}:{config.PORT}/health
    {'='*60}
    """
    
    print(banner)
    
    # Run server
    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_level="info" if config.ENV == Environment.PRODUCTION else "debug",
        timeout_keep_alive=config.CHART_CONFIG.get("request_timeout", 30),
        access_log=False if config.ENV == Environment.PRODUCTION else True
    )
