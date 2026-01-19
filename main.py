"""
UG Board Engine - Canonical Implementation (v1.0)
File-backed, immutable weekly chart system for Ugandan music
Primary Focus: Ugandan artists with strict validation rules
Foreign artists only allowed in collaborations with Ugandan artists
"""

import os
import json
import hashlib
import uuid
import re
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from pathlib import Path
import logging
import asyncio

from fastapi import FastAPI, HTTPException, Header, Body, Path, Query, status, Depends
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

from pydantic import BaseModel, Field, validator
import yaml

# =========================
# Configuration & Constants
# =========================

# Environment-based configuration
PORT = int(os.getenv("PORT", 8000))
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Authentication tokens (must be set in production)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2026")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "ingest-ug-board-2026")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# Chart week configuration
WEEK_START_DAY = 1  # Monday (ISO week)
CURRENT_WEEK = datetime.utcnow().isocalendar()[1]
CURRENT_YEAR = datetime.utcnow().year

# =========================
# Logging Configuration
# =========================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(DATA_DIR / "logs" / "engine.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# =========================
# Data Directory Structure
# =========================

# Ensure canonical directory structure
DIRECTORIES = [
    DATA_DIR / "weeks",          # Weekly chart snapshots (immutable)
    DATA_DIR / "regions",        # Regional data
    DATA_DIR / "ingestion",      # Ingestion logs
    DATA_DIR / "store",          # Live data store
    DATA_DIR / "state",          # Scheduler and state
    DATA_DIR / "index",          # Published week index
    DATA_DIR / "logs",           # Application logs
    DATA_DIR / "backups",        # Manual backups
]

for directory in DIRECTORIES:
    directory.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured directory exists: {directory}")

# =========================
# Pydantic Models
# =========================

class Artist(BaseModel):
    """Artist model with Ugandan validation"""
    name: str
    type: str = Field(description="ugandan, foreign_collaborator, or unknown")
    
    @validator('type')
    def validate_type(cls, v):
        allowed = {"ugandan", "foreign_collaborator", "unknown"}
        if v not in allowed:
            raise ValueError(f"Artist type must be one of {allowed}")
        return v

class SongItem(BaseModel):
    """Song item with Ugandan music validation"""
    title: str
    artist: str
    song_id: Optional[str] = None
    score: float = Field(ge=0.0, le=100.0, default=0.0)
    plays: int = Field(ge=0, default=0)
    change: str = Field(default="same", pattern="^(up|down|same|new)$")
    genre: str = "afrobeat"
    region: str = "ug"
    release_date: Optional[str] = None
    metadata: Dict[str, Any] = {}
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "Nalumansi",
                "artist": "Bobi Wine",
                "score": 95.5,
                "plays": 10000,
                "change": "up",
                "genre": "kadongo kamu",
                "region": "ug"
            }
        }

class TVIngestionPayload(BaseModel):
    """TV ingestion payload with validation"""
    items: List[SongItem]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    source: str = Field(description="TV station name or identifier")
    metadata: Dict[str, Any] = {}
    
    @validator('items')
    def validate_items(cls, v):
        if not v:
            raise ValueError("Items list cannot be empty")
        if len(v) > 100:
            raise ValueError("Maximum 100 items per ingestion")
        return v

class RadioIngestionPayload(BaseModel):
    """Radio ingestion payload with validation"""
    items: List[SongItem]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    station: str = Field(description="Radio station name")
    frequency: Optional[str] = None
    metadata: Dict[str, Any] = {}

class ChartWeek(BaseModel):
    """Chart week model for immutable snapshots"""
    week_id: str
    year: int
    week_number: int
    status: str = Field(pattern="^(open|locked|published)$")
    opened_at: str
    locked_at: Optional[str] = None
    published_at: Optional[str] = None
    regions: List[str] = ["ug"]
    metadata: Dict[str, Any] = {}

# =========================
# UGANDAN MUSIC RULES & VALIDATION ENGINE
# =========================

class UgandanMusicRules:
    """Canonical Ugandan music validation engine"""
    
    # Primary region - Uganda is the main focus
    PRIMARY_REGION = "ug"
    
    # Known Ugandan artists database (canonical list)
    UGANDAN_ARTISTS = {
        # Male Artists
        "bobi wine", "joseph mayanja", "eddy kenzo", "daddy andre", "gravity omutujju",
        "fik fameica", "vyroota", "geosteady", "choppa", "feffe busi", "leyla kayondo",
        "recho rey", "vivian todi", "cindy sanyu", "niniola", "sheebah karungi",
        "spice diana", "winnie nwagi", "vinka", "zex bilangilangi", "john blaq",
        "pallaso", "navio", "gnl zamba", "rickman manrick", "buchaman", "ragga dee",
        "bebe cool", "goodlyfe", "radio & weasel", "moses matthew",
        
        # Female Artists
        "lillian mbabazi", "judith babirye", "irene namatovu", "remina", 
        "kate kasumba", "karole kasita", "fille", "maro", "lady slyke",
        
        # Groups/Bands
        "ghetto kids", "team no sleep", "swangz avenue", "black skin",
        "east african party", "acholi artists", "bataka squad", "kadongo kamu ensemble"
    }
    
    # Foreign artists known to collaborate with Ugandans (approved list)
    FOREIGN_COLLABORATORS = {
        "davido", "wizkid", "burna boy", "tiwa savage", "yemi alade",
        "diamond platnumz", "rayvanny", "harmonize", "nandy", "juma nature",
        "khaligraph jones", "sauti sol", "nyashinski", "otile brown",
        "shakira", "akon", "rick ross", "michael jackson"
    }
    
    # Ugandan music genres
    UGANDAN_GENRES = {
        "kadongo kamu", "kidandali", "afrobeat", "dancehall", "reggae",
        "gospel", "hip hop", "rnb", "traditional", "zouk", "bongo flava",
        "folk", "contemporary", "afropop", "ragga"
    }
    
    @classmethod
    def is_ugandan_artist(cls, artist_name: str) -> bool:
        """Check if artist is Ugandan using canonical rules"""
        if not artist_name:
            return False
        
        artist_lower = artist_name.lower().strip()
        
        # Check exact match in canonical database
        if artist_lower in cls.UGANDAN_ARTISTS:
            return True
        
        # Check for Ugandan name patterns (fallback)
        ugandan_patterns = [
            r'\b(mayanja|omutujju|busi|kayondo|sanyu|karungi)\b',
            r'\b(fameica|blaq|pallaso|navio|zamba|choppa)\b',
            r'\b(ghetto|team no|swangz|black skin|bataka)\b',
            r'\b(bobi|eddy|daddy|gravity|sheebah|spice)\b'
        ]
        
        for pattern in ugandan_patterns:
            if re.search(pattern, artist_lower, re.IGNORECASE):
                logger.debug(f"Pattern match for {artist_name}: {pattern}")
                return True
        
        return False
    
    @classmethod
    def is_known_collaborator(cls, artist_name: str) -> bool:
        """Check if artist is a known foreign collaborator"""
        if not artist_name:
            return False
        return artist_name.lower().strip() in cls.FOREIGN_COLLABORATORS
    
    @classmethod
    def extract_artists(cls, artist_field: str) -> List[str]:
        """Canonical artist extraction from artist field"""
        if not artist_field:
            return []
        
        # Standardize separators
        standardized = artist_field
        separators = [' feat. ', ' ft. ', ' featuring ', ' & ', ' x ', ' , ', ' with ', ' and ', ' vs. ']
        for sep in separators:
            standardized = standardized.replace(sep, '|')
        
        # Split and clean
        artists = [a.strip() for a in standardized.split('|') if a.strip()]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_artists = []
        for artist in artists:
            if artist.lower() not in seen:
                seen.add(artist.lower())
                unique_artists.append(artist)
        
        return unique_artists
    
    @classmethod
    def validate_song(cls, song: SongItem) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Canonical song validation with Ugandan music rules.
        Returns: (is_valid, error_message, metadata)
        """
        try:
            # Extract artists
            artists = cls.extract_artists(song.artist)
            
            if not artists:
                return False, "No artists specified", {}
            
            # Check if all artists are Ugandan (allowed)
            all_ugandan = all(cls.is_ugandan_artist(artist) for artist in artists)
            if all_ugandan:
                metadata = {
                    "artist_types": ["ugandan"] * len(artists),
                    "validation": "pure_ugandan",
                    "risk_level": "low"
                }
                return True, "", metadata
            
            # Check if there's at least one Ugandan artist
            has_ugandan = any(cls.is_ugandan_artist(artist) for artist in artists)
            if not has_ugandan:
                return False, "No Ugandan artist found. Foreign artists must collaborate with Ugandan artists.", {}
            
            # Check foreign artists are known collaborators
            foreign_artists = [a for a in artists if not cls.is_ugandan_artist(a)]
            unknown_foreign = []
            
            for foreign in foreign_artists:
                if not cls.is_known_collaborator(foreign):
                    unknown_foreign.append(foreign)
            
            if unknown_foreign:
                error_msg = f"Foreign artist(s) not in approved collaborator list: {', '.join(unknown_foreign)}"
                return False, error_msg, {}
            
            # Valid collaboration
            artist_types = []
            for artist in artists:
                if cls.is_ugandan_artist(artist):
                    artist_types.append("ugandan")
                else:
                    artist_types.append("foreign_collaborator")
            
            metadata = {
                "artist_types": artist_types,
                "validation": "ugandan_collaboration",
                "risk_level": "medium",
                "foreign_count": len(foreign_artists),
                "ugandan_count": len(artists) - len(foreign_artists)
            }
            
            return True, "", metadata
            
        except Exception as e:
            logger.error(f"Validation error for song {song.title}: {e}")
            return False, f"Validation error: {str(e)}", {}
    
    @classmethod
    def generate_song_id(cls, song: SongItem) -> str:
        """Generate deterministic song ID for tracking"""
        # Create hash from title, artist, and release date
        hash_input = f"{song.title}|{song.artist}|{song.release_date or ''}"
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        
        # Convert to base62 for shorter ID
        import base64
        b64 = base64.urlsafe_b64encode(hash_bytes[:12]).decode().rstrip('=')
        
        return f"song_{b64}"

# =========================
# Chart Week Authority (Canonical)
# =========================

class ChartWeekAuthority:
    """Canonical week lifecycle management"""
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.current_week = self._get_or_create_current_week()
    
    def _get_or_create_current_week(self) -> ChartWeek:
        """Get current week or create if doesn't exist"""
        week_number = datetime.utcnow().isocalendar()[1]
        year = datetime.utcnow().year
        week_id = f"{year}-W{week_number:02d}"
        
        week_file = self.data_dir / "weeks" / f"{week_id}.json"
        
        if week_file.exists():
            try:
                with open(week_file, 'r') as f:
                    data = json.load(f)
                return ChartWeek(**data)
            except Exception as e:
                logger.error(f"Failed to load week {week_id}: {e}")
                # Create new week as fallback
        
        # Create new week
        week = ChartWeek(
            week_id=week_id,
            year=year,
            week_number=week_number,
            status="open",
            opened_at=datetime.utcnow().isoformat(),
            regions=["ug", "eac", "afr", "ww"],
            metadata={
                "created_by": "system",
                "environment": ENVIRONMENT
            }
        )
        
        # Save week file
        self._save_week(week)
        
        logger.info(f"Created new chart week: {week_id}")
        return week
    
    def _save_week(self, week: ChartWeek):
        """Save week file (immutable after publishing)"""
        week_file = self.data_dir / "weeks" / f"{week.week_id}.json"
        
        # Check if week is already published (immutable)
        if week_file.exists() and week.status == "published":
            raise ValueError(f"Week {week.week_id} is already published and cannot be modified")
        
        with open(week_file, 'w') as f:
            json.dump(week.dict(), f, indent=2, default=str)
    
    def lock_week(self, week_id: str) -> bool:
        """Lock a week (prevent further modifications)"""
        week_file = self.data_dir / "weeks" / f"{week_id}.json"
        
        if not week_file.exists():
            logger.error(f"Week {week_id} not found")
            return False
        
        try:
            with open(week_file, 'r') as f:
                data = json.load(f)
            
            week = ChartWeek(**data)
            
            if week.status != "open":
                logger.error(f"Week {week_id} is not open (status: {week.status})")
                return False
            
            week.status = "locked"
            week.locked_at = datetime.utcnow().isoformat()
            
            with open(week_file, 'w') as f:
                json.dump(week.dict(), f, indent=2, default=str)
            
            logger.info(f"Locked week: {week_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to lock week {week_id}: {e}")
            return False
    
    def publish_week(self, week_id: str) -> bool:
        """Publish a week (make immutable)"""
        week_file = self.data_dir / "weeks" / f"{week_id}.json"
        
        if not week_file.exists():
            logger.error(f"Week {week_id} not found")
            return False
        
        try:
            with open(week_file, 'r') as f:
                data = json.load(f)
            
            week = ChartWeek(**data)
            
            if week.status != "locked":
                logger.error(f"Week {week_id} is not locked (status: {week.status})")
                return False
            
            week.status = "published"
            week.published_at = datetime.utcnow().isoformat()
            
            # Write final version
            with open(week_file, 'w') as f:
                json.dump(week.dict(), f, indent=2, default=str)
            
            # Add to published index
            self._add_to_index(week)
            
            # Create region snapshots
            self._create_region_snapshots(week_id)
            
            logger.info(f"Published week: {week_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish week {week_id}: {e}")
            return False
    
    def _add_to_index(self, week: ChartWeek):
        """Add published week to immutable index"""
        index_file = self.data_dir / "index" / "published_weeks.json"
        
        index = []
        if index_file.exists():
            with open(index_file, 'r') as f:
                try:
                    index = json.load(f)
                except json.JSONDecodeError:
                    index = []
        
        # Add week to index
        index_entry = {
            "week_id": week.week_id,
            "published_at": week.published_at,
            "year": week.year,
            "week_number": week.week_number,
            "regions": week.regions,
            "hash": self._calculate_week_hash(week.week_id)
        }
        
        index.append(index_entry)
        
        # Sort by publication date
        index.sort(key=lambda x: x["published_at"], reverse=True)
        
        # Keep only last 52 weeks in main index (1 year)
        if len(index) > 52:
            old_index = index[52:]
            index = index[:52]
            
            # Move old weeks to archive
            self._archive_weeks(old_index)
        
        with open(index_file, 'w') as f:
            json.dump(index, f, indent=2, default=str)
    
    def _calculate_week_hash(self, week_id: str) -> str:
        """Calculate hash of week file for integrity verification"""
        week_file = self.data_dir / "weeks" / f"{week_id}.json"
        
        if not week_file.exists():
            return ""
        
        with open(week_file, 'rb') as f:
            content = f.read()
        
        return hashlib.sha256(content).hexdigest()
    
    def _create_region_snapshots(self, week_id: str):
        """Create immutable region snapshots for published week"""
        # This would create region-specific chart snapshots
        # Implementation depends on your region aggregation logic
        pass
    
    def _archive_weeks(self, old_weeks: List[Dict]):
        """Archive old weeks to separate file"""
        archive_file = self.data_dir / "index" / "archive.json"
        
        archive = []
        if archive_file.exists():
            with open(archive_file, 'r') as f:
                try:
                    archive = json.load(f)
                except json.JSONDecodeError:
                    archive = []
        
        archive.extend(old_weeks)
        
        with open(archive_file, 'w') as f:
            json.dump(archive, f, indent=2, default=str)

# =========================
# Data Store with File-Backed Storage
# =========================

class UgandanMusicStore:
    """File-backed music data store following canonical architecture"""
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.live_store = data_dir / "store" / "live_data.json"
        self.week_authority = ChartWeekAuthority(data_dir)
        
        # Initialize store if not exists
        self._init_store()
    
    def _init_store(self):
        """Initialize the live data store"""
        if not self.live_store.exists():
            initial_data = {
                "version": "1.0",
                "created_at": datetime.utcnow().isoformat(),
                "regions": {
                    "ug": {"songs": [], "last_updated": None},
                    "eac": {"songs": [], "last_updated": None},
                    "afr": {"songs": [], "last_updated": None},
                    "ww": {"songs": [], "last_updated": None}
                },
                "statistics": {
                    "total_songs": 0,
                    "ugandan_songs": 0,
                    "collaborations": 0,
                    "last_ingestion": None
                }
            }
            
            self.live_store.parent.mkdir(parents=True, exist_ok=True)
            with open(self.live_store, 'w') as f:
                json.dump(initial_data, f, indent=2)
    
    def ingest_tv_data(self, payload: TVIngestionPayload) -> Dict[str, Any]:
        """Ingest TV data with canonical validation"""
        try:
            # Validate all songs
            valid_items = []
            validation_report = {
                "total_received": len(payload.items),
                "valid": 0,
                "invalid": 0,
                "reasons": {},
                "artist_breakdown": {
                    "pure_ugandan": 0,
                    "collaborations": 0,
                    "rejected_foreign": 0
                }
            }
            
            for item in payload.items:
                is_valid, error_msg, metadata = UgandanMusicRules.validate_song(item)
                
                if is_valid:
                    valid_items.append({
                        **item.dict(),
                        "metadata": {
                            **item.metadata,
                            **metadata,
                            "ingestion_source": "tv",
                            "tv_station": payload.source,
                            "ingested_at": datetime.utcnow().isoformat()
                        }
                    })
                    validation_report["valid"] += 1
                    
                    # Update artist breakdown
                    if metadata.get("validation") == "pure_ugandan":
                        validation_report["artist_breakdown"]["pure_ugandan"] += 1
                    else:
                        validation_report["artist_breakdown"]["collaborations"] += 1
                else:
                    validation_report["invalid"] += 1
                    if error_msg not in validation_report["reasons"]:
                        validation_report["reasons"][error_msg] = 0
                    validation_report["reasons"][error_msg] += 1
            
            if valid_items:
                # Update live store
                self._update_live_store(valid_items, "tv", payload.source)
                
                # Log ingestion
                self._log_ingestion("tv", validation_report, payload.source)
            
            return {
                "status": "success",
                "message": f"Processed {validation_report['valid']} valid items from TV",
                "validation_report": validation_report,
                "week_id": self.week_authority.current_week.week_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"TV ingestion failed: {e}")
            raise
    
    def ingest_radio_data(self, payload: RadioIngestionPayload) -> Dict[str, Any]:
        """Ingest radio data with canonical validation"""
        try:
            # Similar to TV ingestion but with radio-specific metadata
            valid_items = []
            validation_report = {
                "total_received": len(payload.items),
                "valid": 0,
                "invalid": 0,
                "reasons": {},
                "artist_breakdown": {
                    "pure_ugandan": 0,
                    "collaborations": 0,
                    "rejected_foreign": 0
                }
            }
            
            for item in payload.items:
                is_valid, error_msg, metadata = UgandanMusicRules.validate_song(item)
                
                if is_valid:
                    valid_items.append({
                        **item.dict(),
                        "metadata": {
                            **item.metadata,
                            **metadata,
                            "ingestion_source": "radio",
                            "radio_station": payload.station,
                            "frequency": payload.frequency,
                            "ingested_at": datetime.utcnow().isoformat()
                        }
                    })
                    validation_report["valid"] += 1
                    
                    if metadata.get("validation") == "pure_ugandan":
                        validation_report["artist_breakdown"]["pure_ugandan"] += 1
                    else:
                        validation_report["artist_breakdown"]["collaborations"] += 1
                else:
                    validation_report["invalid"] += 1
                    if error_msg not in validation_report["reasons"]:
                        validation_report["reasons"][error_msg] = 0
                    validation_report["reasons"][error_msg] += 1
            
            if valid_items:
                # Update live store
                self._update_live_store(valid_items, "radio", payload.station)
                
                # Log ingestion
                self._log_ingestion("radio", validation_report, payload.station)
            
            return {
                "status": "success",
                "message": f"Processed {validation_report['valid']} valid items from radio",
                "validation_report": validation_report,
                "week_id": self.week_authority.current_week.week_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Radio ingestion failed: {e}")
            raise
    
    def _update_live_store(self, items: List[Dict], source: str, station: str):
        """Update the live data store with new items"""
        try:
            # Read current store
            with open(self.live_store, 'r') as f:
                store = json.load(f)
            
            # Update regions
            for item in items:
                region = item.get("region", "ug")
                if region in store["regions"]:
                    # Add or update song in region
                    song_id = item.get("song_id") or UgandanMusicRules.generate_song_id(SongItem(**item))
                    item["song_id"] = song_id
                    
                    # Check if song already exists in region
                    existing_idx = None
                    for idx, song in enumerate(store["regions"][region]["songs"]):
                        if song.get("song_id") == song_id:
                            existing_idx = idx
                            break
                    
                    if existing_idx is not None:
                        # Update existing song (increase plays, update score)
                        existing = store["regions"][region]["songs"][existing_idx]
                        existing["plays"] += item.get("plays", 1)
                        existing["score"] = max(existing.get("score", 0), item.get("score", 0))
                        existing["metadata"]["last_updated"] = datetime.utcnow().isoformat()
                    else:
                        # Add new song
                        store["regions"][region]["songs"].append(item)
                    
                    store["regions"][region]["last_updated"] = datetime.utcnow().isoformat()
            
            # Update statistics
            store["statistics"]["total_songs"] = sum(
                len(region_data["songs"]) for region_data in store["regions"].values()
            )
            store["statistics"]["last_ingestion"] = {
                "source": source,
                "station": station,
                "timestamp": datetime.utcnow().isoformat(),
                "count": len(items)
            }
            
            # Write back to store
            with open(self.live_store, 'w') as f:
                json.dump(store, f, indent=2, default=str)
            
            logger.info(f"Updated live store with {len(items)} items from {source}")
            
        except Exception as e:
            logger.error(f"Failed to update live store: {e}")
            raise
    
    def _log_ingestion(self, source: str, report: Dict, station: str):
        """Log ingestion to immutable log file"""
        log_file = self.data_dir / "ingestion" / f"{datetime.utcnow().date().isoformat()}.json"
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "station": station,
            "report": report,
            "week_id": self.week_authority.current_week.week_id
        }
        
        log_data = []
        if log_file.exists():
            with open(log_file, 'r') as f:
                try:
                    log_data = json.load(f)
                except json.JSONDecodeError:
                    log_data = []
        
        log_data.append(log_entry)
        
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2, default=str)
    
    def get_top100(self, region: str = "ug") -> List[Dict]:
        """Get top 100 songs for a region"""
        try:
            with open(self.live_store, 'r') as f:
                store = json.load(f)
            
            if region not in store["regions"]:
                return []
            
            songs = store["regions"][region]["songs"]
            
            # Sort by score (descending) and then plays (descending)
            sorted_songs = sorted(
                songs,
                key=lambda x: (x.get("score", 0), x.get("plays", 0)),
                reverse=True
            )
            
            # Add ranks
            for i, song in enumerate(sorted_songs[:100], 1):
                song["rank"] = i
            
            return sorted_songs[:100]
            
        except Exception as e:
            logger.error(f"Failed to get top100 for region {region}: {e}")
            return []
    
    def get_region_stats(self, region: str) -> Dict[str, Any]:
        """Get statistics for a region"""
        try:
            with open(self.live_store, 'r') as f:
                store = json.load(f)
            
            if region not in store["regions"]:
                return {}
            
            songs = store["regions"][region]["songs"]
            
            # Calculate statistics
            ugandan_count = sum(1 for song in songs 
                              if song.get("metadata", {}).get("validation") == "pure_ugandan")
            collaboration_count = sum(1 for song in songs 
                                     if song.get("metadata", {}).get("validation") == "ugandan_collaboration")
            total_plays = sum(song.get("plays", 0) for song in songs)
            avg_score = sum(song.get("score", 0) for song in songs) / len(songs) if songs else 0
            
            return {
                "total_songs": len(songs),
                "ugandan_songs": ugandan_count,
                "collaborations": collaboration_count,
                "total_plays": total_plays,
                "average_score": round(avg_score, 2),
                "last_updated": store["regions"][region]["last_updated"]
            }
            
        except Exception as e:
            logger.error(f"Failed to get stats for region {region}: {e}")
            return {}

# =========================
# Authentication Dependencies
# =========================

async def verify_admin_token(authorization: Optional[str] = Header(None)):
    """Verify admin token for privileged operations"""
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing"
        )
    
    if authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin token"
        )
    
    return True

async def verify_ingest_token(authorization: Optional[str] = Header(None)):
    """Verify ingestion token for data ingestion"""
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing"
        )
    
    if authorization != f"Bearer {INGEST_TOKEN}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid ingestion token"
        )
    
    return True

async def verify_internal_token(x_internal_token: Optional[str] = Header(None)):
    """Verify internal token for service-to-service communication"""
    if not x_internal_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Internal token header missing"
        )
    
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal token"
        )
    
    return True

# =========================
# Create FastAPI App with Canonical Configuration
# =========================

app = FastAPI(
    title="UG Board Engine - Canonical Implementation",
    description="File-backed, immutable weekly chart system for Ugandan music",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "charts",
            "description": "Chart data operations (read-only)"
        },
        {
            "name": "ingestion",
            "description": "Data ingestion endpoints (authenticated)"
        },
        {
            "name": "admin",
            "description": "Administrative operations (admin only)"
        },
        {
            "name": "system",
            "description": "System health and monitoring"
        }
    ]
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for documentation
app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize data store
data_store = UgandanMusicStore()
week_authority = ChartWeekAuthority()

# =========================
# System Endpoints
# =========================

@app.get("/", tags=["system"])
async def root():
    """Canonical root endpoint with system information"""
    return {
        "service": "UG Board Engine",
        "version": "1.0.0",
        "status": "online",
        "environment": ENVIRONMENT,
        "timestamp": datetime.utcnow().isoformat(),
        "focus": "Ugandan music and artists",
        "canonical_rules": [
            "Foreign artists only allowed in collaborations with Ugandan artists",
            "File-backed immutable weekly snapshots",
            "Truth over completeness - missing data is better than fabricated data",
            "Published weeks can never be modified"
        ],
        "current_week": week_authority.current_week.week_id,
        "week_status": week_authority.current_week.status,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "metrics": "/metrics",
            "charts": "/charts/top100",
            "tv_ingestion": "/ingest/tv",
            "radio_ingestion": "/ingest/radio"
        },
        "instance_id": hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()[:8]
    }

@app.get("/health", tags=["system"])
async def health_check():
    """Comprehensive health check"""
    try:
        # Check data directory
        data_dir_ok = DATA_DIR.exists() and DATA_DIR.is_dir()
        
        # Check live store
        store_ok = data_store.live_store.exists()
        
        # Check current week
        week_ok = week_authority.current_week is not None
        
        status = "healthy" if all([data_dir_ok, store_ok, week_ok]) else "degraded"
        
        return {
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {
                "data_directory": data_dir_ok,
                "live_store": store_ok,
                "current_week": week_ok
            },
            "week_id": week_authority.current_week.week_id,
            "week_status": week_authority.current_week.status,
            "environment": ENVIRONMENT
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/metrics", tags=["system"])
async def metrics():
    """System metrics for monitoring"""
    try:
        # Get store statistics
        store_stats = {}
        try:
            with open(data_store.live_store, 'r') as f:
                store = json.load(f)
            store_stats = store.get("statistics", {})
        except:
            pass
        
        # Count files in data directory
        file_counts = {}
        for directory in DIRECTORIES:
            if directory.exists():
                try:
                    file_counts[directory.name] = len(list(directory.iterdir()))
                except:
                    file_counts[directory.name] = 0
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "system": {
                "python_version": os.sys.version,
                "environment": ENVIRONMENT,
                "uptime": "N/A",  # Would need process start time
                "memory_usage": "N/A"  # Would need psutil
            },
            "data": {
                "file_counts": file_counts,
                "store_statistics": store_stats,
                "current_week": week_authority.current_week.week_id,
                "week_status": week_authority.current_week.status
            },
            "performance": {
                "requests_served": "N/A",  # Would need request counter
                "avg_response_time": "N/A"
            }
        }
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Metrics error: {str(e)}")

# =========================
# Chart Endpoints (Read-Only)
# =========================

@app.get("/charts/top100", tags=["charts"])
async def get_top100_chart(
    region: str = Query("ug", description="Region code: ug, eac, afr, ww")
):
    """Get Uganda Top 100 chart (or regional top)"""
    if region not in ["ug", "eac", "afr", "ww"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid region. Must be one of: ug, eac, afr, ww"
        )
    
    chart_data = data_store.get_top100(region)
    region_stats = data_store.get_region_stats(region)
    
    region_names = {
        "ug": "Uganda Top 100",
        "eac": "East African Community Top 100",
        "afr": "Africa Top 100",
        "ww": "Worldwide (Diaspora) Top 100"
    }
    
    return {
        "region": region,
        "region_name": region_names.get(region, "Unknown"),
        "chart_name": f"UG Board - {region_names.get(region, 'Chart')}",
        "week_id": week_authority.current_week.week_id,
        "week_status": week_authority.current_week.status,
        "published": week_authority.current_week.published_at or "Not published yet",
        "entries": chart_data[:20],  # Return first 20 for brevity
        "total_entries": len(chart_data),
        "statistics": region_stats,
        "rules_enforced": [
            "Foreign artists only in collaborations with Ugandan artists",
            "All songs validated through canonical Ugandan music rules"
        ],
        "timestamp": datetime.utcnow().isoformat(),
        "canonical": True
    }

@app.get("/charts/regions/{region}/top5", tags=["charts"])
async def get_region_top5(region: str = Path(..., description="Region code")):
    """Get regional top 5 chart (lightweight version)"""
    if region not in ["ug", "eac", "afr", "ww"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid region. Must be one of: ug, eac, afr, ww"
        )
    
    chart_data = data_store.get_top100(region)[:5]
    
    return {
        "region": region,
        "entries": chart_data,
        "week_id": week_authority.current_week.week_id,
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Ingestion Endpoints (Authenticated)
# =========================

@app.post("/ingest/tv", tags=["ingestion"])
async def ingest_tv_data(
    payload: TVIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
):
    """
    Ingest TV data from Ugandan TV stations.
    
    Requires: Bearer token with ingestion permissions.
    """
    try:
        result = data_store.ingest_tv_data(payload)
        
        # Additional TV-specific processing
        tv_metadata = {
            "station": payload.source,
            "ingestion_type": "tv",
            "processing_time": datetime.utcnow().isoformat()
        }
        
        result["metadata"] = tv_metadata
        
        logger.info(f"TV ingestion successful from {payload.source}: {result['message']}")
        return result
        
    except Exception as e:
        logger.error(f"TV ingestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"TV ingestion failed: {str(e)}"
        )

@app.post("/ingest/radio", tags=["ingestion"])
async def ingest_radio_data(
    payload: RadioIngestionPayload,
    auth: bool = Depends(verify_ingest_token)
):
    """
    Ingest radio data from Ugandan radio stations.
    
    Requires: Bearer token with ingestion permissions.
    """
    try:
        result = data_store.ingest_radio_data(payload)
        
        # Additional radio-specific processing
        radio_metadata = {
            "station": payload.station,
            "frequency": payload.frequency,
            "ingestion_type": "radio",
            "processing_time": datetime.utcnow().isoformat()
        }
        
        result["metadata"] = radio_metadata
        
        logger.info(f"Radio ingestion successful from {payload.station}: {result['message']}")
        return result
        
    except Exception as e:
        logger.error(f"Radio ingestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Radio ingestion failed: {str(e)}"
        )

# =========================
# Admin Endpoints (Admin Only)
# =========================

@app.post("/admin/week/lock", tags=["admin"])
async def lock_current_week(auth: bool = Depends(verify_admin_token)):
    """Lock the current week (admin only)"""
    try:
        week_id = week_authority.current_week.week_id
        success = week_authority.lock_week(week_id)
        
        if success:
            return {
                "status": "success",
                "message": f"Week {week_id} locked successfully",
                "week_id": week_id,
                "locked_at": datetime.utcnow().isoformat(),
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to lock week {week_id}"
            )
            
    except Exception as e:
        logger.error(f"Week lock failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Week lock failed: {str(e)}"
        )

@app.post("/admin/week/publish", tags=["admin"])
async def publish_current_week(auth: bool = Depends(verify_admin_token)):
    """Publish the current week (admin only)"""
    try:
        week_id = week_authority.current_week.week_id
        success = week_authority.publish_week(week_id)
        
        if success:
            return {
                "status": "success",
                "message": f"Week {week_id} published successfully",
                "week_id": week_id,
                "published_at": datetime.utcnow().isoformat(),
                "timestamp": datetime.utcnow().isoformat(),
                "note": "Week is now immutable and cannot be modified"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to publish week {week_id}"
            )
            
    except Exception as e:
        logger.error(f"Week publish failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Week publish failed: {str(e)}"
        )

@app.get("/admin/weeks", tags=["admin"])
async def list_all_weeks(auth: bool = Depends(verify_admin_token)):
    """List all weeks (admin only)"""
    try:
        weeks_dir = DATA_DIR / "weeks"
        weeks = []
        
        for week_file in weeks_dir.glob("*.json"):
            try:
                with open(week_file, 'r') as f:
                    week_data = json.load(f)
                weeks.append(week_data)
            except:
                continue
        
        # Sort by week_id descending
        weeks.sort(key=lambda x: x.get("week_id", ""), reverse=True)
        
        return {
            "total_weeks": len(weeks),
            "weeks": weeks,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to list weeks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list weeks: {str(e)}"
        )

# =========================
# Error Handlers
# =========================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Canonical HTTP exception handler"""
    error_id = str(uuid.uuid4())
    
    logger.error(f"HTTP error {exc.status_code}: {exc.detail} (ID: {error_id})")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "error_id": error_id,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "method": request.method,
            "canonical_error": True
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Canonical general exception handler"""
    error_id = str(uuid.uuid4())
    
    logger.error(f"Unhandled exception: {exc} (ID: {error_id})", exc_info=True)
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "status_code": 500,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "method": request.method,
            "canonical_error": True,
            "note": "Error has been logged with ID for investigation"
        }
    )

# =========================
# Application Startup Event
# =========================

@app.on_event("startup")
async def startup_event():
    """Canonical startup procedures"""
    logger.info("=" * 60)
    logger.info(f"Starting UG Board Engine v1.0.0")
    logger.info(f"Environment: {ENVIRONMENT}")
    logger.info(f"Data directory: {DATA_DIR.absolute()}")
    logger.info(f"Current week: {week_authority.current_week.week_id}")
    logger.info(f"Week status: {week_authority.current_week.status}")
    logger.info("=" * 60)
    
    # Validate critical configuration
    if ENVIRONMENT == "production":
        if ADMIN_TOKEN == "admin-ug-board-2026":
            logger.warning("⚠️  Using default admin token in production!")
        if INGEST_TOKEN == "ingest-ug-board-2026":
            logger.warning("⚠️  Using default ingest token in production!")
    
    # Create startup marker
    startup_file = DATA_DIR / "state" / "startup.log"
    with open(startup_file, 'a') as f:
        f.write(f"{datetime.utcnow().isoformat()} - Started\n")

# =========================
# Application Shutdown Event
# =========================

@app.on_event("shutdown")
async def shutdown_event():
    """Canonical shutdown procedures"""
    logger.info("Shutting down UG Board Engine")
    
    # Create shutdown marker
    shutdown_file = DATA_DIR / "state" / "shutdown.log"
    with open(shutdown_file, 'a') as f:
        f.write(f"{datetime.utcnow().isoformat()} - Shutdown\n")

# =========================
# Main Entry Point
# =========================

if __name__ == "__main__":
    print("=" * 60)
    print("UG Board Engine - Canonical Implementation")
    print("=" * 60)
    print(f"📊 Service: UG Board Engine v1.0.0")
    print(f"🌍 Environment: {ENVIRONMENT}")
    print(f"📁 Data directory: {DATA_DIR.absolute()}")
    print(f"📅 Current week: {week_authority.current_week.week_id}")
    print(f"🔒 Week status: {week_authority.current_week.status}")
    print(f"🚀 Starting on port {PORT}")
    print("=" * 60)
    print("📚 API Documentation: http://localhost:{PORT}/docs")
    print("📺 TV Ingestion: POST /ingest/tv (Bearer token required)")
    print("📻 Radio Ingestion: POST /ingest/radio (Bearer token required)")
    print("📊 Charts: GET /charts/top100")
    print("⚙️  Admin: Various endpoints (Admin token required)")
    print("=" * 60)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True
    )
