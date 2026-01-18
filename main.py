"""
UG Board Engine - MINIMAL WORKING VERSION for Render
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

from fastapi import FastAPI, HTTPException, Header, Body, Path, Query, Depends, status
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

# =========================
# Configuration
# =========================

PORT = int(os.getenv("PORT", 8000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "ugboard-engine")
INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID", "local")[:8]

# Authentication tokens
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INJECT_TOKEN = os.getenv("INJECT_TOKEN", "inject-ug-board-2025")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# =========================
# UGANDAN MUSIC RULES & VALIDATION
# =========================

class MusicRules:
    """Rules for Ugandan music charting system"""
    
    # Primary region - Uganda is the main focus
    PRIMARY_REGION = "ug"
    
    # Known Ugandan artists database
    UGANDAN_ARTISTS = {
        # Male Artists
        "bobi wine", "joseph mayanja", "eddy kenzo", "daddy andre", "gravity omutujju",
        "fik fameica", "vyroota", "geosteady", "choppa", "feffe busi", "leyla kayondo",
        "recho rey", "vivian todi", "cindy sanyu", "niniola", "sheebah karungi",
        "spice diana", "winnie nwagi", "vinka", "zex bilangilangi", "john blaq",
        "pallaso", "navio", "gnl zamba", "rickman manrick", "buchaman", "ragga dee",
        "bebe cool", "goodlyfe", "radio & weasel",
        
        # Groups/Bands
        "ghetto kids", "team no sleep", "swangz avenue", "black skin",
        "east african party", "kadongo kamu artists", "acholi artists"
    }
    
    # Foreign artists known to collaborate with Ugandans
    FOREIGN_COLLABORATORS = {
        "davido", "wizkid", "burna boy", "tiwa savage", "yemi alade",
        "diamond platnumz", "rayvanny", "harmonize", "nandy", "juma nature",
        "khaligraph jones", "sauti sol", "nyashinski", "otile brown"
    }
    
    # Ugandan music genres
    UGANDAN_GENRES = {
        "kadongo kamu", "kidandali", "afrobeat", "dancehall", "reggae",
        "gospel", "hip hop", "rnb", "traditional", "zouk", "bongo flava"
    }
    
    @classmethod
    def is_ugandan_artist(cls, artist_name: str) -> bool:
        """Check if artist is Ugandan"""
        artist_lower = artist_name.lower().strip()
        
        # Check exact match
        if artist_lower in cls.UGANDAN_ARTISTS:
            return True
        
        # Check for Ugandan name patterns
        ugandan_patterns = [
            r'\b(kenzo|pallaso|choppa|gnl|navio|zamba)\b',
            r'\b(wine|mayanja|omutujju|busi|kayondo)\b',
            r'\b(sanyu|sheebah|diana|nwagi|vinka)\b',
            r'\b(ghetto|team no|swangz|black skin)\b'
        ]
        
        for pattern in ugandan_patterns:
            if re.search(pattern, artist_lower):
                return True
        
        return False
    
    @classmethod
    def is_known_collaborator(cls, artist_name: str) -> bool:
        """Check if artist is a known foreign collaborator"""
        return artist_name.lower().strip() in cls.FOREIGN_COLLABORATORS
    
    @classmethod
    def validate_artists(cls, artists: List[str]) -> Tuple[bool, str]:
        """
        Validate that foreign artists only appear with Ugandan collaborators
        Returns: (is_valid, error_message)
        """
        if not artists:
            return False, "No artists specified"
        
        # Check if all artists are Ugandan (allowed)
        all_ugandan = all(cls.is_ugandan_artist(artist) for artist in artists)
        if all_ugandan:
            return True, ""
        
        # Check if there's at least one Ugandan artist
        has_ugandan = any(cls.is_ugandan_artist(artist) for artist in artists)
        if not has_ugandan:
            return False, "No Ugandan artist found. Foreign artists must collaborate with Ugandan artists."
        
        # Check foreign artists are known collaborators
        foreign_artists = [a for a in artists if not cls.is_ugandan_artist(a)]
        for foreign in foreign_artists:
            if not cls.is_known_collaborator(foreign):
                return False, f"Foreign artist '{foreign}' is not in the approved collaborator list"
        
        return True, ""
    
    @classmethod
    def extract_artist_list(cls, artist_field: str) -> List[str]:
        """Extract individual artists from artist field"""
        if not artist_field:
            return []
        
        separators = [' feat. ', ' ft. ', ' & ', ' x ', ' , ', ' with ', ' and ']
        normalized = artist_field.lower()
        for sep in separators:
            normalized = normalized.replace(sep, '|')
        
        artists = [a.strip() for a in normalized.split('|') if a.strip()]
        return artists
    
    @classmethod
    def get_artist_type(cls, artist_name: str) -> str:
        """Get artist type: 'ugandan', 'foreign_collaborator', or 'unknown'"""
        if cls.is_ugandan_artist(artist_name):
            return "ugandan"
        elif cls.is_known_collaborator(artist_name):
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
# In-Memory Data Store (for initial deployment)
# =========================

class UgandanMusicStore:
    """In-memory data store for initial deployment"""
    
    def __init__(self):
        self.ugandan_songs = [
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
        
        self.charts = {}
        self.ingestion_log = []
        self._init_charts()
    
    def _init_charts(self):
        """Initialize charts with Ugandan music"""
        # Uganda Top 100
        ug_top100 = []
        for i in range(1, 101):
            song_idx = (i - 1) % len(self.ugandan_songs)
            song = self.ugandan_songs[song_idx].copy()
            
            song.update({
                "rank": i,
                "song_id": f"ug_song_{i:03d}",
                "plays": 10000 - (i * 90),
                "score": 95.5 - (i * 0.5),
                "change": "up" if i % 5 == 0 else "down" if i % 5 == 2 else "same",
                "region": "ug",
                "weeks_on_chart": min((i // 10) + 1, 52)
            })
            
            artists = MusicRules.extract_artist_list(song["artist"])
            song["artist_metadata"] = {
                "artists_list": artists,
                "artist_types": [MusicRules.get_artist_type(a) for a in artists],
                "is_collaboration": len(artists) > 1,
                "has_ugandan_artist": any(MusicRules.is_ugandan_artist(a) for a in artists),
                "has_foreign_artist": any(not MusicRules.is_ugandan_artist(a) for a in artists)
            }
            
            ug_top100.append(song)
        
        self.charts["ug_top100"] = ug_top100
        
        # Regional charts
        for region in ["eac", "afr", "ww"]:
            region_data = []
            for i in range(1, 6):
                song_idx = (i - 1) % len(self.ugandan_songs)
                song = self.ugandan_songs[song_idx].copy()
                
                song.update({
                    "rank": i,
                    "song_id": f"{region}_song_{i:02d}",
                    "plays": 5000 - (i * 800),
                    "score": 90.0 - (i * 5),
                    "change": "new" if i == 1 else "up" if i % 2 == 0 else "same",
                    "region": region,
                    "weeks_on_chart": min((i // 2) + 1, 26)
                })
                
                region_data.append(song)
            
            self.charts[f"{region}_top5"] = region_data
    
    def get_top100(self):
        return self.charts.get("ug_top100", [])
    
    def get_region_top5(self, region: str):
        return self.charts.get(f"{region}_top5", [])
    
    def get_trending(self, limit: int = 20):
        """Get trending Ugandan songs"""
        trending = []
        for i in range(1, min(limit, len(self.ugandan_songs)) + 1):
            song = self.ugandan_songs[i % len(self.ugandan_songs)].copy()
            
            song.update({
                "id": f"trend_{i:02d}",
                "velocity": 85 + (i * 2),
                "trend_score": 75 + (i * 3),
                "source": "youtube" if i % 2 == 0 else "radio",
                "region": ["ug", "eac", "afr"][i % 3],
                "trend_change": f"+{i * 5}%",
                "social_mentions": 1000 + (i * 200)
            })
            
            trending.append(song)
        
        return trending
    
    def log_ingestion(self, source: str, count: int, items: List[Dict] = None):
        """Log ingestion with artist validation stats"""
        ugandan_count = 0
        foreign_count = 0
        collaboration_count = 0
        
        if items:
            for item in items:
                artists = MusicRules.extract_artist_list(item.get("artist", ""))
                if any(MusicRules.is_ugandan_artist(a) for a in artists):
                    ugandan_count += 1
                if any(not MusicRules.is_ugandan_artist(a) for a in artists):
                    foreign_count += 1
                if len(artists) > 1:
                    collaboration_count += 1
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "count": count,
            "ugandan_artists": ugandan_count,
            "foreign_artists": foreign_count,
            "collaborations": collaboration_count,
            "instance": INSTANCE_ID
        }
        
        self.ingestion_log.append(log_entry)
        return log_entry
    
    def get_artist_stats(self):
        """Get statistics about Ugandan artists"""
        all_artists = set()
        ugandan_artists = set()
        foreign_collaborators = set()
        
        for song in self.ugandan_songs:
            artists = MusicRules.extract_artist_list(song["artist"])
            all_artists.update(artists)
            for artist in artists:
                if MusicRules.is_ugandan_artist(artist):
                    ugandan_artists.add(artist)
                elif MusicRules.is_known_collaborator(artist):
                    foreign_collaborators.add(artist)
        
        return {
            "total_unique_artists": len(all_artists),
            "ugandan_artists": len(ugandan_artists),
            "foreign_collaborators": len(foreign_collaborators),
            "collaboration_rate": f"{(len([s for s in self.ugandan_songs if len(MusicRules.extract_artist_list(s['artist'])) > 1]) / len(self.ugandan_songs)) * 100:.1f}%"
        }


# Initialize data store
data_store = UgandanMusicStore()

# =========================
# FastAPI App
# =========================

app = FastAPI(
    title="UG Board Engine",
    description="Automated Ugandan music chart system",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# =========================
# Authentication
# =========================

def verify_admin(authorization: Optional[str] = Header(None)):
    if not authorization or authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid admin token")
    return True

def verify_ingestion(authorization: Optional[str] = Header(None)):
    if not authorization or authorization != f"Bearer {INJECT_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    return True

def verify_internal(x_internal_token: Optional[str] = Header(None)):
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    return True

# =========================
# Endpoints
# =========================

@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "version": "1.0.0",
        "status": "online",
        "focus": "Ugandan music and artists"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/charts/top100")
async def get_top100():
    chart_data = data_store.get_top100()
    return {
        "region": "ug",
        "chart_name": "Uganda Top 100",
        "entries": chart_data,
        "total_entries": 100
    }

@app.get("/charts/regions/{region}")
async def get_region_chart(region: Region):
    chart_data = data_store.get_region_top5(region.value)
    return {
        "region": region.value,
        "entries": chart_data,
        "total_entries": len(chart_data)
    }

@app.post("/ingest/radio")
async def ingest_radio(
    payload: Dict = Body(...),
    x_internal_token: Optional[str] = Header(None)
):
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    
    # Basic validation
    if "items" not in payload or not isinstance(payload["items"], list):
        raise HTTPException(status_code=400, detail="Invalid payload")
    
    log_entry = data_store.log_ingestion("radio", len(payload.get("items", [])))
    
    return {
        "status": "success",
        "message": f"Received {len(payload.get('items', []))} items",
        "log_entry": log_entry
    }

# =========================
# Error Handling
# =========================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# =========================
# Server Startup
# =========================

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Starting UG Board Engine on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
