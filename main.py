"""
UG Board Engine - WITH TV INGESTION (CLEAN VERSION)
Primary focus: Ugandan music and artists
Foreign artists only allowed in collaborations with Ugandan artists
"""

import os
import json
import uuid
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum

from fastapi import FastAPI, HTTPException, Header, Body, Path, Query, status
from fastapi.responses import JSONResponse

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
        "kadongo kamu", "kidandali", "afrobeat", "dancehall", "reggea",
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
# Validation Functions
# =========================

def validate_song_item(item: Dict) -> Dict:
    """Validate song item with Ugandan music rules"""
    required = ["title", "artist"]
    for field in required:
        if field not in item:
            raise ValueError(f"Missing required field: {field}")
    
    # Extract artists and validate Ugandan music rules
    artists = MusicRules.extract_artist_list(item["artist"])
    is_valid, error_msg = MusicRules.validate_artists(artists)
    if not is_valid:
        raise ValueError(f"Artist validation failed: {error_msg}")
    
    # Add artist metadata
    item["artist_metadata"] = {
        "artists_list": artists,
        "artist_types": [MusicRules.get_artist_type(a) for a in artists],
        "is_collaboration": len(artists) > 1,
        "has_ugandan_artist": any(MusicRules.is_ugandan_artist(a) for a in artists),
        "has_foreign_artist": any(not MusicRules.is_ugandan_artist(a) for a in artists)
    }
    
    # Add defaults
    item.setdefault("id", str(uuid.uuid4())[:8])
    item.setdefault("score", 0.0)
    item.setdefault("plays", 0)
    item.setdefault("change", "same")
    item.setdefault("genre", "afrobeat")
    item.setdefault("region", "ug")
    item.setdefault("release_date", datetime.utcnow().date().isoformat())
    
    return item

def validate_ingestion_payload(payload: Dict) -> Dict:
    """Validate ingestion payload with Ugandan music rules"""
    if "items" not in payload:
        raise ValueError("Missing 'items' field")
    
    if not isinstance(payload["items"], list):
        raise ValueError("'items' must be a list")
    
    if len(payload["items"]) == 0:
        raise ValueError("'items' list cannot be empty")
    
    # Validate each song item
    for i, item in enumerate(payload["items"]):
        try:
            payload["items"][i] = validate_song_item(item)
        except ValueError as e:
            raise ValueError(f"Item {i} validation failed: {e}")
    
    payload.setdefault("timestamp", datetime.utcnow().isoformat())
    payload.setdefault("metadata", {})
    payload.setdefault("source", "unknown")
    
    return payload

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
# Data Store with Ugandan Music Data
# =========================

class UgandanMusicStore:
    """Ugandan-focused music data store"""
    
    def __init__(self):
        # Real Ugandan songs with artists
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
        
        # Initialize charts
        self.charts = {}
        self._init_charts()
        
        # Other data structures
        self.ingestion_log = []
    
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

# Initialize data store
data_store = UgandanMusicStore()

# =========================
# Create FastAPI App
# =========================

app = FastAPI(
    title="UG Board Engine",
    description="Automated Ugandan music chart system",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# =========================
# Default Category
# =========================

@app.get("/")
async def root():
    """Public engine health check - Ugandan Music Focus"""
    return {
        "service": "UG Board Engine - Ugandan Music",
        "version": "1.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "focus": "Ugandan music and artists",
        "rule": "Foreign artists only allowed in collaborations with Ugandan artists",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "uganda_top100": "/charts/top100",
            "regional_charts": "/charts/regions/{region}",
            "tv_ingestion": "/ingest/tv",
            "radio_ingestion": "/ingest/radio"
        }
    }

# =========================
# Health Category
# =========================

@app.get("/health")
async def health():
    """Public health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "focus": "Ugandan music charting"
    }

# =========================
# Charts Category
# =========================

@app.get("/charts/top100")
async def get_top100():
    """Uganda Top 100 (current week) - Ugandan music only"""
    chart_data = data_store.get_top100()
    
    collaborations = sum(1 for song in chart_data 
                        if song.get("artist_metadata", {}).get("is_collaboration", False))
    foreign_involved = sum(1 for song in chart_data 
                          if song.get("artist_metadata", {}).get("has_foreign_artist", False))
    
    return {
        "region": "ug",
        "chart_name": "Uganda Top 100 - Ugandan Music",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "published": datetime.utcnow().isoformat(),
        "entries": chart_data[:10],  # Return only first 10 for brevity
        "total_entries": 100,
        "statistics": {
            "collaborations": collaborations,
            "foreign_involved": foreign_involved,
            "pure_ugandan": 100 - foreign_involved,
            "collaboration_rate": f"{(collaborations / 100) * 100:.1f}%"
        },
        "instance": INSTANCE_ID,
        "rule_enforced": "Foreign artists only in collaborations with Ugandan artists"
    }

@app.get("/charts/regions/{region}")
async def get_region_chart(region: Region):
    """Get regional chart for Ugandan music"""
    chart_data = data_store.get_region_top5(region.value)
    
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
        "instance": INSTANCE_ID,
        "timestamp": datetime.utcnow().isoformat(),
        "note": "All charts feature Ugandan artists or collaborations with Ugandan artists"
    }

# =========================
# Ingestion Category - TV ENDPOINT
# =========================

@app.post("/ingest/tv")
async def ingest_tv(
    payload: Dict = Body(...),
    x_internal_token: Optional[str] = Header(None)
):
    """Ingest TV data with Ugandan artist validation"""
    # Verify internal token
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    
    try:
        # Validate the entire payload
        validated = validate_ingestion_payload(payload)
        
        # TV-specific validation - require channel field
        for item in validated["items"]:
            if "channel" not in item:
                raise ValueError("TV items must include 'channel' field")
        
        # Ugandan music validation
        validation_report = {
            "total_items": len(validated["items"]),
            "valid_items": 0,
            "artist_breakdown": {
                "ugandan_only": 0,
                "collaborations": 0,
                "invalid_foreign": 0
            }
        }
        
        valid_items = []
        for i, item in enumerate(validated["items"]):
            artists = item.get("artist_metadata", {}).get("artists_list", [])
            has_ugandan = any(MusicRules.is_ugandan_artist(a) for a in artists)
            has_foreign = any(not MusicRules.is_ugandan_artist(a) for a in artists)
            
            if has_ugandan:
                validation_report["valid_items"] += 1
                valid_items.append(item)
                
                if has_foreign:
                    validation_report["artist_breakdown"]["collaborations"] += 1
                else:
                    validation_report["artist_breakdown"]["ugandan_only"] += 1
            else:
                validation_report["artist_breakdown"]["invalid_foreign"] += 1
        
        # Log the ingestion
        log_entry = data_store.log_ingestion("tv", len(valid_items), valid_items)
        
        return {
            "status": "success",
            "message": f"Ingested {len(valid_items)} TV items with Ugandan artist validation",
            "source": "tv",
            "items_processed": len(valid_items),
            "validation_passed": True,
            "validation_report": validation_report,
            "timestamp": datetime.utcnow().isoformat(),
            "log_entry": log_entry,
            "instance": INSTANCE_ID
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )
"endpoints": {
    "tv_ingestion": "/ingest/tv",
    "tv_scraper_status": "https://github.com/your-org/ugboard_engine/actions",
    # ... rest remains the same
}
# =========================
# Ingestion Category - Radio ENDPOINT
# =========================

@app.post("/ingest/radio")
async def ingest_radio(
    payload: Dict = Body(...),
    x_internal_token: Optional[str] = Header(None)
):
    """Ingest Radio data with Ugandan artist validation"""
    # Verify internal token
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    
    try:
        # Validate the entire payload
        validated = validate_ingestion_payload(payload)
        
        # Radio-specific validation - require station field
        for item in validated["items"]:
            if "station" not in item:
                raise ValueError("Radio items must include 'station' field")
        
        # Ugandan music validation
        validation_report = {
            "total_items": len(validated["items"]),
            "valid_items": 0,
            "artist_breakdown": {
                "ugandan_only": 0,
                "collaborations": 0,
                "invalid_foreign": 0
            }
        }
        
        valid_items = []
        for i, item in enumerate(validated["items"]):
            artists = item.get("artist_metadata", {}).get("artists_list", [])
            has_ugandan = any(MusicRules.is_ugandan_artist(a) for a in artists)
            has_foreign = any(not MusicRules.is_ugandan_artist(a) for a in artists)
            
            if has_ugandan:
                validation_report["valid_items"] += 1
                valid_items.append(item)
                
                if has_foreign:
                    validation_report["artist_breakdown"]["collaborations"] += 1
                else:
                    validation_report["artist_breakdown"]["ugandan_only"] += 1
            else:
                validation_report["artist_breakdown"]["invalid_foreign"] += 1
        
        # Log the ingestion
        log_entry = data_store.log_ingestion("radio", len(valid_items), valid_items)
        
        return {
            "status": "success",
            "message": f"Ingested {len(valid_items)} radio items with Ugandan artist validation",
            "source": "radio",
            "items_processed": len(valid_items),
            "validation_passed": True,
            "validation_report": validation_report,
            "timestamp": datetime.utcnow().isoformat(),
            "log_entry": log_entry,
            "instance": INSTANCE_ID
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

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
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "instance": INSTANCE_ID,
            "request_id": str(uuid.uuid4())
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    error_id = str(uuid.uuid4())
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "instance": INSTANCE_ID,
            "request_id": error_id
        }
    )

# =========================
# Server Startup
# =========================

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Starting UG Board Engine on port {PORT}")
    print(f"🌐 Service: {SERVICE_NAME}")
    print(f"📺 TV Ingestion: POST /ingest/tv")
    print(f"📻 Radio Ingestion: POST /ingest/radio")
    print(f"📊 Charts: GET /charts/top100")
    print(f"📚 Docs: http://localhost:{PORT}/docs")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
