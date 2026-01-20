"""
UG Board Engine - Complete Production API with Your Swagger Structure
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query, Path as FPath, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
import json

# ====== CONFIGURE LOGGING ======
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ====== SECURITY TOKENS ======
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)  # Use ingest token for YouTube

security = HTTPBearer()

# ====== GLOBAL STATE ======
app_start_time = datetime.utcnow()
request_count = 0
current_chart_week = datetime.utcnow().strftime("%Y-W%W")

# ====== MODELS ======
class SongItem(BaseModel):
    title: str
    artist: str
    plays: int = Field(0, ge=0)
    score: float = Field(0.0, ge=0.0, le=100.0)
    station: Optional[str] = None
    region: str = Field("ug", regex="^(ug|ke|tz|rw)$")
    timestamp: Optional[str] = None
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        if v:
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError('Invalid ISO 8601 timestamp')
        return v

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

class YouTubeIngestPayload(IngestPayload):
    channel_id: Optional[str] = None
    video_id: Optional[str] = None
    category: Optional[str] = "music"

# ====== DATABASE (In-memory with file backup) ======
class Database:
    def __init__(self):
        self.songs = []
        self.chart_history = []
        self.regions = {
            "ug": {"name": "Uganda", "songs": []},
            "ke": {"name": "Kenya", "songs": []},
            "tz": {"name": "Tanzania", "songs": []},
            "rw": {"name": "Rwanda", "songs": []}
        }
        self.trending_songs = []
        self.load_from_disk()
        
    def load_from_disk(self):
        """Load data from JSON files"""
        try:
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            # Load songs
            songs_file = data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                logger.info(f"Loaded {len(self.songs)} songs from disk")
                
            # Load chart history
            history_file = data_dir / "chart_history.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    self.chart_history = json.load(f)
                    
            # Load regions
            regions_file = data_dir / "regions.json"
            if regions_file.exists():
                with open(regions_file, 'r') as f:
                    self.regions = json.load(f)
                    
        except Exception as e:
            logger.error(f"Failed to load data from disk: {e}")
            
    def save_to_disk(self):
        """Save data to JSON files"""
        try:
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            # Save songs
            with open(data_dir / "songs.json", 'w') as f:
                json.dump(self.songs, f, indent=2)
                
            # Save chart history
            with open(data_dir / "chart_history.json", 'w') as f:
                json.dump(self.chart_history, f, indent=2)
                
            # Save regions
            with open(data_dir / "regions.json", 'w') as f:
                json.dump(self.regions, f, indent=2)
                
            logger.info("Data saved to disk")
        except Exception as e:
            logger.error(f"Failed to save data to disk: {e}")
    
    def add_songs(self, songs: List[SongItem], source: str):
        for song in songs:
            song_dict = song.dict()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + 1}"
            self.songs.append(song_dict)
            
            # Add to region
            region = song.region.lower()
            if region in self.regions:
                self.regions[region]["songs"].append(song_dict)
                
        self.save_to_disk()
        
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None):
        if region and region in self.regions:
            songs = self.regions[region]["songs"]
        else:
            songs = self.songs
            
        sorted_songs = sorted(songs, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_songs[:limit]
    
    def get_trending_songs(self, limit: int = 10):
        """Get trending songs (songs with recent activity)"""
        if not self.songs:
            return []
            
        # Simple trending algorithm: recent songs with high score
        recent_songs = [s for s in self.songs 
                       if datetime.fromisoformat(s.get("ingested_at", "2024-01-01").replace('Z', '+00:00')) 
                       > datetime.utcnow() - timedelta(hours=24)]
        
        sorted_trending = sorted(recent_songs, 
                               key=lambda x: (x.get("score", 0) * 0.7 + 
                                             x.get("plays", 0) * 0.3), 
                               reverse=True)
        return sorted_trending[:limit]
    
    def publish_weekly_chart(self):
        """Publish weekly chart and rotate"""
        global current_chart_week
        
        # Create weekly snapshot
        weekly_snapshot = {
            "week": current_chart_week,
            "published_at": datetime.utcnow().isoformat(),
            "top100": self.get_top_songs(100),
            "regions": {
                region: self.get_top_songs(5, region)
                for region in self.regions.keys()
            }
        }
        
        # Add to history
        self.chart_history.append(weekly_snapshot)
        
        # Keep only last 52 weeks (1 year)
        if len(self.chart_history) > 52:
            self.chart_history = self.chart_history[-52:]
            
        # Update chart week
        current_chart_week = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-W%W")
        
        self.save_to_disk()
        return weekly_snapshot

db = Database()

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("🚀 UG Board Engine starting up...")
    logger.info(f"📅 Current chart week: {current_chart_week}")
    logger.info(f"🗄️  Loaded {len(db.songs)} songs from database")
    
    # Initialize YouTube worker integration
    worker_url = "https://ugboard-youtube-puller.kimkp015.workers.dev"
    logger.info(f"🔗 YouTube Worker: {worker_url}")
    
    yield
    
    # Shutdown
    logger.info("🛑 UG Board Engine shutting down...")
    db.save_to_disk()

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine",
    version="8.0.0",
    description="Official Ugandan Music Chart System with YouTube Worker Integration",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Root", "description": "Root endpoint"},
        {"name": "Charts", "description": "Music chart endpoints"},
        {"name": "Regions", "description": "Regional chart data"},
        {"name": "Trending", "description": "Trending songs"},
        {"name": "Ingestion", "description": "Data ingestion endpoints"},
        {"name": "Admin", "description": "Administrative functions"},
        {"name": "Worker", "description": "YouTube Worker integration"}
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== AUTHENTICATION ======
def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")
    return True

def verify_ingest(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != INGEST_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    return True

def verify_youtube(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != YOUTUBE_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid YouTube ingestion token")
    return True

def verify_internal(x_internal_token: Optional[str] = Header(None)):
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    return True

# ====== API ENDPOINTS ======

# === ROOT ===
@app.get("/", tags=["Root"], summary="Root endpoint")
async def root():
    """Root endpoint with service information"""
    global request_count
    request_count += 1
    
    return {
        "service": "UG Board Engine",
        "version": "8.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "charts": {
                "top100": "/charts/top100",
                "index": "/charts/index",
                "regions": "/charts/regions/{region}",
                "trending": "/charts/trending"
            },
            "ingestion": {
                "youtube": "/ingest/youtube",
                "radio": "/ingest/radio",
                "tv": "/ingest/tv"
            },
            "admin": {
                "health": "/admin/health",
                "publish": "/admin/publish/weekly",
                "index": "/admin/index"
            }
        }
    }

@app.get("/health", tags=["Root"], summary="Health check")
async def health():
    """Health check endpoint"""
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
        "chart_week": current_chart_week,
        "worker_integration": "active"
    }

# === CHARTS ===
@app.get("/charts/top100", 
         tags=["Charts"], 
         summary="Uganda Top 100 (current week)")
async def get_top100(
    limit: int = Query(100, ge=1, le=200, description="Number of songs to return")
):
    """Get Uganda Top 100 chart for current week"""
    songs = db.get_top_songs(limit)
    
    for i, song in enumerate(songs, 1):
        song["rank"] = i
        song["change"] = "new"  # Simple change indicator
    
    return {
        "chart": "Uganda Top 100",
        "week": current_chart_week,
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/index", 
         tags=["Charts"], 
         summary="Public chart publish index")
async def get_chart_index():
    """Get chart publication index"""
    return {
        "current_week": current_chart_week,
        "available_weeks": [h["week"] for h in db.chart_history[-10:]],
        "history_count": len(db.chart_history),
        "last_published": db.chart_history[-1]["published_at"] if db.chart_history else None
    }

@app.get("/charts/regions/{region}", 
         tags=["Charts", "Regions"], 
         summary="Get Top 5 songs per region")
async def get_region_top5(
    region: str = FPath(..., description="Region code: ug, ke, tz, rw")
):
    """Get top 5 songs for a specific region"""
    if region not in db.regions:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found")
    
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

@app.get("/charts/trending", 
         tags=["Charts", "Trending"], 
         summary="Trending songs (live)")
async def get_trending(
    limit: int = Query(10, ge=1, le=50, description="Number of trending songs")
):
    """Get currently trending songs"""
    songs = db.get_trending_songs(limit)
    
    for i, song in enumerate(songs, 1):
        song["trend_rank"] = i
        song["trend_score"] = round(song.get("score", 0) * 0.7 + song.get("plays", 0) * 0.3, 2)
    
    return {
        "chart": "Trending Now",
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat(),
        "refresh_rate": "real-time"
    }

# === INGESTION ===
@app.post("/ingest/youtube", 
          tags=["Ingestion"], 
          summary="Ingest YouTube data (validated)")
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(verify_youtube)
):
    """Ingest YouTube data from your Cloudflare Worker"""
    try:
        # Validate Ugandan content
        ugandan_artists = {"bobi wine", "eddy kenzo", "sheebah", "daddy andre", 
                          "gravity", "vyroota", "geosteady", "feffe busi"}
        
        valid_items = []
        for item in payload.items:
            artist_lower = item.artist.lower()
            is_ugandan = any(ug_artist in artist_lower for ug_artist in ugandan_artists)
            
            if is_ugandan:
                item_dict = item.dict()
                item_dict["source"] = f"youtube_{payload.source}"
                item_dict["category"] = payload.category
                if payload.channel_id:
                    item_dict["channel_id"] = payload.channel_id
                if payload.video_id:
                    item_dict["video_id"] = payload.video_id
                
                valid_items.append(item_dict)
        
        # Add to database
        db.add_songs([SongItem(**item) for item in valid_items], f"youtube_{payload.source}")
        
        logger.info(f"YouTube ingestion: {len(valid_items)} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {len(valid_items)} YouTube songs",
            "source": payload.source,
            "valid_count": len(valid_items),
            "invalid_count": len(payload.items) - len(valid_items),
            "timestamp": datetime.utcnow().isoformat(),
            "worker": "ugboard-youtube-puller.kimkp015.workers.dev"
        }
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}")
        raise HTTPException(status_code=500, detail=f"YouTube ingestion error: {str(e)}")

@app.post("/ingest/radio", 
          tags=["Ingestion"], 
          summary="Ingest Radio data (validated)")
async def ingest_radio(
    payload: IngestPayload,
    auth: bool = Depends(verify_ingest)
):
    """Ingest radio data"""
    try:
        db.add_songs(payload.items, f"radio_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {len(payload.items)} radio songs",
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Radio ingestion error: {str(e)}")

@app.post("/ingest/tv", 
          tags=["Ingestion"], 
          summary="Ingest TV data (validated)")
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(verify_ingest)
):
    """Ingest TV data"""
    try:
        db.add_songs(payload.items, f"tv_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {len(payload.items)} TV songs",
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TV ingestion error: {str(e)}")

# === ADMIN ===
@app.get("/admin/health", 
         tags=["Admin"], 
         summary="Admin health check",
         dependencies=[Depends(verify_admin)])
async def admin_health():
    """Admin health check with detailed system info"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "memory_usage": "N/A",  # Add psutil for actual metrics
            "cpu_usage": "N/A"
        },
        "database": {
            "total_songs": len(db.songs),
            "unique_artists": len(set(s.get("artist", "") for s in db.songs)),
            "regions": list(db.regions.keys()),
            "chart_history_entries": len(db.chart_history)
        },
        "integrations": {
            "youtube_worker": "active",
            "tv_scraper": "configured",
            "radio_scraper": "configured"
        }
    }

@app.post("/admin/publish/weekly", 
          tags=["Admin"], 
          summary="Publish all regions and rotate chart week",
          dependencies=[Depends(verify_admin)])
async def publish_weekly():
    """Publish weekly chart for all regions"""
    try:
        result = db.publish_weekly_chart()
        
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

@app.get("/admin/index", 
         tags=["Admin"], 
         summary="(Admin) Read-only weekly publish index",
         dependencies=[Depends(verify_admin)])
async def admin_index():
    """Admin-only detailed publish index"""
    return {
        "current_week": current_chart_week,
        "chart_history": db.chart_history[-5:],  # Last 5 weeks
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

# === WORKER INTEGRATION ===
@app.post("/worker/youtube/pull", 
          tags=["Worker"], 
          summary="Trigger YouTube worker pull",
          dependencies=[Depends(verify_internal)])
async def trigger_youtube_pull(
    channels: Optional[List[str]] = Query(None, description="Specific channels to pull")
):
    """Trigger YouTube worker to pull data (simulated)"""
    # In production, this would call your Cloudflare Worker
    return {
        "status": "triggered",
        "message": "YouTube worker pull initiated",
        "worker_url": "https://ugboard-youtube-puller.kimkp015.workers.dev",
        "channels": channels or ["all"],
        "timestamp": datetime.utcnow().isoformat(),
        "next_scheduled": "Every 30 minutes"
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.error(f"HTTP Error {exc.status_code}: {exc.detail}")
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
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    
    print(f"""
    🎵 UG Board Engine v8.0.0
    📅 Chart Week: {current_chart_week}
    🌐 URL: http://localhost:{port}
    📚 Docs: http://localhost:{port}/docs
    🔗 YouTube Worker: https://ugboard-youtube-puller.kimkp015.workers.dev
    🗄️  Database: {len(db.songs)} songs loaded
    """)
    
    uvicorn.run(app, host="0.0.0.0", port=port)
