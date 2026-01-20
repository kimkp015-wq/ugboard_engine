"""
UG Board Engine - Production Ready with Fixed Pydantic v2 Syntax
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

# ====== FIXED LOGGING SETUP ======
def setup_logging():
    """Configure logging without complex class structure"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            # File handler commented out for Render.com compatibility
            # logging.FileHandler(logs_dir / "ugboard.log")
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ====== SECURITY TOKENS ======
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
YOUTUBE_TOKEN = os.getenv("YOUTUBE_TOKEN", INGEST_TOKEN)

security = HTTPBearer()

# ====== MODELS WITH PYDANTIC V2 SYNTAX ======
class SongItem(BaseModel):
    """Song data model - FIXED: Using pattern instead of regex"""
    title: str
    artist: str
    plays: int = Field(0, ge=0)
    score: float = Field(0.0, ge=0.0, le=100.0)
    station: Optional[str] = None
    region: str = Field("ug", pattern="^(ug|ke|tz|rw)$")  # FIXED: pattern not regex
    timestamp: Optional[str] = None
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISO 8601 timestamp"""
        if v:
            try:
                # Handle Z suffix
                if v.endswith('Z'):
                    v = v[:-1] + '+00:00'
                datetime.fromisoformat(v)
            except ValueError:
                raise ValueError('Invalid ISO 8601 timestamp')
        return v

class IngestPayload(BaseModel):
    """Base ingestion payload"""
    items: List[SongItem]
    source: str
    metadata: Optional[Dict[str, Any]] = None

class YouTubeIngestPayload(IngestPayload):
    """YouTube-specific ingestion payload"""
    channel_id: Optional[str] = None
    video_id: Optional[str] = None
    category: Optional[str] = "music"

# ====== SIMPLIFIED DATABASE ======
class Database:
    """Simplified in-memory database"""
    def __init__(self):
        self.songs = []
        self.regions = {
            "ug": {"name": "Uganda", "songs": []},
            "ke": {"name": "Kenya", "songs": []},
            "tz": {"name": "Tanzania", "songs": []},
            "rw": {"name": "Rwanda", "songs": []}
        }
        logger.info("Database initialized")
        
    def add_songs(self, songs: List[SongItem], source: str):
        """Add songs to database"""
        for song in songs:
            song_dict = song.model_dump()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + 1}"
            self.songs.append(song_dict)
            
            # Add to region
            region = song.region.lower()
            if region in self.regions:
                self.regions[region]["songs"].append(song_dict)
        
        logger.info(f"Added {len(songs)} songs from {source}")
        
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None):
        """Get top songs sorted by score"""
        if region and region in self.regions:
            songs = self.regions[region]["songs"]
        else:
            songs = self.songs
            
        sorted_songs = sorted(songs, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_songs[:limit]

db = Database()

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("🚀 UG Board Engine starting up...")
    logger.info(f"📊 Database initialized with {len(db.songs)} songs")
    
    yield
    
    # Shutdown
    logger.info("🛑 UG Board Engine shutting down...")

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine",
    version="8.0.0",
    description="Official Ugandan Music Chart System",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# ====== CORS MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== AUTHENTICATION ======
def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify admin token"""
    if credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")
    return True

def verify_ingest(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify ingestion token"""
    if credentials.credentials != INGEST_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid ingestion token")
    return True

def verify_youtube(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify YouTube ingestion token"""
    if credentials.credentials != YOUTUBE_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid YouTube ingestion token")
    return True

# ====== GLOBAL STATE ======
app_start_time = datetime.utcnow()
request_count = 0

# ====== API ENDPOINTS ======
@app.get("/")
async def root():
    """Root endpoint"""
    global request_count
    request_count += 1
    
    return {
        "service": "UG Board Engine",
        "version": "8.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": int(uptime.total_seconds()),
        "database_songs": len(db.songs),
        "request_count": request_count
    }

@app.get("/charts/top100")
async def get_top100(limit: int = Query(100, ge=1, le=200)):
    """Get Uganda Top 100 chart"""
    songs = db.get_top_songs(limit)
    
    # Add ranks
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100",
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/charts/regions/{region}")
async def get_region_top5(region: str = FPath(..., description="Region code: ug, ke, tz, rw")):
    """Get top songs for a specific region"""
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
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/youtube")
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(verify_youtube)
):
    """Ingest YouTube data"""
    try:
        db.add_songs(payload.items, f"youtube_{payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {len(payload.items)} YouTube songs",
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"YouTube ingestion error: {str(e)}")

@app.post("/ingest/tv")
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

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    """Admin status endpoint"""
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "total_songs": len(db.songs),
        "request_count": request_count
    }

# ====== ERROR HANDLER ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
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
    🌐 URL: http://localhost:{port}
    📚 Docs: http://localhost:{port}/docs
    🗄️  Database: {len(db.songs)} songs loaded
    """)
    
    uvicorn.run(app, host="0.0.0.0", port=port)
