"""
UG Board Engine - Production Ready (No Rate Limiting)
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

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

security = HTTPBearer()

# ====== GLOBAL STATE ======
app_start_time = datetime.utcnow()
request_count = 0
TV_SCRAPER_AVAILABLE = False
tv_scraper = None
scraping_state = {
    "is_scraping": False,
    "last_scrape_time": None,
    "scrape_results": {},
    "errors": []
}

# ====== MODELS ======
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    station: Optional[str] = None
    timestamp: Optional[str] = None

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str

# ====== DATABASE ======
class Database:
    def __init__(self):
        self.songs = []
        
    def add_songs(self, songs: List[SongItem], source: str):
        for song in songs:
            song_dict = song.dict()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + 1}"
            self.songs.append(song_dict)
            
    def get_top_songs(self, limit: int = 100):
        sorted_songs = sorted(self.songs, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_songs[:limit]

db = Database()

# ====== SCRAPER IMPORT ======
def initialize_scraper():
    """Initialize TV scraper with fallback"""
    global tv_scraper, TV_SCRAPER_AVAILABLE
    
    try:
        # Add scripts directory to path
        scripts_path = Path(__file__).parent / "scripts"
        if scripts_path.exists():
            sys.path.insert(0, str(scripts_path))
        
        from tv_scraper import TVScraper
        tv_scraper = TVScraper()
        TV_SCRAPER_AVAILABLE = True
        logger.info("✅ TV Scraper imported successfully")
        
    except ImportError as e:
        logger.warning(f"⚠️ Could not import TVScraper: {e}")
        
        # Mock scraper
        class MockScraper:
            async def scrape_station(self, station_name: str) -> List[Dict[str, Any]]:
                logger.info(f"Mock scraping station: {station_name}")
                return [
                    {
                        "title": f"Demo Song - {station_name}",
                        "artist": "Demo Artist",
                        "plays": 100,
                        "score": 85.0,
                        "timestamp": datetime.utcnow().isoformat(),
                        "station": station_name
                    }
                ]
        
        tv_scraper = MockScraper()
        TV_SCRAPER_AVAILABLE = False

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("🚀 UG Board Engine starting up...")
    initialize_scraper()
    
    yield
    
    # Shutdown
    logger.info("🛑 UG Board Engine shutting down...")
    scraping_state["is_scraping"] = False

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine",
    version="7.0.0",
    description="Official Ugandan Music Chart System",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
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

# ====== API ENDPOINTS ======
@app.get("/")
async def root():
    """Root endpoint"""
    global request_count
    request_count += 1
    
    return {
        "service": "UG Board Engine",
        "version": "7.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "scraper_available": TV_SCRAPER_AVAILABLE,
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
        "service": "ugboard-engine.onrender.com",
        "uptime_seconds": int(uptime.total_seconds()),
        "database_songs": len(db.songs),
        "scraper": "available" if TV_SCRAPER_AVAILABLE else "mock",
        "request_count": request_count
    }

@app.get("/charts/top100")
async def top_charts(limit: int = Query(100, ge=1, le=200)):
    """Get Uganda Top 100 chart"""
    songs = db.get_top_songs(limit)
    
    # Add ranks
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": f"Uganda Top {len(songs)}",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/scrape/tv")
async def scrape_tv(
    station: str = "ntv",
    auth: bool = Depends(verify_admin)
):
    """Trigger TV scraping"""
    try:
        if scraping_state["is_scraping"]:
            raise HTTPException(status_code=409, detail="Scraping already in progress")
        
        scraping_state["is_scraping"] = True
        scraping_state["last_scrape_time"] = datetime.utcnow()
        
        # Use scraper
        items = await tv_scraper.scrape_station(station)
        
        # Convert to SongItems
        song_items = [
            SongItem(
                title=item.get("title", ""),
                artist=item.get("artist", ""),
                plays=item.get("plays", 0),
                score=item.get("score", 50.0),
                station=station,
                timestamp=item.get("timestamp")
            ) for item in items
        ]
        
        # Add to database
        db.add_songs(song_items, f"tv_scrape_{station}")
        
        scraping_state["scrape_results"] = {
            station: {
                "count": len(items),
                "items": items[:5]
            }
        }
        
        return {
            "status": "success",
            "message": f"Scraped {len(items)} songs from {station}",
            "station": station,
            "items": len(items),
            "timestamp": datetime.utcnow().isoformat(),
            "scraper_mode": "real" if TV_SCRAPER_AVAILABLE else "mock"
        }
        
    except Exception as e:
        scraping_state["errors"].append(str(e))
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")
    finally:
        scraping_state["is_scraping"] = False

@app.get("/scrape/status")
async def get_scrape_status(auth: bool = Depends(verify_admin)):
    """Get scraping status"""
    return {
        "is_scraping": scraping_state["is_scraping"],
        "last_scrape_time": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None,
        "scrape_results": scraping_state["scrape_results"],
        "errors": scraping_state["errors"]
    }

@app.post("/ingest/tv")
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(verify_ingest)
):
    """Ingest TV data"""
    try:
        db.add_songs(payload.items, payload.source)
        
        return {
            "status": "success",
            "message": f"Ingested {len(payload.items)} songs from {payload.source}",
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat(),
            "total_songs": len(db.songs)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    """Admin status endpoint"""
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "total_songs": len(db.songs),
        "scraper_available": TV_SCRAPER_AVAILABLE,
        "scraping_state": {
            "is_scraping": scraping_state["is_scraping"],
            "last_scrape": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None
        },
        "request_count": request_count
    }

# ====== ERROR HANDLER ======
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

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
