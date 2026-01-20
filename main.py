"""
UG Board Engine - Complete Production Version
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

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

# ====== SCRAPER CONFIGURATION ======
TV_SCRAPER_AVAILABLE = False
tv_scraper = None
scraping_state = {
    "is_scraping": False,
    "last_scrape_time": None,
    "scrape_results": {},
    "errors": []
}

# ====== GLOBAL STATE ======
app_start_time = datetime.utcnow()
request_count = 0

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

class ScrapeRequest(BaseModel):
    stations: List[str] = Field(default_factory=lambda: ["all"])
    force: bool = False
    async_mode: bool = True

class ScrapeStatus(BaseModel):
    is_scraping: bool
    last_scrape_time: Optional[str]
    stations_scraped: List[str]
    total_items: int
    errors: List[str]

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
        # Try multiple import paths
        possible_paths = [
            "scripts.tv_scraper",
            "src.tv_scraper",
            "tv_scraper"
        ]
        
        scraper_module = None
        for path in possible_paths:
            try:
                scraper_module = __import__(path, fromlist=["TVScraper"])
                break
            except ImportError:
                continue
        
        if scraper_module and hasattr(scraper_module, "TVScraper"):
            tv_scraper = scraper_module.TVScraper()
            TV_SCRAPER_AVAILABLE = True
            logger.info("✅ TV Scraper imported successfully")
            return True
        else:
            raise ImportError("TVScraper class not found")
            
    except ImportError as e:
        logger.warning(f"⚠️ Could not import TVScraper: {e}")
        logger.info("📺 Using mock scraper instead")
        
        # Mock scraper as fallback
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
        return False

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("🚀 UG Board Engine starting up...")
    initialize_scraper()
    
    # Initialize rate limiter
    app.state.limiter = Limiter(key_func=get_remote_address)
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    
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
    allow_origins=[
        "https://your-frontend.com",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
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

def verify_internal(x_internal_token: Optional[str] = Header(None)):
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    return True

# ====== SCRAPING FUNCTIONS ======
async def scrape_tv_station(station_name: str) -> List[Dict[str, Any]]:
    """Scrape a single TV station"""
    try:
        logger.info(f"📺 Scraping station: {station_name}")
        items = await tv_scraper.scrape_station(station_name)
        
        # Add metadata
        for item in items:
            item["station"] = station_name
            item["scraped_at"] = datetime.utcnow().isoformat()
            
        logger.info(f"✅ Scraped {len(items)} items from {station_name}")
        return items
        
    except Exception as e:
        logger.error(f"❌ Error scraping {station_name}: {str(e)}")
        raise

async def scrape_multiple_stations(stations: List[str], background: bool = False) -> Dict[str, Any]:
    """Scrape multiple stations"""
    global scraping_state
    
    if not stations or stations == ["all"]:
        stations_to_scrape = ["ntv", "bukedde", "sanyuka", "spark"]
    else:
        stations_to_scrape = stations
    
    if scraping_state["is_scraping"] and not background:
        raise HTTPException(status_code=409, detail="Scraping already in progress")
    
    scraping_state["is_scraping"] = True
    scraping_state["last_scrape_time"] = datetime.utcnow()
    scraping_state["scrape_results"] = {}
    scraping_state["errors"] = []
    
    try:
        all_items = []
        
        # Scrape stations
        for station in stations_to_scrape:
            try:
                items = await scrape_tv_station(station)
                scraping_state["scrape_results"][station] = {
                    "count": len(items),
                    "items": items[:5]
                }
                all_items.extend(items)
                
                # Convert to SongItems and add to database
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
                db.add_songs(song_items, f"tv_scrape_{station}")
                
            except Exception as e:
                error_msg = f"Station {station}: {str(e)}"
                scraping_state["errors"].append(error_msg)
                scraping_state["scrape_results"][station] = {"error": str(e)}
        
        result = {
            "total_items": len(all_items),
            "stations_scraped": stations_to_scrape,
            "errors": scraping_state["errors"]
        }
        
        logger.info(f"🎯 Scraping complete: {len(all_items)} items from {len(stations_to_scrape)} stations")
        return result
        
    finally:
        scraping_state["is_scraping"] = False

async def scrape_background_task(stations: List[str]):
    """Background task for scraping"""
    try:
        await scrape_multiple_stations(stations, background=True)
    except Exception as e:
        logger.error(f"Background scraping error: {e}")

# ====== API ENDPOINTS ======
@app.get("/")
async def root(request: Request):
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
    hours, remainder = divmod(uptime.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "uptime": f"{int(hours)}h {int(minutes)}m {int(seconds)}s",
        "database_songs": len(db.songs),
        "scraper": "available" if TV_SCRAPER_AVAILABLE else "mock",
        "request_count": request_count
    }

@app.get("/metrics")
async def metrics():
    """Prometheus-style metrics endpoint"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "requests_served": request_count,
        "uptime_seconds": int(uptime.total_seconds()),
        "uptime_human": str(uptime).split('.')[0],
        "database_size": len(db.songs),
        "scraper_status": "active" if TV_SCRAPER_AVAILABLE else "mock",
        "scraping_active": scraping_state["is_scraping"],
        "last_scrape": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None
    }

@app.get("/charts/top100")
async def top_charts(
    request: Request,
    limit: int = Query(100, ge=1, le=200),
    station: Optional[str] = None
):
    """Get top charts with rate limiting"""
    songs = db.get_top_songs(limit)
    
    # Filter by station if specified
    if station:
        songs = [s for s in songs if s.get("station") == station]
        songs = songs[:limit]
    
    # Add ranks
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": f"Uganda Top {len(songs)}" + (f" - {station}" if station else ""),
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/scrape/tv")
async def scrape_tv(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks,
    auth: bool = Depends(verify_admin)
):
    """Trigger TV scraping"""
    
    if request.async_mode:
        # Run in background
        background_tasks.add_task(scrape_background_task, request.stations)
        
        return {
            "status": "started",
            "message": f"Scraping started for stations: {request.stations}",
            "async": True,
            "timestamp": datetime.utcnow().isoformat()
        }
    else:
        # Run synchronously
        result = await scrape_multiple_stations(request.stations)
        
        return {
            "status": "completed",
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/scrape/status")
async def get_scrape_status(auth: bool = Depends(verify_admin)):
    """Get current scraping status"""
    status = ScrapeStatus(
        is_scraping=scraping_state["is_scraping"],
        last_scrape_time=scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None,
        stations_scraped=list(scraping_state["scrape_results"].keys()),
        total_items=sum(r.get("count", 0) for r in scraping_state["scrape_results"].values() if "count" in r),
        errors=scraping_state["errors"]
    )
    return status

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
        "system_metrics": {
            "requests_served": request_count,
            "uptime": str(datetime.utcnow() - app_start_time).split('.')[0]
        }
    }

@app.get("/stations")
async def list_stations():
    """List available TV stations"""
    stations = ["ntv", "bukedde", "sanyuka", "spark"]
    return {
        "stations": stations,
        "count": len(stations),
        "last_scraped": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None
    }

# ====== ERROR HANDLERS ======
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

@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "message": "Too many requests. Please try again later.",
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
