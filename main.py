"""
UG Board Engine - With TV Scraper Integration (FIXED VERSION)
"""
import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import aiohttp

# ====== FIX: GLOBAL VARIABLES ======
tv_scraper = None
TV_SCRAPER_AVAILABLE = False

# ====== FIX: PROPER SCRAPER IMPORT ======
try:
    # Try root level first (your actual file location)
    from tv_scraper import TVScraper
    TV_SCRAPER_AVAILABLE = True
    logging.info("✅ Imported TVScraper from root level")
except ImportError:
    try:
        # Try src directory
        from src.tv_scraper import TVScraper
        TV_SCRAPER_AVAILABLE = True
        logging.info("✅ Imported TVScraper from src directory")
    except ImportError:
        # Mock scraper for fallback
        class TVScraper:
            async def scrape_station(self, station_name: str) -> List[Dict[str, Any]]:
                # Mock data
                return [
                    {
                        "title": f"Demo Song from {station_name}", 
                        "artist": "Demo Artist", 
                        "plays": 100, 
                        "timestamp": datetime.utcnow().isoformat()
                    }
                ]
        TV_SCRAPER_AVAILABLE = False
        logging.warning("❌ TVScraper module not found, using mock data")

# Security tokens - YOUR ORIGINAL VALUES
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

security = HTTPBearer()

# Scraper Configuration
SCRAPER_CONFIG = {
    "stations": ["ntv", "bukedde", "sanyuka", "spark"],  # Add your actual stations
    "scrape_interval": 3600,  # 1 hour in seconds
    "max_scraping_time": 300,  # 5 minutes max per scrape
    "enable_auto_scrape": False  # Start with manual scraping first
}

# Global state for scraping
scraping_state = {
    "last_scrape_time": None,
    "is_scraping": False,
    "scrape_results": {},
    "errors": []
}

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events
    """
    # Startup
    logger.info("🚀 UG Board Engine starting up...")
    
    # Initialize scraper
    global tv_scraper
    try:
        tv_scraper = TVScraper()
        logger.info(f"✅ TV Scraper initialized successfully (Available: {TV_SCRAPER_AVAILABLE})")
    except Exception as e:
        logger.error(f"❌ Failed to initialize TV Scraper: {e}")
        tv_scraper = None
    
    yield
    
    # Shutdown
    logger.info("🛑 UG Board Engine shutting down...")
    scraping_state["is_scraping"] = False

app = FastAPI(
    title="UG Board Engine",
    version="7.0.0",
    description="UG Board Engine with TV Scraping Integration",
    lifespan=lifespan
)

# ====== MODELS ======
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    timestamp: Optional[str] = None
    station: Optional[str] = None

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str
    station: Optional[str] = None

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

# ====== DATABASE (In-memory for now) ======
class Database:
    def __init__(self):
        self.songs = []
        self.scraping_history = []
        
    def add_songs(self, songs: List[SongItem], source: str):
        for song in songs:
            song_dict = song.dict()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            self.songs.append(song_dict)
        
        # Keep only last 1000 songs
        if len(self.songs) > 1000:
            self.songs = self.songs[-1000:]
            
    def get_top_songs(self, limit: int = 100):
        sorted_songs = sorted(self.songs, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_songs[:limit]
    
    def get_songs_by_station(self, station: str, limit: int = 50):
        station_songs = [s for s in self.songs if s.get("station") == station]
        sorted_songs = sorted(station_songs, key=lambda x: x.get("plays", 0), reverse=True)
        return sorted_songs[:limit]

db = Database()

# ====== SCRAPING FUNCTIONS ======
async def scrape_tv_station(station_name: str) -> List[Dict[str, Any]]:
    """
    Scrape a single TV station
    """
    try:
        if not tv_scraper:
            raise Exception("TV scraper not initialized")
        
        logger.info(f"📺 Scraping station: {station_name}")
        
        # Add timeout to prevent hanging
        async with asyncio.timeout(SCRAPER_CONFIG["max_scraping_time"]):
            items = await tv_scraper.scrape_station(station_name)
            
        # Add metadata
        for item in items:
            item["station"] = station_name
            item["scraped_at"] = datetime.utcnow().isoformat()
            
        logger.info(f"✅ Scraped {len(items)} items from {station_name}")
        return items
        
    except asyncio.TimeoutError:
        logger.error(f"⏰ Scraping timeout for {station_name}")
        raise HTTPException(status_code=408, detail=f"Scraping timeout for {station_name}")
    except Exception as e:
        logger.error(f"❌ Error scraping {station_name}: {str(e)}")
        raise

async def scrape_multiple_stations(stations: List[str]) -> Dict[str, Any]:
    """
    Scrape multiple stations concurrently
    """
    if not stations or stations == ["all"]:
        stations_to_scrape = SCRAPER_CONFIG["stations"]
    else:
        stations_to_scrape = [s for s in stations if s in SCRAPER_CONFIG["stations"]]
    
    if not stations_to_scrape:
        return {"error": "No valid stations to scrape"}
    
    scraping_state["is_scraping"] = True
    scraping_state["last_scrape_time"] = datetime.utcnow()
    scraping_state["scrape_results"] = {}
    scraping_state["errors"] = []
    
    all_items = []
    
    try:
        # Scrape stations concurrently
        tasks = []
        for station in stations_to_scrape:
            task = asyncio.create_task(
                scrape_tv_station_safe(station)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            station = stations_to_scrape[i]
            
            if isinstance(result, Exception):
                error_msg = f"Station {station}: {str(result)}"
                scraping_state["errors"].append(error_msg)
                scraping_state["scrape_results"][station] = {"error": str(result)}
            else:
                scraping_state["scrape_results"][station] = {
                    "count": len(result),
                    "items": result[:10]  # Store only first 10 for status
                }
                all_items.extend(result)
                
                # Ingest into database
                song_items = [
                    SongItem(
                        title=item.get("title", ""),
                        artist=item.get("artist", ""),
                        plays=item.get("plays", 0),
                        score=calculate_score(item),
                        station=station,
                        timestamp=item.get("timestamp")
                    ) for item in result
                ]
                db.add_songs(song_items, f"tv_scrape_{station}")
        
        logger.info(f"🎯 Total scraped items: {len(all_items)} from {len(stations_to_scrape)} stations")
        return {
            "total_items": len(all_items),
            "stations_scraped": stations_to_scrape,
            "errors": scraping_state["errors"]
        }
        
    finally:
        scraping_state["is_scraping"] = False

async def scrape_tv_station_safe(station: str):
    """
    Safe wrapper for scraping with error handling
    """
    try:
        return await scrape_tv_station(station)
    except Exception as e:
        logger.error(f"Error in safe scrape for {station}: {e}")
        return []

def calculate_score(item: Dict[str, Any]) -> float:
    """
    Calculate a score for a song based on various factors
    """
    base_score = item.get("plays", 0) * 0.1
    # Add more scoring logic here
    return min(base_score + 50.0, 100.0)  # Cap at 100

# ====== API ENDPOINTS ======
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "version": "7.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "features": ["tv_scraping", "charts", "admin", "health"],
        "scraper_available": TV_SCRAPER_AVAILABLE,
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
async def health():
    scraper_health = "healthy" if TV_SCRAPER_AVAILABLE else "unavailable"
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "scraper": scraper_health,
        "database_songs": len(db.songs),
        "uptime": "running",
        "environment": os.getenv("ENV", "production")
    }

@app.get("/charts/top100")
async def top_charts(
    limit: int = Query(100, ge=1, le=200),
    station: Optional[str] = None
):
    if station:
        songs = db.get_songs_by_station(station, limit)
        chart_name = f"Uganda Top {limit} - {station}"
    else:
        songs = db.get_top_songs(limit)
        chart_name = f"Uganda Top {limit}"
    
    return {
        "chart": chart_name,
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
    """
    Trigger TV scraping manually
    """
    if not TV_SCRAPER_AVAILABLE:
        raise HTTPException(status_code=503, detail="TV scraper not available")
    
    if scraping_state["is_scraping"] and not request.force:
        raise HTTPException(
            status_code=409,
            detail="Scraping already in progress. Use force=true to override."
        )
    
    if request.async_mode:
        # Run in background
        background_tasks.add_task(scrape_multiple_stations, request.stations)
        
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
    """
    Get current scraping status
    """
    last_time = None
    if scraping_state["last_scrape_time"]:
        last_time = scraping_state["last_scrape_time"].isoformat()
    
    status = ScrapeStatus(
        is_scraping=scraping_state["is_scraping"],
        last_scrape_time=last_time,
        stations_scraped=list(scraping_state["scrape_results"].keys()),
        total_items=sum(len(r.get("items", [])) for r in scraping_state["scrape_results"].values() if "items" in r),
        errors=scraping_state["errors"]
    )
    return status

@app.post("/ingest/tv")
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(verify_ingest)
):
    """
    Ingest TV data (manual or from scraper)
    """
    try:
        # Add station info if not present
        for item in payload.items:
            if not item.station and payload.station:
                item.station = payload.station
        
        db.add_songs(payload.items, payload.source)
        
        return {
            "status": "success",
            "message": f"Ingested {len(payload.items)} songs from {payload.source}",
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat(),
            "total_songs_in_db": len(db.songs)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "scraping_enabled": SCRAPER_CONFIG["enable_auto_scrape"],
        "scraper_available": TV_SCRAPER_AVAILABLE,
        "total_songs": len(db.songs),
        "scraping_state": {
            "is_scraping": scraping_state["is_scraping"],
            "last_scrape": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None
        }
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
