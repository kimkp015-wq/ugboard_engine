"""
UG Board Engine - With TV Scraper Integration (FIXED FOR scripts/tv_scraper.py)
"""
import os
import asyncio
import logging
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ====== FIX: ADD scripts TO PATH ======
# Add scripts directory to Python path
scripts_path = os.path.join(os.path.dirname(__file__), "scripts")
if os.path.exists(scripts_path):
    sys.path.insert(0, scripts_path)

# ====== FIX: GLOBAL VARIABLES ======
tv_scraper = None
TV_SCRAPER_AVAILABLE = False

# ====== FIX: IMPORT FROM scripts DIRECTORY ======
try:
    # Try to import from scripts directory
    from tv_scraper import TVScraper
    TV_SCRAPER_AVAILABLE = True
    logging.info("✅ Imported TVScraper from scripts directory")
except ImportError as e:
    logging.warning(f"❌ Could not import TVScraper from scripts: {e}")
    
    # Try alternative import paths
    try:
        # Try direct import
        import tv_scraper
        TVScraper = tv_scraper.TVScraper
        TV_SCRAPER_AVAILABLE = True
        logging.info("✅ Imported TVScraper directly")
    except ImportError:
        # Mock scraper for fallback
        class TVScraper:
            async def scrape_station(self, station_name: str) -> List[Dict[str, Any]]:
                logging.info(f"📺 Mock scraping station: {station_name}")
                # Mock data matching your scraper output format
                return [
                    {
                        "title": f"Nalumansi - {station_name} Edition", 
                        "artist": "Bobi Wine", 
                        "plays": 150, 
                        "timestamp": datetime.utcnow().isoformat(),
                        "duration": "3:45",
                        "category": "music"
                    },
                    {
                        "title": f"Sitya Loss - {station_name} Mix", 
                        "artist": "Eddy Kenzo", 
                        "plays": 120, 
                        "timestamp": datetime.utcnow().isoformat(),
                        "duration": "4:20",
                        "category": "music"
                    }
                ]
        TV_SCRAPER_AVAILABLE = False
        logging.warning("❌ TVScraper module not found, using mock data")

# Security tokens - YOUR ORIGINAL VALUES
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

security = HTTPBearer()

# ====== SCRAPER CONFIGURATION ======
# Based on your tv_scraper.py structure
SCRAPER_CONFIG = {
    "stations": ["ntv", "bukedde", "sanyuka", "spark"],  # From your TV stations
    "scrape_interval": 3600,  # 1 hour in seconds
    "max_scraping_time": 300,  # 5 minutes max per scrape
    "enable_auto_scrape": False,  # Start with manual scraping
    "retry_attempts": 3,
    "retry_delay": 5  # seconds
}

# ====== GLOBAL STATE FOR SCRAPING ======
scraping_state = {
    "last_scrape_time": None,
    "is_scraping": False,
    "scrape_results": {},
    "errors": [],
    "total_scrapes": 0,
    "successful_scrapes": 0
}

# ====== LIFECYCLE MANAGEMENT ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events
    """
    # Startup
    logger.info("🚀 UG Board Engine starting up...")
    logger.info(f"📁 Scripts path: {scripts_path}")
    logger.info(f"📺 TV Scraper Available: {TV_SCRAPER_AVAILABLE}")
    
    # Initialize scraper
    global tv_scraper
    try:
        if TV_SCRAPER_AVAILABLE:
            tv_scraper = TVScraper()
            logger.info("✅ TV Scraper initialized successfully")
        else:
            tv_scraper = TVScraper()  # Mock scraper
            logger.info("⚠️ Using mock TV Scraper")
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
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# ====== MODELS ======
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    timestamp: Optional[str] = None
    station: Optional[str] = None
    duration: Optional[str] = None
    category: Optional[str] = "music"

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
    scraper_available: bool

# ====== AUTHENTICATION (YOUR ORIGINAL CODE) ======
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
            song_dict["id"] = f"song_{len(self.songs) + 1}"
            self.songs.append(song_dict)
        
        # Keep only last 5000 songs
        if len(self.songs) > 5000:
            self.songs = self.songs[-5000:]
            
    def get_top_songs(self, limit: int = 100):
        sorted_songs = sorted(self.songs, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_songs[:limit]
    
    def get_songs_by_station(self, station: str, limit: int = 50):
        station_songs = [s for s in self.songs if s.get("station") == station]
        sorted_songs = sorted(station_songs, key=lambda x: x.get("plays", 0), reverse=True)
        return sorted_songs[:limit]
    
    def get_stats(self):
        return {
            "total_songs": len(self.songs),
            "unique_artists": len(set(s.get("artist", "") for s in self.songs)),
            "total_plays": sum(s.get("plays", 0) for s in self.songs),
            "stations": list(set(s.get("station", "") for s in self.songs if s.get("station")))
        }

db = Database()

# ====== SCRAPING FUNCTIONS ======
async def scrape_tv_station(station_name: str, retry_count: int = 0) -> List[Dict[str, Any]]:
    """
    Scrape a single TV station with retry logic
    """
    try:
        if not tv_scraper:
            raise Exception("TV scraper not initialized")
        
        logger.info(f"📺 Scraping station: {station_name} (attempt {retry_count + 1})")
        
        # Add timeout to prevent hanging
        async with asyncio.timeout(SCRAPER_CONFIG["max_scraping_time"]):
            items = await tv_scraper.scrape_station(station_name)
            
        # Validate items
        valid_items = []
        for item in items:
            if isinstance(item, dict) and item.get("title") and item.get("artist"):
                item["station"] = station_name
                item["scraped_at"] = datetime.utcnow().isoformat()
                valid_items.append(item)
        
        logger.info(f"✅ Scraped {len(valid_items)} items from {station_name}")
        return valid_items
        
    except asyncio.TimeoutError:
        logger.error(f"⏰ Scraping timeout for {station_name}")
        if retry_count < SCRAPER_CONFIG["retry_attempts"]:
            logger.info(f"🔄 Retrying {station_name} in {SCRAPER_CONFIG['retry_delay']}s...")
            await asyncio.sleep(SCRAPER_CONFIG["retry_delay"])
            return await scrape_tv_station(station_name, retry_count + 1)
        raise HTTPException(status_code=408, detail=f"Scraping timeout for {station_name}")
    except Exception as e:
        logger.error(f"❌ Error scraping {station_name}: {str(e)}")
        if retry_count < SCRAPER_CONFIG["retry_attempts"]:
            logger.info(f"🔄 Retrying {station_name} in {SCRAPER_CONFIG['retry_delay']}s...")
            await asyncio.sleep(SCRAPER_CONFIG["retry_delay"])
            return await scrape_tv_station(station_name, retry_count + 1)
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
                logger.error(f"Failed to scrape {station}: {result}")
            else:
                scraping_state["scrape_results"][station] = {
                    "count": len(result),
                    "items": result[:5]  # Store only first 5 for status
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
                        timestamp=item.get("timestamp"),
                        duration=item.get("duration"),
                        category=item.get("category", "music")
                    ) for item in result
                ]
                db.add_songs(song_items, f"tv_scrape_{station}")
                logger.info(f"📥 Ingested {len(result)} songs from {station}")
        
        scraping_state["total_scrapes"] += 1
        scraping_state["successful_scrapes"] += 1 if len(all_items) > 0 else 0
        
        logger.info(f"🎯 Total scraped items: {len(all_items)} from {len(stations_to_scrape)} stations")
        return {
            "status": "success",
            "total_items": len(all_items),
            "stations_scraped": stations_to_scrape,
            "errors": scraping_state["errors"],
            "timestamp": datetime.utcnow().isoformat()
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
    duration_bonus = 0
    
    # Add duration bonus (longer songs get slightly higher score)
    if item.get("duration"):
        try:
            # Parse duration like "3:45"
            if ":" in item["duration"]:
                mins, secs = map(int, item["duration"].split(":"))
                total_seconds = mins * 60 + secs
                duration_bonus = min(total_seconds / 600, 10)  # Max 10 points
        except:
            pass
    
    return min(base_score + duration_bonus + 50.0, 100.0)  # Cap at 100

# ====== BACKGROUND SCRAPING TASK ======
async def periodic_scraping():
    """
    Background task for periodic scraping (if enabled)
    """
    while True:
        try:
            if SCRAPER_CONFIG["enable_auto_scrape"] and TV_SCRAPER_AVAILABLE:
                logger.info("🔄 Running periodic TV scraping...")
                result = await scrape_multiple_stations(["all"])
                logger.info(f"Periodic scraping result: {result}")
                
            # Wait for next interval
            await asyncio.sleep(SCRAPER_CONFIG["scrape_interval"])
                
        except Exception as e:
            logger.error(f"Error in periodic scraping: {e}")
            await asyncio.sleep(300)  # Wait 5 minutes on error

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
        "health": "/health",
        "scrape_endpoint": "/scrape/tv (POST)"
    }

@app.get("/health")
async def health():
    scraper_status = "available" if TV_SCRAPER_AVAILABLE else "unavailable"
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "scraper": scraper_status,
        "database": {
            "songs": len(db.songs),
            "stations": len(db.get_stats()["stations"])
        },
        "scraping": {
            "total_scrapes": scraping_state["total_scrapes"],
            "successful_scrapes": scraping_state["successful_scrapes"],
            "last_scrape": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None
        }
    }

@app.get("/charts/top100")
async def top_charts(
    limit: int = Query(100, ge=1, le=200),
    station: Optional[str] = None
):
    if station:
        songs = db.get_songs_by_station(station, limit)
        chart_name = f"Uganda Top {limit} - {station.upper()}"
    else:
        songs = db.get_top_songs(limit)
        chart_name = f"Uganda Top {limit}"
    
    # Add ranks
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": chart_name,
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "ugboard-engine"
    }

@app.post("/scrape/tv")
async def trigger_scrape(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks,
    auth: bool = Depends(verify_admin)
):
    """
    Trigger TV scraping manually
    """
    if not TV_SCRAPER_AVAILABLE:
        raise HTTPException(
            status_code=503, 
            detail="TV scraper not available. Check if tv_scraper.py is in scripts directory."
        )
    
    if scraping_state["is_scraping"] and not request.force:
        raise HTTPException(
            status_code=409,
            detail="Scraping already in progress. Use force=true to override."
        )
    
    logger.info(f"🚀 Manual scrape requested for stations: {request.stations}")
    
    if request.async_mode:
        # Run in background
        background_tasks.add_task(scrape_multiple_stations, request.stations)
        
        return {
            "status": "started",
            "message": f"Scraping started for stations: {request.stations}",
            "async": True,
            "timestamp": datetime.utcnow().isoformat(),
            "monitor": "/scrape/status"
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
    
    # Calculate total items
    total_items = 0
    for station_data in scraping_state["scrape_results"].values():
        if "items" in station_data:
            total_items += len(station_data["items"])
    
    status = ScrapeStatus(
        is_scraping=scraping_state["is_scraping"],
        last_scrape_time=last_time,
        stations_scraped=list(scraping_state["scrape_results"].keys()),
        total_items=total_items,
        errors=scraping_state["errors"],
        scraper_available=TV_SCRAPER_AVAILABLE
    )
    return status

@app.get("/scrape/results")
async def get_scrape_results(
    station: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    auth: bool = Depends(verify_admin)
):
    """
    Get detailed scraping results
    """
    if station:
        if station in scraping_state["scrape_results"]:
            station_data = scraping_state["scrape_results"][station]
            if "items" in station_data:
                return {
                    "station": station,
                    "items": station_data["items"][:limit],
                    "total": station_data.get("count", 0),
                    "timestamp": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None
                }
        raise HTTPException(status_code=404, detail=f"No results for station: {station}")
    
    # Return all results
    return {
        "results": scraping_state["scrape_results"],
        "last_scrape": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None,
        "total_stations": len(scraping_state["scrape_results"])
    }

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
        stats = db.get_stats()
        
        return {
            "status": "success",
            "message": f"Ingested {len(payload.items)} songs from {payload.source}",
            "source": payload.source,
            "timestamp": datetime.utcnow().isoformat(),
            "database_stats": stats
        }
    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    db_stats = db.get_stats()
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "scraping": {
            "enabled": SCRAPER_CONFIG["enable_auto_scrape"],
            "available": TV_SCRAPER_AVAILABLE,
            "is_scraping": scraping_state["is_scraping"],
            "last_scrape": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None,
            "total_scrapes": scraping_state["total_scrapes"]
        },
        "database": db_stats,
        "config": {
            "stations": SCRAPER_CONFIG["stations"],
            "scrape_interval": SCRAPER_CONFIG["scrape_interval"],
            "max_scraping_time": SCRAPER_CONFIG["max_scraping_time"]
        }
    }

@app.get("/stations")
async def list_stations():
    """
    List available TV stations for scraping
    """
    return {
        "stations": SCRAPER_CONFIG["stations"],
        "count": len(SCRAPER_CONFIG["stations"]),
        "last_scraped": scraping_state["last_scrape_time"].isoformat() if scraping_state["last_scrape_time"] else None,
        "scraper_available": TV_SCRAPER_AVAILABLE
    }

@app.get("/stations/{station}/songs")
async def get_station_songs(
    station: str,
    limit: int = Query(50, ge=1, le=100)
):
    """
    Get songs from a specific station
    """
    songs = db.get_songs_by_station(station, limit)
    
    return {
        "station": station,
        "songs": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
    }

# ====== ERROR HANDLERS ======
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    logger.error(f"HTTP Error {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
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
            "detail": str(exc) if os.getenv("ENV") != "production" else "Contact support",
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

# ====== START BACKGROUND TASK IF ENABLED ======
@app.on_event("startup")
async def startup_event():
    """Start background tasks on startup"""
    if SCRAPER_CONFIG["enable_auto_scrape"] and TV_SCRAPER_AVAILABLE:
        asyncio.create_task(periodic_scraping())
        logger.info("🔄 Background scraping task started")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    
    print(f"""
    🎵 UG Board Engine v7.0.0
    📺 TV Scraper: {'✅ Available' if TV_SCRAPER_AVAILABLE else '❌ Not Found'}
    📁 Scripts Path: {scripts_path}
    🌐 Port: {port}
    📚 Docs: http://localhost:{port}/docs
    🔧 Scrape Endpoint: POST /scrape/tv
    """)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
