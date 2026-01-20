"""
UG Board Engine - Working Production Version
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ====== FIX: CREATE LOGS DIRECTORY ======
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# ====== CONFIGURE LOGGING ======
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

# ====== SCRAPER IMPORT ======
TV_SCRAPER_AVAILABLE = False
tv_scraper = None

try:
    # Try to import from scripts directory
    sys.path.insert(0, str(Path(__file__).parent / "scripts"))
    from tv_scraper import TVScraper
    
    # Initialize scraper
    tv_scraper = TVScraper()
    TV_SCRAPER_AVAILABLE = True
    logger.info("✅ TV Scraper imported successfully from scripts/tv_scraper.py")
    
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

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine",
    version="7.0.0",
    description="Official Ugandan Music Chart System",
    docs_url="/docs",
    redoc_url="/redoc"
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
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "database_songs": len(db.songs),
        "scraper": "available" if TV_SCRAPER_AVAILABLE else "mock"
    }

@app.get("/charts/top100")
async def top_charts(limit: int = Query(100, ge=1, le=200)):
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

@app.post("/scrape/tv")
async def scrape_tv(
    station: str = "ntv",
    auth: bool = Depends(verify_admin)
):
    """Trigger TV scraping"""
    try:
        logger.info(f"Starting TV scrape for station: {station}")
        
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
        
        return {
            "status": "success",
            "message": f"Scraped {len(items)} songs from {station}",
            "station": station,
            "items": len(items),
            "timestamp": datetime.utcnow().isoformat(),
            "scraper_mode": "real" if TV_SCRAPER_AVAILABLE else "mock"
        }
        
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")

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
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "total_songs": len(db.songs),
        "scraper_available": TV_SCRAPER_AVAILABLE
    }

# ====== ERROR HANDLER ======
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
