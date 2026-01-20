"""
UG Board Engine - Working Version (TV Scraper Import Fixed)
"""
import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Configure logging without file handler
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Security tokens - YOUR ORIGINAL VALUES
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

security = HTTPBearer()

# ====== FIX: MOCK SCRAPER (NO FILE IMPORT) ======
TV_SCRAPER_AVAILABLE = False

class MockTVScraper:
    """Mock TV scraper for demonstration"""
    async def scrape_station(self, station_name: str) -> List[Dict[str, Any]]:
        logger.info(f"Mock scraping station: {station_name}")
        return [
            {
                "title": f"Nalumansi - {station_name} Edition", 
                "artist": "Bobi Wine", 
                "plays": 150 + len(station_name) * 10,
                "timestamp": datetime.utcnow().isoformat(),
                "duration": "3:45",
                "category": "music"
            },
            {
                "title": f"Sitya Loss - {station_name} Mix", 
                "artist": "Eddy Kenzo", 
                "plays": 120 + len(station_name) * 8,
                "timestamp": datetime.utcnow().isoformat(),
                "duration": "4:20",
                "category": "music"
            }
        ]

tv_scraper = MockTVScraper()

# ====== SCRAPER CONFIGURATION ======
SCRAPER_CONFIG = {
    "stations": ["ntv", "bukedde", "sanyuka", "spark"],
    "scrape_interval": 3600,
    "max_scraping_time": 300,
    "enable_auto_scrape": False
}

# ====== GLOBAL STATE ======
scraping_state = {
    "last_scrape_time": None,
    "is_scraping": False,
    "scrape_results": {},
    "errors": []
}

app = FastAPI(
    title="UG Board Engine",
    version="7.0.0",
    description="UG Board Engine with TV Scraping Integration",
    docs_url="/docs",
    redoc_url="/redoc"
)

# ====== MODELS ======
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0
    station: Optional[str] = None

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str

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
        
        # Keep only last 1000 songs
        if len(self.songs) > 1000:
            self.songs = self.songs[-1000:]
            
    def get_top_songs(self, limit: int = 100):
        sorted_songs = sorted(self.songs, key=lambda x: x.get("score", 0), reverse=True)
        return sorted_songs[:limit]

db = Database()

# ====== API ENDPOINTS ======
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "version": "7.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "docs": "/docs",
        "health": "/health",
        "scraper": "mock_mode"
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com",
        "database_songs": len(db.songs),
        "environment": os.getenv("ENV", "production")
    }

@app.get("/charts/top100")
async def top_charts(limit: int = Query(100, ge=1, le=200)):
    songs = db.get_top_songs(limit)
    
    # Add ranks
    for i, song in enumerate(songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": songs,
        "count": len(songs),
        "timestamp": datetime.utcnow().isoformat()
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
            "total_songs_in_db": len(db.songs)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

@app.post("/scrape/tv")
async def scrape_tv(
    station: str = "ntv",
    auth: bool = Depends(verify_admin)
):
    """Mock TV scraping endpoint"""
    try:
        scraping_state["is_scraping"] = True
        scraping_state["last_scrape_time"] = datetime.utcnow()
        
        # Use mock scraper
        items = await tv_scraper.scrape_station(station)
        
        # Convert to SongItems
        song_items = [
            SongItem(
                title=item.get("title", ""),
                artist=item.get("artist", ""),
                plays=item.get("plays", 0),
                score=min(item.get("plays", 0) * 0.1 + 50, 100),
                station=station
            ) for item in items
        ]
        
        db.add_songs(song_items, f"tv_scrape_{station}")
        
        scraping_state["scrape_results"] = {
            station: {
                "count": len(items),
                "items": items[:5]
            }
        }
        
        return {
            "status": "success",
            "message": f"Mock scraped {len(items)} items from {station}",
            "station": station,
            "items_scraped": len(items),
            "timestamp": datetime.utcnow().isoformat()
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

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "total_songs": len(db.songs),
        "scraper_mode": "mock",
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
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
