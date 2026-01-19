# api/main.py - SIMPLE WORKING VERSION
from fastapi import FastAPI
from datetime import datetime

# Define app FIRST
app = FastAPI(
    title="UG Board Engine",
    description="Ugandan Music Chart System",
    version="1.0.0"
)

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "status": "online",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "health": "/health",
            "charts": "/charts/top100",
            "docs": "/docs",
            "ingest": "/ingest/tv"
        }
    }

# Health endpoint
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com"
    }

# Charts endpoint
@app.get("/charts/top100")
async def get_top100(limit: int = 100):
    # Sample Ugandan music data
    sample_songs = [
        {"id": "1", "title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5, "rank": 1},
        {"id": "2", "title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3, "rank": 2},
        {"id": "3", "title": "Mummy", "artist": "Daddy Andre", "plays": 7800, "score": 88.7, "rank": 3},
        {"id": "4", "title": "Bailando", "artist": "Sheebah Karungi", "plays": 9200, "score": 94.1, "rank": 4},
        {"id": "5", "title": "Tonny On Low", "artist": "Gravity Omutujju", "plays": 7500, "score": 87.2, "rank": 5},
    ]
    
    return {
        "chart": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": sample_songs[:limit],
        "timestamp": datetime.utcnow().isoformat()
    }
