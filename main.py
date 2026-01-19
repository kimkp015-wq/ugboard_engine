# api/main_minimal.py
"""
Minimal UG Board Engine - Works with broken dependencies
"""

from fastapi import FastAPI, HTTPException
from datetime import datetime
import os
import json
from pathlib import Path
import hashlib

# Create minimal data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

app = FastAPI(
    title="UG Board Engine - Minimal Mode",
    description="Running with limited dependencies",
    version="1.0.0"
)

# In-memory store for minimal operation
store = {
    "songs": [],
    "ingestions": [],
    "started_at": datetime.utcnow().isoformat()
}

@app.get("/")
async def root():
    return {
        "status": "online",
        "mode": "minimal",
        "message": "UG Board Engine running in minimal mode",
        "dependencies": "Core only - recovery in progress",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "health": "/health",
            "ingest_tv": "/ingest/tv (POST)",
            "top100": "/charts/top100"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "store_size": len(store["songs"]),
        "mode": "minimal_recovery"
    }

@app.post("/ingest/tv")
async def ingest_tv(data: dict):
    """Minimal TV ingestion endpoint"""
    try:
        items = data.get("items", [])
        
        # Simple validation
        valid_items = []
        for item in items:
            if "title" in item and "artist" in item:
                # Add metadata
                item["id"] = hashlib.md5(
                    f"{item['title']}{item['artist']}".encode()
                ).hexdigest()[:8]
                item["ingested_at"] = datetime.utcnow().isoformat()
                item["source"] = "tv"
                
                valid_items.append(item)
        
        # Add to store
        store["songs"].extend(valid_items)
        store["ingestions"].append({
            "source": "tv",
            "count": len(valid_items),
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Save to file (persistence)
        with open(DATA_DIR / "minimal_store.json", "w") as f:
            json.dump(store, f, indent=2)
        
        return {
            "status": "success",
            "message": f"Ingested {len(valid_items)} items",
            "mode": "minimal",
            "total_songs": len(store["songs"])
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/charts/top100")
async def get_top100():
    """Minimal chart endpoint"""
    # Sort by ingestion time (simple ranking)
    sorted_songs = sorted(
        store["songs"],
        key=lambda x: x.get("ingested_at", ""),
        reverse=True
    )[:20]  # Top 20 only
    
    # Add ranks
    for i, song in enumerate(sorted_songs, 1):
        song["rank"] = i
    
    return {
        "chart": "Uganda Top 100 (Minimal)",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": sorted_songs,
        "total": len(store["songs"]),
        "mode": "minimal",
        "timestamp": datetime.utcnow().isoformat()
    }

# Load existing data on startup
if (DATA_DIR / "minimal_store.json").exists():
    try:
        with open(DATA_DIR / "minimal_store.json", "r") as f:
            loaded = json.load(f)
            store.update(loaded)
        print(f"📁 Loaded {len(store['songs'])} songs from storage")
    except:
        pass

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting UG Board Engine (Minimal Mode)")
    print("📡 Endpoints available:")
    print("   GET  /          - Root endpoint")
    print("   GET  /health    - Health check")
    print("   POST /ingest/tv - TV ingestion")
    print("   GET  /top100    - Chart data")
    uvicorn.run(app, host="0.0.0.0", port=8000)
