"""
UG Board Engine - Minimal Working Version
"""
import os
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

# Security tokens - YOUR ORIGINAL VALUES
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "1994199620002019866")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

security = HTTPBearer()

app = FastAPI(title="UG Board Engine", version="6.0.0")

# Authentication - YOUR ORIGINAL CODE
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

# Models
class SongItem(BaseModel):
    title: str
    artist: str
    plays: Optional[int] = 0
    score: Optional[float] = 0.0

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str

# Database
SONGS = [
    {"title": "Nalumansi", "artist": "Bobi Wine", "plays": 10000, "score": 95.5},
    {"title": "Sitya Loss", "artist": "Eddy Kenzo", "plays": 8500, "score": 92.3},
]

# ====== ENDPOINTS ======
@app.get("/")
async def root():
    return {"service": "UG Board Engine", "status": "online"}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine.onrender.com"
    }

@app.get("/charts/top100")
async def top_charts(limit: int = 100):
    sorted_songs = sorted(SONGS, key=lambda x: x["score"], reverse=True)
    return {
        "chart": "Uganda Top 100",
        "entries": sorted_songs[:limit],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/tv")
async def ingest_tv(payload: IngestPayload, auth: bool = Depends(verify_ingest)):
    return {
        "status": "success",
        "message": f"Ingested {len(payload.items)} songs from TV",
        "source": payload.source,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/admin/status")
async def admin_status(auth: bool = Depends(verify_admin)):
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
