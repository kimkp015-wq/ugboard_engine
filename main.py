"""
UG Board Engine - Simplified Version for Koyeb
"""
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request, status
from fastapi.security import HTTPBearer
from pydantic import BaseModel, Field

# Create FastAPI app
app = FastAPI(title="UG Board Engine", version="1.0.0")

# Simple models
class SongItem(BaseModel):
    title: str
    artist: str
    plays: int = 0
    score: float = Field(0.0, ge=0.0, le=100.0)
    region: str = "central"

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str

# Simple auth
def verify_token(authorization: Optional[str] = Header(None)):
    expected = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.replace("Bearer ", "")
    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid token")
    return True

# Endpoints
@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": {"songs": 0, "regions": 4}
    }

@app.post("/ingest/youtube")
async def ingest_youtube(payload: IngestPayload, auth: bool = Depends(verify_token)):
    return {
        "status": "success",
        "message": f"Received {len(payload.items)} songs",
        "source": payload.source
    }

# Run the app
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
