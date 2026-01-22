from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List, Optional
import os

app = FastAPI()

# Simple models for testing
class SongItem(BaseModel):
    title: str
    artist: str
    plays: int = 0
    score: float = 0.0
    region: str = "central"

class IngestPayload(BaseModel):
    items: List[SongItem]
    source: str

# Authentication check
def verify_internal_token(x_internal_token: Optional[str] = Header(None)):
    expected = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
    if x_internal_token != expected:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    return True

def verify_youtube_token(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.replace("Bearer ", "")
    expected = os.getenv("YOUTUBE_TOKEN", os.getenv("INTERNAL_TOKEN"))
    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid YouTube token")
    return True

# Endpoints
@app.get("/")
def read_root():
    return {"status": "ok", "service": "UG Board Test"}

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "database": {"songs": 0, "regions": 4}
    }

@app.post("/ingest/youtube")
def ingest_youtube(payload: IngestPayload, auth: bool = Depends(verify_youtube_token)):
    return {
        "status": "success",
        "message": f"Received {len(payload.items)} songs",
        "source": payload.source
    }

@app.post("/ingest/radio")
def ingest_radio(payload: IngestPayload, auth: bool = Depends(verify_internal_token)):
    return {
        "status": "success",
        "message": f"Received {len(payload.items)} radio songs",
        "source": payload.source
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
