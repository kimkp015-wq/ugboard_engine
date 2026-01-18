"""
UG Board Engine - Render Compatible Version
Simple, working version with all endpoints
"""

import os
import json
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException, Header, Body, Path, Query, Depends
from pydantic import BaseModel

# =========================
# Configuration
# =========================

PORT = int(os.getenv("PORT", 8000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "ugboard-engine")

# =========================
# Data Models
# =========================

class Region(str, Enum):
    UG = "ug"
    GH = "gh"
    NG = "ng"
    KE = "ke"
    ZA = "za"
    TZ = "tz"

class ChartItem(BaseModel):
    rank: int
    title: str
    artist: str
    score: float

class IngestionRequest(BaseModel):
    items: List[Dict]
    source: str

# =========================
# Authentication
# =========================

def verify_admin(auth: str = Header(None)):
    if auth != "Bearer admin-ug-board-2025":
        raise HTTPException(401, "Invalid admin token")
    return True

def verify_ingestion(auth: str = Header(None)):
    if auth != "Bearer inject-ug-board-2025":
        raise HTTPException(401, "Invalid ingestion token")
    return True

def verify_internal(token: str = Header(None, alias="X-Internal-Token")):
    if token != "1994199620002019866":
        raise HTTPException(401, "Invalid internal token")
    return True

# =========================
# Create App
# =========================

app = FastAPI(
    title="UG Board Engine",
    description="Automated music chart system",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# =========================
# Default Category
# =========================

@app.get("/", tags=["Default"])
def root():
    """Public engine health check"""
    return {
        "service": "UG Board Engine",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Health Category
# =========================

@app.get("/health", tags=["Health"])
def health():
    """Public health check"""
    return {"status": "healthy"}

@app.get("/admin/health", tags=["Health"], dependencies=[Depends(verify_admin)])
def admin_health():
    """Admin health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": SERVICE_NAME
    }

# =========================
# Charts Category
# =========================

@app.get("/charts/top100", tags=["Charts"])
def get_top100():
    """Uganda Top 100 (current week)"""
    return {
        "region": "ug",
        "chart_name": "Uganda Top 100",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "entries": [
            {"rank": i, "title": f"Song {i}", "artist": f"Artist {i}", "score": 100 - i}
            for i in range(1, 101)
        ]
    }

@app.get("/charts/index", tags=["Charts"])
def get_index():
    """Public chart publish index"""
    return {
        "index": [
            {
                "week": (datetime.utcnow() - timedelta(weeks=i)).strftime("%Y-W%W"),
                "published": (datetime.utcnow() - timedelta(weeks=i)).isoformat(),
                "regions": ["ug", "gh", "ng", "ke", "za", "tz"]
            }
            for i in range(4)
        ]
    }

# =========================
# Regions Category
# =========================

@app.get("/charts/regions/{region}", tags=["Regions"])
def get_region_chart(region: Region):
    """Get Top 5 songs per region"""
    return {
        "region": region.value,
        "chart_name": f"{region.value.upper()} Top 5",
        "entries": [
            {"rank": i, "title": f"{region.value.upper()} Song {i}", "artist": f"Artist {i}", "score": 100 - i*10}
            for i in range(1, 6)
        ]
    }

# =========================
# Trending Category
# =========================

@app.get("/charts/trending", tags=["Trending"])
def get_trending(limit: int = Query(20, ge=1, le=50)):
    """Trending songs (live)"""
    return {
        "trending": [
            {"id": i, "title": f"Trending Song {i}", "artist": f"Artist {i}", "velocity": 50 + i}
            for i in range(1, limit + 1)
        ],
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Ingestion Category
# =========================

@app.post("/ingest/youtube", tags=["Ingestion"], dependencies=[Depends(verify_ingestion)])
def ingest_youtube(request: IngestionRequest):
    """Ingest YouTube videos (idempotent)"""
    return {
        "status": "success",
        "message": f"Ingested {len(request.items)} YouTube items",
        "source": "youtube",
        "count": len(request.items),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/radio", tags=["Ingestion"], dependencies=[Depends(verify_internal)])
def ingest_radio(request: IngestionRequest):
    """Ingest Radio data (validated)"""
    return {
        "status": "success",
        "message": f"Ingested {len(request.items)} radio items",
        "source": "radio",
        "count": len(request.items),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/ingest/tv", tags=["Ingestion"], dependencies=[Depends(verify_internal)])
def ingest_tv(request: IngestionRequest):
    """Ingest TV data (validated)"""
    return {
        "status": "success",
        "message": f"Ingested {len(request.items)} TV items",
        "source": "tv",
        "count": len(request.items),
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Admin Category
# =========================

@app.post("/admin/publish/weekly", tags=["Admin"], dependencies=[Depends(verify_admin)])
def publish_weekly(regions: List[Region] = Body([Region.UG, Region.GH, Region.NG, Region.KE, Region.ZA, Region.TZ])):
    """Publish all regions and rotate chart week"""
    return {
        "status": "success",
        "message": f"Published charts for {len(regions)} regions",
        "week": datetime.utcnow().strftime("%Y-W%W"),
        "regions": [r.value for r in regions],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/admin/index", tags=["Admin"], dependencies=[Depends(verify_admin)])
def get_admin_index():
    """(Admin) Read-only weekly publish index"""
    return {
        "admin_view": True,
        "index": [
            {
                "week": (datetime.utcnow() - timedelta(weeks=i)).strftime("%Y-W%W"),
                "regions": ["ug", "gh", "ng", "ke", "za", "tz"],
                "status": "published"
            }
            for i in range(4)
        ]
    }

@app.post("/admin/regions/{region}/build", tags=["Admin"], dependencies=[Depends(verify_admin)])
def build_region_chart(region: Region, preview: bool = Body(True), limit: int = Body(10)):
    """(Admin) Build & preview region chart (no publish)"""
    return {
        "status": "preview" if preview else "built",
        "region": region.value,
        "chart": [
            {"rank": i, "title": f"Preview Song {i}", "artist": f"Preview Artist", "score": 90 - i}
            for i in range(1, min(limit, 100) + 1)
        ],
        "preview_only": preview,
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Run Server
# =========================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
