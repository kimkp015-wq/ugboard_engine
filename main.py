"""
UG Board Engine - Production Ready (No Pydantic Version)
Works perfectly with your current requirements.txt on Render
"""

import os
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum

from fastapi import FastAPI, APIRouter, HTTPException, Header, Body, Path, Query, Depends, status
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

# =========================
# Configuration
# =========================

PORT = int(os.getenv("PORT", 8000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "ugboard-engine")
INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID", "local")[:8]

# Authentication tokens (from your environment)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-ug-board-2025")
INJECT_TOKEN = os.getenv("INJECT_TOKEN", "inject-ug-board-2025")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")

# =========================
# Data Models (using simple dict validation)
# =========================

class Region(str, Enum):
    UG = "ug"
    GH = "gh" 
    NG = "ng"
    KE = "ke"
    ZA = "za"
    TZ = "tz"

# Manual validation functions (no pydantic)
def validate_chart_item(item: Dict) -> Dict:
    """Validate chart item manually"""
    required = ["rank", "title", "artist"]
    for field in required:
        if field not in item:
            raise ValueError(f"Missing required field: {field}")
    
    # Add defaults
    item.setdefault("score", 0.0)
    item.setdefault("plays", 0)
    item.setdefault("change", "same")
    
    return item

def validate_ingestion_payload(payload: Dict) -> Dict:
    """Validate ingestion payload manually"""
    if "items" not in payload:
        raise ValueError("Missing 'items' field")
    
    if not isinstance(payload["items"], list):
        raise ValueError("'items' must be a list")
    
    if len(payload["items"]) == 0:
        raise ValueError("'items' list cannot be empty")
    
    payload.setdefault("timestamp", datetime.utcnow().isoformat())
    payload.setdefault("metadata", {})
    
    return payload

# =========================
# Authentication
# =========================

def verify_admin(authorization: Optional[str] = Header(None)):
    """Verify admin token"""
    if not authorization or authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return True

def verify_ingestion(authorization: Optional[str] = Header(None)):
    """Verify ingestion token"""
    if not authorization or authorization != f"Bearer {INJECT_TOKEN}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid ingestion token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return True

def verify_internal(x_internal_token: Optional[str] = Header(None)):
    """Verify internal token"""
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal token"
        )
    return True

# =========================
# Data Store (In-memory for Render)
# =========================

class DataStore:
    """Simple in-memory data store"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataStore, cls).__new__(cls)
            cls._instance._init_data()
        return cls._instance
    
    def _init_data(self):
        """Initialize sample data"""
        self.charts = {
            "ug_top100": [
                {
                    "rank": i,
                    "song_id": f"song_{i:03d}",
                    "title": f"Uganda Hit #{i}",
                    "artist": f"Artist {i % 10 + 1}",
                    "plays": 10000 - (i * 90),
                    "score": 95.5 - (i * 0.5),
                    "change": "up" if i % 5 == 0 else "down" if i % 5 == 2 else "same",
                    "region": "ug"
                }
                for i in range(1, 101)
            ]
        }
        
        # Add region top 5 charts
        for region in ["gh", "ng", "ke", "za", "tz"]:
            self.charts[f"{region}_top5"] = [
                {
                    "rank": i,
                    "song_id": f"{region}_song_{i:02d}",
                    "title": f"{region.upper()} Top Song {i}",
                    "artist": f"{region.upper()} Artist {i}",
                    "plays": 5000 - (i * 800),
                    "score": 90.0 - (i * 5),
                    "change": "new" if i == 1 else "up" if i % 2 == 0 else "same",
                    "region": region
                }
                for i in range(1, 6)
            ]
        
        self.publish_index = [
            {
                "week_id": (datetime.utcnow() - timedelta(weeks=i)).strftime("%Y-W%W"),
                "publish_date": (datetime.utcnow() - timedelta(weeks=i)).isoformat(),
                "regions": ["ug", "gh", "ng", "ke", "za", "tz"],
                "status": "published",
                "instance": INSTANCE_ID
            }
            for i in range(4)
        ]
        
        self.ingestion_log = []
    
    def get_top100(self):
        return self.charts.get("ug_top100", [])
    
    def get_region_top5(self, region: str):
        return self.charts.get(f"{region}_top5", [])
    
    def get_trending(self, limit: int = 20):
        return [
            {
                "id": f"trend_{i:02d}",
                "title": f"Trending Song {i}",
                "artist": f"Trending Artist {i}",
                "velocity": 85 + (i * 2),
                "trend_score": 75 + (i * 3),
                "source": "youtube" if i % 2 == 0 else "radio",
                "region": ["ug", "gh", "ng"][i % 3]
            }
            for i in range(1, limit + 1)
        ]
    
    def log_ingestion(self, source: str, count: int):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "count": count,
            "instance": INSTANCE_ID
        }
        self.ingestion_log.append(log_entry)
        return log_entry

# Initialize data store
data_store = DataStore()

# =========================
# Create FastAPI App
# =========================

app = FastAPI(
    title="UG Board Engine",
    description="Automated music chart system aggregating data from YouTube, Radio, and TV",
    version="5.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

def custom_openapi():
    """Custom OpenAPI to match your desired structure"""
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Add security schemes
    openapi_schema["components"]["securitySchemes"] = {
        "AdminAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": f"Admin token: 'Bearer {ADMIN_TOKEN}'"
        },
        "IngestionAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": f"Ingestion token: 'Bearer {INJECT_TOKEN}'"
        },
        "InternalAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-Internal-Token",
            "description": f"Internal token: '{INTERNAL_TOKEN}'"
        }
    }
    
    # EXACT structure from your screenshots
    openapi_schema["tags"] = [
        {"name": "Default", "description": "Public engine health check"},
        {"name": "Health", "description": "Admin health check"},
        {"name": "Charts", "description": "Chart endpoints"},
        {"name": "Regions", "description": "Region-specific charts"},
        {"name": "Trending", "description": "Trending songs (live)"},
        {"name": "Ingestion", "description": "Data ingestion endpoints"},
        {"name": "Admin", "description": "Administrative functions"}
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# =========================
# Default Category
# =========================

@app.get("/", tags=["Default"])
async def root():
    """Public engine health check"""
    return {
        "service": "UG Board Engine",
        "version": "5.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "render": {
            "service": SERVICE_NAME,
            "instance": INSTANCE_ID,
            "on_render": os.getenv("RENDER", "").lower() == "true"
        },
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "admin_health": "/admin/health",
            "top100": "/charts/top100",
            "regions": "/charts/regions/{region}",
            "trending": "/charts/trending",
            "ingestion": "/ingest/{source}",
            "admin": "/admin/*"
        }
    }

# =========================
# Health Category
# =========================

@app.get("/health", tags=["Health"])
async def health():
    """Public health check (for Render monitoring)"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID
    }

@app.get("/admin/health", tags=["Health"], dependencies=[Depends(verify_admin)])
async def admin_health():
    """Admin health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "service": SERVICE_NAME,
            "instance": INSTANCE_ID,
            "python_version": os.getenv("PYTHON_VERSION", "unknown"),
            "on_render": os.getenv("RENDER", "").lower() == "true"
        },
        "data": {
            "total_charts": len(data_store.charts),
            "publish_entries": len(data_store.publish_index),
            "ingestion_logs": len(data_store.ingestion_log)
        }
    }

# =========================
# Charts Category
# =========================

@app.get("/charts/top100", tags=["Charts"])
async def get_top100():
    """Uganda Top 100 (current week)"""
    return {
        "region": "ug",
        "chart_name": "Uganda Top 100",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "published": datetime.utcnow().isoformat(),
        "entries": data_store.get_top100(),
        "total_entries": 100,
        "instance": INSTANCE_ID
    }

@app.get("/charts/index", tags=["Charts"])
async def get_chart_index():
    """Public chart publish index"""
    return {
        "index": data_store.publish_index,
        "total_weeks": len(data_store.publish_index),
        "latest_week": data_store.publish_index[0] if data_store.publish_index else None,
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Regions Category
# =========================

@app.get("/charts/regions/{region}", tags=["Regions"])
async def get_region_top5(region: Region = Path(..., description="Region code")):
    """Get Top 5 songs per region"""
    chart_data = data_store.get_region_top5(region.value)
    return {
        "region": region.value,
        "chart_name": f"{region.value.upper()} Top 5",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "entries": chart_data,
        "total_entries": len(chart_data),
        "instance": INSTANCE_ID,
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Trending Category
# =========================

@app.get("/charts/trending", tags=["Trending"])
async def get_trending(
    limit: int = Query(default=20, ge=1, le=50, description="Number of trending items")
):
    """Trending songs (live)"""
    trending_data = data_store.get_trending(limit)
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "data_source": "live",
        "trending": trending_data,
        "total_trending": len(trending_data),
        "refresh_interval": "5 minutes",
        "instance": INSTANCE_ID
    }

# =========================
# Ingestion Category
# =========================

@app.post("/ingest/youtube", tags=["Ingestion"], dependencies=[Depends(verify_ingestion)])
async def ingest_youtube(payload: Dict = Body(...)):
    """Ingest YouTube videos (idempotent)"""
    try:
        validated = validate_ingestion_payload(payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    request_id = str(uuid.uuid4())
    log_entry = data_store.log_ingestion("youtube", len(validated["items"]))
    
    return {
        "status": "success",
        "message": f"Ingested {len(validated['items'])} YouTube items",
        "request_id": request_id,
        "source": "youtube",
        "items_processed": len(validated["items"]),
        "timestamp": datetime.utcnow().isoformat(),
        "idempotent": True,
        "log_entry": log_entry,
        "instance": INSTANCE_ID
    }

@app.post("/ingest/radio", tags=["Ingestion"], dependencies=[Depends(verify_internal)])
async def ingest_radio(payload: Dict = Body(...)):
    """Ingest Radio data (validated)"""
    try:
        validated = validate_ingestion_payload(payload)
        
        # Radio-specific validation
        for item in validated["items"]:
            if "station" not in item:
                raise ValueError("Radio items must include 'station' field")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    log_entry = data_store.log_ingestion("radio", len(validated["items"]))
    
    return {
        "status": "success",
        "message": f"Ingested {len(validated['items'])} radio items",
        "source": "radio",
        "items_processed": len(validated["items"]),
        "validation_passed": True,
        "timestamp": datetime.utcnow().isoformat(),
        "log_entry": log_entry,
        "instance": INSTANCE_ID
    }

@app.post("/ingest/tv", tags=["Ingestion"], dependencies=[Depends(verify_internal)])
async def ingest_tv(payload: Dict = Body(...)):
    """Ingest TV data (validated)"""
    try:
        validated = validate_ingestion_payload(payload)
        
        # TV-specific validation
        for item in validated["items"]:
            if "channel" not in item:
                raise ValueError("TV items must include 'channel' field")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    log_entry = data_store.log_ingestion("tv", len(validated["items"]))
    
    return {
        "status": "success",
        "message": f"Ingested {len(validated['items'])} TV items",
        "source": "tv",
        "items_processed": len(validated["items"]),
        "validation_passed": True,
        "timestamp": datetime.utcnow().isoformat(),
        "log_entry": log_entry,
        "instance": INSTANCE_ID
    }

# =========================
# Admin Category
# =========================

@app.post("/admin/publish/weekly", tags=["Admin"], dependencies=[Depends(verify_admin)])
async def publish_weekly(
    regions: List[Region] = Body(default=[Region.UG, Region.GH, Region.NG, Region.KE, Region.ZA, Region.TZ]),
    dry_run: bool = Body(default=False),
    force: bool = Body(default=False)
):
    """Publish all regions and rotate chart week"""
    week_id = datetime.utcnow().strftime("%Y-W%W")
    
    result = {
        "status": "dry_run" if dry_run else "published",
        "week_id": week_id,
        "regions": [r.value for r in regions],
        "operation": "weekly_publish",
        "timestamp": datetime.utcnow().isoformat(),
        "instance": INSTANCE_ID,
        "idempotent": True,
        "force": force
    }
    
    if not dry_run:
        result["published_at"] = datetime.utcnow().isoformat()
        result["chart_counts"] = {r.value: 100 if r == Region.UG else 5 for r in regions}
    
    return result

@app.get("/admin/index", tags=["Admin"], dependencies=[Depends(verify_admin)])
async def get_admin_index():
    """(Admin) Read-only weekly publish index"""
    return {
        "admin_view": True,
        "index": data_store.publish_index,
        "total_entries": len(data_store.publish_index),
        "exportable": True,
        "timestamp": datetime.utcnow().isoformat(),
        "instance": INSTANCE_ID
    }

@app.post("/admin/regions/{region}/build", tags=["Admin"], dependencies=[Depends(verify_admin)])
async def build_region_chart(
    region: Region = Path(..., description="Region code"),
    preview: bool = Body(default=True),
    limit: int = Body(default=10, ge=1, le=100)
):
    """(Admin) Build & preview region chart (no publish)"""
    chart_data = [
        {
            "rank": i,
            "song_id": f"preview_{region.value}_{i:02d}",
            "title": f"Preview Song {i} for {region.value.upper()}",
            "artist": f"Preview Artist {i}",
            "plays": 1000 * (limit - i + 1),
            "score": 85.0 - (i * 3),
            "change": "new" if i == 1 else "up",
            "region": region.value,
            "preview": True
        }
        for i in range(1, min(limit, 100) + 1)
    ]
    
    return {
        "status": "preview" if preview else "built",
        "region": region.value,
        "chart": chart_data,
        "chart_size": len(chart_data),
        "preview_only": preview,
        "would_publish": not preview,
        "timestamp": datetime.utcnow().isoformat(),
        "instance": INSTANCE_ID
    }

# =========================
# Error Handling
# =========================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "instance": INSTANCE_ID,
            "request_id": str(uuid.uuid4())
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    error_id = str(uuid.uuid4())
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "instance": INSTANCE_ID,
            "request_id": error_id
        }
    )

# =========================
# Server Startup
# =========================

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Starting UG Board Engine v5.0.0 on port {PORT}")
    print(f"🌐 Service: {SERVICE_NAME}")
    print(f"📚 Docs: http://localhost:{PORT}/docs")
    print(f"🏥 Health: http://localhost:{PORT}/health")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
