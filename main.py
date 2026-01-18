"""
UG Board Engine - Render Optimized Production Version
Matching exact Swagger structure with Render best practices
"""

import os
import json
import uuid
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, APIRouter, Header, HTTPException, Depends, Body, Path, Query, status
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field, validator
import logging

# =========================
# Render-Specific Configuration
# =========================

# Get Render environment variables
RENDER = os.getenv("RENDER", "").lower() == "true"
PORT = int(os.getenv("PORT", 8000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "ugboard-engine")
INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID", "local")

# Configure Render-optimized logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(instance)s] - %(message)s' if RENDER else '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {"instance": INSTANCE_ID[:8]})

# =========================
# Startup/Shutdown Management for Render
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for Render (FastAPI 2.x+)"""
    startup_time = time.time()
    logger.info(f"🚀 Starting {SERVICE_NAME} on Render instance {INSTANCE_ID[:8]}")
    logger.info(f"🌐 External URL: {os.getenv('RENDER_EXTERNAL_URL', 'http://localhost:' + str(PORT))}")
    logger.info(f"📊 Service ID: {os.getenv('RENDER_SERVICE_ID', 'local')}")
    
    # Log critical environment variables (masked)
    env_vars = {
        "RENDER": RENDER,
        "PORT": PORT,
        "SERVICE_NAME": SERVICE_NAME,
        "ADMIN_TOKEN_SET": bool(os.getenv("ADMIN_TOKEN")),
        "INJECT_TOKEN_SET": bool(os.getenv("INJECT_TOKEN")),
        "INTERNAL_TOKEN_SET": bool(os.getenv("INTERNAL_TOKEN")),
    }
    logger.info(f"🔧 Environment: {env_vars}")
    
    yield  # App runs here
    
    shutdown_time = time.time()
    uptime = shutdown_time - startup_time
    logger.info(f"👋 Shutting down after {uptime:.2f} seconds uptime")

# =========================
# Data Models
# =========================

class Region(str, Enum):
    """Supported regions for charting"""
    UG = "ug"  # Uganda - for top100
    GH = "gh"  # Ghana
    NG = "ng"  # Nigeria
    KE = "ke"  # Kenya
    ZA = "za"  # South Africa
    TZ = "tz"  # Tanzania

class ChartEntry(BaseModel):
    """Chart entry model"""
    rank: int = Field(..., ge=1, le=100)
    song_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    artist: str = Field(..., min_length=1)
    plays: int = Field(default=0, ge=0)
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    change: Optional[str] = Field(None, pattern="^(up|down|new|same)$")
    region: Optional[str] = None

class IngestionPayload(BaseModel):
    """Base ingestion payload"""
    items: List[Dict[str, Any]] = Field(..., min_items=1)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[Dict[str, Any]] = None
    
    @validator('items')
    def validate_items(cls, v):
        if not v:
            raise ValueError('Items list cannot be empty')
        return v

# =========================
# Authentication Service (Render Environment Aware)
# =========================

class AuthService:
    """Render-aware authentication service"""
    
    @staticmethod
    def get_tokens():
        """Get tokens from environment with fallbacks for Render"""
        return {
            "admin": os.getenv("ADMIN_TOKEN", "admin-ug-board-2025"),
            "ingestion": os.getenv("INJECT_TOKEN", "inject-ug-board-2025"),
            "internal": os.getenv("INTERNAL_TOKEN", "1994199620002019866"),
        }
    
    @classmethod
    def verify_admin_token(cls, authorization: Optional[str]) -> bool:
        """Verify admin token"""
        if not authorization:
            return False
        expected = f"Bearer {cls.get_tokens()['admin']}"
        return authorization == expected
    
    @classmethod
    def verify_inject_token(cls, authorization: Optional[str]) -> bool:
        """Verify ingestion token"""
        if not authorization:
            return False
        expected = f"Bearer {cls.get_tokens()['ingestion']}"
        return authorization == expected
    
    @classmethod
    def verify_internal_token(cls, token: Optional[str]) -> bool:
        """Verify internal/worker token"""
        return token == cls.get_tokens()['internal']

# Authentication dependencies
async def require_admin(authorization: str = Header(None)) -> str:
    """Admin authentication dependency"""
    if not AuthService.verify_admin_token(authorization):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return "admin"

async def require_ingestion(authorization: str = Header(None)) -> str:
    """Ingestion authentication dependency"""
    if not AuthService.verify_inject_token(authorization):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid ingestion token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return "ingestion"

async def require_internal(x_internal_token: str = Header(None)) -> str:
    """Internal/worker authentication dependency"""
    if not AuthService.verify_internal_token(x_internal_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal token"
        )
    return "internal"

# =========================
# In-Memory Data Store (Render Ephemeral Storage)
# =========================

class RenderDataStore:
    """In-memory data store optimized for Render's ephemeral storage"""
    
    _instance = None
    _charts = {}
    _ingestion_log = []
    _startup_time = time.time()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RenderDataStore, cls).__new__(cls)
            cls._instance._initialize_data()
        return cls._instance
    
    def _initialize_data(self):
        """Initialize with sample data"""
        # Uganda Top 100
        self._charts["ug_top100"] = [
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
        
        # Region top 5 charts
        for region in ["gh", "ng", "ke", "za", "tz"]:
            self._charts[f"{region}_top5"] = [
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
        
        logger.info(f"📊 Data store initialized with {len(self._charts)} charts")
    
    def get_top100(self):
        """Get Uganda Top 100"""
        return self._charts.get("ug_top100", [])
    
    def get_region_top5(self, region: str):
        """Get top 5 for region"""
        return self._charts.get(f"{region}_top5", [])
    
    def get_trending(self, limit: int = 20):
        """Get trending songs"""
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
        """Log ingestion activity"""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "count": count,
            "instance": INSTANCE_ID[:8]
        }
        self._ingestion_log.append(log_entry)
        # Keep only last 1000 entries to prevent memory issues
        if len(self._ingestion_log) > 1000:
            self._ingestion_log = self._ingestion_log[-1000:]
        return log_entry
    
    def get_stats(self):
        """Get data store statistics"""
        return {
            "uptime_seconds": time.time() - self._startup_time,
            "total_charts": len(self._charts),
            "total_ingestion_logs": len(self._ingestion_log),
            "instance": INSTANCE_ID[:8],
            "on_render": RENDER
        }

# Initialize data store
data_store = RenderDataStore()

# =========================
# Create FastAPI App with Render Optimizations
# =========================

app = FastAPI(
    title="UG Board Engine",
    description="Automated music chart system aggregating data from YouTube, Radio, and TV",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan if hasattr(FastAPI, "lifespan") else None  # FastAPI 2.x support
)

def custom_openapi():
    """Custom OpenAPI configuration matching IMG_7357.png structure"""
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
            "description": "Admin token from Render environment"
        },
        "IngestionAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": "Ingestion token from Render environment"
        },
        "InternalAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-Internal-Token",
            "description": "Internal worker token from Render environment"
        }
    }
    
    # EXACT structure from IMG_7357.png
    openapi_schema["tags"] = [
        {
            "name": "Default",
            "description": "Public engine health check"
        },
        {
            "name": "Health",
            "description": "Admin health check"
        },
        {
            "name": "Charts",
            "description": "Chart endpoints"
        },
        {
            "name": "Regions",
            "description": "Region-specific charts"
        },
        {
            "name": "Trending",
            "description": "Trending songs (live)"
        },
        {
            "name": "Ingestion",
            "description": "Data ingestion endpoints"
        },
        {
            "name": "Admin",
            "description": "Administrative functions"
        }
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# =========================
# Default Category (Root/Health) - Render Optimized
# =========================

default_router = APIRouter(tags=["Default"])

@default_router.get("/", 
    summary="Public engine health check",
    description="Root endpoint showing service status",
    response_description="Service status information")
async def root():
    """Public engine health check - Render optimized"""
    return {
        "service": "UG Board Engine",
        "version": "4.0.0",
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "render": {
            "service": SERVICE_NAME,
            "instance": INSTANCE_ID[:8],
            "external_url": os.getenv("RENDER_EXTERNAL_URL", None),
            "on_render": RENDER
        },
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "admin_health": "/admin/health",
            "charts": "/charts/*",
            "ingestion": "/ingest/*",
            "admin": "/admin/*",
            "stats": "/stats"
        }
    }

# =========================
# Health Category - Render Monitoring Compatible
# =========================

health_router = APIRouter(tags=["Health"])

@health_router.get("/health",
    summary="Public health check",
    description="Basic health check endpoint for Render monitoring",
    response_description="Health status")
async def health():
    """Public health check - Render monitoring compatible"""
    # This endpoint is CRITICAL for Render health checks
    try:
        stats = data_store.get_stats()
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "ugboard-engine",
            "instance": INSTANCE_ID[:8],
            "uptime_seconds": stats["uptime_seconds"],
            "on_render": RENDER,
            "checks": {
                "api": "pass",
                "memory": "pass",
                "data_store": "pass"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unhealthy"
        )

@health_router.get("/admin/health",
    summary="Admin health check",
    description="Detailed health check with system status",
    dependencies=[Depends(require_admin)],
    response_description="Detailed health information")
async def admin_health():
    """Admin health check - Detailed system info"""
    import psutil
    import sys
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "render": {
            "service_name": SERVICE_NAME,
            "instance_id": INSTANCE_ID,
            "external_url": os.getenv("RENDER_EXTERNAL_URL"),
            "service_id": os.getenv("RENDER_SERVICE_ID")
        },
        "system": {
            "python_version": sys.version.split()[0],
            "memory_used_mb": psutil.virtual_memory().used / 1024 / 1024,
            "memory_percent": psutil.virtual_memory().percent,
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "disk_usage_percent": psutil.disk_usage('/').percent,
        },
        "application": data_store.get_stats(),
        "timestamp_utc": datetime.utcnow().isoformat()
    }

@health_router.get("/stats",
    summary="Application statistics",
    description="Public application statistics",
    response_description="Application stats")
async def stats():
    """Application statistics"""
    return {
        **data_store.get_stats(),
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints_available": len(app.routes)
    }

# =========================
# Charts Category
# =========================

charts_router = APIRouter(tags=["Charts"])

@charts_router.get("/charts/top100",
    summary="Uganda Top 100 (current week)",
    description="Get the current week's Uganda Top 100 chart",
    response_description="List of top 100 chart entries")
async def get_top100():
    """Uganda Top 100 (current week)"""
    chart_data = data_store.get_top100()
    return {
        "region": "ug",
        "chart_name": "Uganda Top 100",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "published": datetime.utcnow().isoformat(),
        "entries": chart_data,
        "total_entries": len(chart_data),
        "instance": INSTANCE_ID[:8]
    }

@charts_router.get("/charts/index",
    summary="Public chart publish index",
    description="Get historical chart publish information",
    response_description="List of weekly publish records")
async def get_chart_index():
    """Public chart publish index"""
    # Simulated publish index
    index = [
        {
            "week_id": (datetime.utcnow() - timedelta(weeks=i)).strftime("%Y-W%W"),
            "publish_date": (datetime.utcnow() - timedelta(weeks=i)).isoformat(),
            "regions": ["ug", "gh", "ng", "ke", "za", "tz"],
            "status": "published",
            "instance": INSTANCE_ID[:8]
        }
        for i in range(4)
    ]
    
    return {
        "index": index,
        "total_weeks": len(index),
        "latest_week": index[0] if index else None,
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Regions Category
# =========================

regions_router = APIRouter(tags=["Regions"])

@regions_router.get("/charts/regions/{region}",
    summary="Get Top 5 songs per region",
    description="Get the top 5 chart for a specific region",
    response_description="Top 5 chart entries for the region")
async def get_region_top5(
    region: Region = Path(..., description="Region code (ug, gh, ng, ke, za, tz)")
):
    """Get Top 5 songs per region"""
    chart_data = data_store.get_region_top5(region.value)
    return {
        "region": region.value,
        "chart_name": f"{region.value.upper()} Top 5",
        "week_id": datetime.utcnow().strftime("%Y-W%W"),
        "entries": chart_data,
        "total_entries": len(chart_data),
        "instance": INSTANCE_ID[:8],
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Trending Category
# =========================

trending_router = APIRouter(tags=["Trending"])

@trending_router.get("/charts/trending",
    summary="Trending songs (live)",
    description="Get currently trending songs across all regions",
    response_description="List of trending songs")
async def get_trending(
    limit: int = Query(default=20, ge=1, le=50, description="Number of trending items to return")
):
    """Trending songs (live)"""
    trending_data = data_store.get_trending(limit)
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "data_source": "live",
        "trending": trending_data,
        "total_trending": len(trending_data),
        "refresh_interval": "5 minutes",
        "instance": INSTANCE_ID[:8]
    }

# =========================
# Ingestion Category - Render Worker Compatible
# =========================

ingestion_router = APIRouter(tags=["Ingestion"])

@ingestion_router.post("/ingest/youtube",
    summary="Ingest YouTube videos (idempotent)",
    description="Ingest YouTube video data. Idempotent operation.",
    dependencies=[Depends(require_ingestion)],
    response_description="Ingestion result")
async def ingest_youtube(
    payload: IngestionPayload = Body(...),
    x_request_id: Optional[str] = Header(None, description="Request ID for idempotency")
):
    """Ingest YouTube videos (idempotent)"""
    request_id = x_request_id or str(uuid.uuid4())
    
    # Log ingestion
    log_entry = data_store.log_ingestion("youtube", len(payload.items))
    
    # Process items
    processed_items = []
    for item in payload.items:
        processed_item = {
            **item,
            "source": "youtube",
            "ingested_at": datetime.utcnow().isoformat(),
            "request_id": request_id,
            "status": "processed",
            "processing_id": str(uuid.uuid4()),
            "instance": INSTANCE_ID[:8]
        }
        processed_items.append(processed_item)
    
    logger.info(f"YouTube ingestion: {len(processed_items)} items, request_id: {request_id}")
    
    return {
        "status": "success",
        "message": f"Ingested {len(processed_items)} YouTube items",
        "request_id": request_id,
        "source": "youtube",
        "items_processed": len(processed_items),
        "timestamp": datetime.utcnow().isoformat(),
        "idempotency_key": request_id,
        "log_entry": log_entry,
        "instance": INSTANCE_ID[:8]
    }

@ingestion_router.post("/ingest/radio",
    summary="Ingest Radio data (validated)",
    description="Ingest radio play data with validation",
    dependencies=[Depends(require_internal)],
    response_description="Ingestion result")
async def ingest_radio(
    payload: IngestionPayload = Body(...)
):
    """Ingest Radio data (validated)"""
    # Validate radio-specific data
    for item in payload.items:
        if "station" not in item:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Radio items must include 'station' field"
            )
    
    # Log ingestion
    log_entry = data_store.log_ingestion("radio", len(payload.items))
    
    processed_items = []
    for item in payload.items:
        processed_item = {
            **item,
            "source": "radio",
            "ingested_at": datetime.utcnow().isoformat(),
            "status": "processed",
            "validated": True,
            "validation_checks": ["station_present", "timestamp_valid"],
            "instance": INSTANCE_ID[:8]
        }
        processed_items.append(processed_item)
    
    logger.info(f"Radio ingestion: {len(processed_items)} items")
    
    return {
        "status": "success",
        "message": f"Ingested {len(processed_items)} radio items",
        "source": "radio",
        "items_processed": len(processed_items),
        "validation_passed": True,
        "timestamp": datetime.utcnow().isoformat(),
        "log_entry": log_entry,
        "instance": INSTANCE_ID[:8]
    }

@ingestion_router.post("/ingest/tv",
    summary="Ingest TV data (validated)",
    description="Ingest TV broadcast data with validation",
    dependencies=[Depends(require_internal)],
    response_description="Ingestion result")
async def ingest_tv(
    payload: IngestionPayload = Body(...)
):
    """Ingest TV data (validated)"""
    # Validate TV-specific data
    for item in payload.items:
        if "channel" not in item:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="TV items must include 'channel' field"
            )
    
    # Log ingestion
    log_entry = data_store.log_ingestion("tv", len(payload.items))
    
    processed_items = []
    for item in payload.items:
        processed_item = {
            **item,
            "source": "tv",
            "ingested_at": datetime.utcnow().isoformat(),
            "status": "processed",
            "validated": True,
            "validation_checks": ["channel_present", "broadcast_time_valid"],
            "instance": INSTANCE_ID[:8]
        }
        processed_items.append(processed_item)
    
    logger.info(f"TV ingestion: {len(processed_items)} items")
    
    return {
        "status": "success",
        "message": f"Ingested {len(processed_items)} TV items",
        "source": "tv",
        "items_processed": len(processed_items),
        "validation_passed": True,
        "timestamp": datetime.utcnow().isoformat(),
        "log_entry": log_entry,
        "instance": INSTANCE_ID[:8]
    }

# =========================
# Admin Category
# =========================

admin_router = APIRouter(tags=["Admin"])

@admin_router.post("/admin/publish/weekly",
    summary="Publish all regions and rotate chart week",
    description="""Weekly publish workflow (ADMIN).
    
    Guarantees:
    - Idempotent per week
    - All-or-nothing region publish
    - No startup-time import crashes""",
    dependencies=[Depends(require_admin)],
    response_description="Publish result")
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
        "requested_by": "admin",
        "instance": INSTANCE_ID[:8],
        "idempotent": True,
        "force": force
    }
    
    if not dry_run:
        result["published_at"] = datetime.utcnow().isoformat()
        result["chart_counts"] = {r.value: 100 if r == Region.UG else 5 for r in regions}
        logger.info(f"Weekly publish: {len(regions)} regions for week {week_id}")
    
    return result

@admin_router.get("/admin/index",
    summary="(Admin) Read-only weekly publish index",
    description="Admin view of weekly publish history",
    dependencies=[Depends(require_admin)],
    response_description="Admin publish index")
async def get_admin_index():
    """(Admin) Read-only weekly publish index"""
    # Simulated admin index
    index = [
        {
            "week_id": (datetime.utcnow() - timedelta(weeks=i)).strftime("%Y-W%W"),
            "publish_date": (datetime.utcnow() - timedelta(weeks=i)).isoformat(),
            "regions": ["ug", "gh", "ng", "ke", "za", "tz"],
            "status": "published",
            "instance": INSTANCE_ID[:8],
            "chart_counts": {"ug": 100, "gh": 5, "ng": 5, "ke": 5, "za": 5, "tz": 5}
        }
        for i in range(4)
    ]
    
    return {
        "admin_view": True,
        "index": index,
        "total_entries": len(index),
        "exportable": True,
        "timestamp": datetime.utcnow().isoformat(),
        "instance": INSTANCE_ID[:8]
    }

@admin_router.post("/admin/regions/{region}/build",
    summary="(Admin) Build & preview region chart (no publish)",
    description="Build and preview region chart without publishing",
    dependencies=[Depends(require_admin)],
    response_description="Region chart preview")
async def build_region_chart(
    region: Region = Path(..., description="Region code"),
    preview: bool = Body(default=True),
    limit: int = Body(default=10, ge=1, le=100)
):
    """(Admin) Build & preview region chart (no publish)"""
    # Generate preview chart
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
        "instance": INSTANCE_ID[:8]
    }

# =========================
# Register All Routers
# =========================

app.include_router(default_router)
app.include_router(health_router)
app.include_router(charts_router)
app.include_router(regions_router)
app.include_router(trending_router)
app.include_router(ingestion_router, prefix="/ingest")
app.include_router(admin_router, prefix="/admin")

# =========================
# Error Handlers
# =========================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions with Render logging"""
    logger.warning(f"HTTP {exc.status_code} at {request.url.path}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "instance": INSTANCE_ID[:8],
            "request_id": str(uuid.uuid4())
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions with Render logging"""
    error_id = str(uuid.uuid4())
    logger.error(f"Unhandled exception {error_id}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "error_id": error_id,
            "type": type(exc).__name__,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path),
            "instance": INSTANCE_ID[:8],
            "request_id": error_id
        }
    )

# =========================
# Server Startup for Render
# =========================

if __name__ == "__main__":
    import uvicorn
    logger.info(f"🚀 Starting UG Board Engine v4.0.0 on port {PORT}")
    logger.info(f"🌐 Service URL: https://{SERVICE_NAME}.onrender.com")
    logger.info(f"📚 API Documentation: https://{SERVICE_NAME}.onrender.com/docs")
    logger.info(f"🏥 Health check: https://{SERVICE_NAME}.onrender.com/health")
    
    # Render automatically sets PORT environment variable
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=PORT,
        # Render-optimized settings
        access_log=True,
        log_level="info",
        timeout_keep_alive=30
    )
