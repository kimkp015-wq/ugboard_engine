import os
from fastapi import FastAPI

# =========================
# Environment
# =========================

ENV = os.getenv("ENV", "development")
IS_PROD = ENV == "production"

# =========================
# Create app FIRST
# =========================

app = FastAPI(
    title="UG Board Engine",
    docs_url=None if IS_PROD else "/docs",
    redoc_url=None if IS_PROD else "/redoc",
)

# =========================
# Root health check (PUBLIC)
# =========================

@app.get("/", summary="Public engine health check")
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online",
        "environment": ENV,
        "docs_enabled": not IS_PROD,
    }

@app.get("/health", summary="Health check endpoint")
def health_check():
    return {
        "status": "healthy",
        "service": "ug-board-engine",
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# Startup contract checks
# =========================

def _validate_engine_contracts() -> None:
    """
    Hard fail early if core engine contracts are missing.
    """
    try:
        import data.chart_week as chart_week

        for name in (
            "get_current_week_id",
            "current_chart_week",
            "close_tracking_week",
            "open_new_tracking_week",
        ):
            if not hasattr(chart_week, name):
                raise RuntimeError(
                    f"Engine startup failed: data.chart_week.{name} missing"
                )

        import data.index as index

        for name in (
            "get_index",
            "record_week_publish",
            "week_already_published",
        ):
            if not hasattr(index, name):
                raise RuntimeError(
                    f"Engine startup failed: data.index.{name} missing"
                )
    except ImportError as e:
        # Don't crash if modules don't exist yet
        print(f"Warning during startup validation: {e}")

_validate_engine_contracts()

# =========================
# Import routers (AFTER validation)
# =========================

# Public charts
from api.charts.top100 import router as top100_router
from api.charts.index import router as index_router
from api.charts.regions import router as regions_router
from api.charts.trending import router as trending_router

# Ingestion (write)
from api.ingestion.youtube import router as youtube_router
from api.ingestion.radio import router as radio_router
from api.ingestion.tv import router as tv_router

# Admin - Check which modules actually exist
try:
    from api.admin.routes import router as admin_routes_router
    ADMIN_ROUTES_AVAILABLE = True
except ImportError:
    ADMIN_ROUTES_AVAILABLE = False
    print("Warning: api.admin.routes not found")

try:
    from api.admin.regions import router as admin_regions_router
    ADMIN_REGIONS_AVAILABLE = True
except ImportError:
    ADMIN_REGIONS_AVAILABLE = False
    print("Warning: api.admin.regions not found")

# =========================
# NEW: Create missing admin endpoints
# =========================

from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from data.permissions import ensure_admin_allowed
from data.store import load_items
from data.region_store import lock_region, unlock_region, is_region_locked
from data.region_snapshots import save_region_snapshot
from data.chart_week import get_current_week_id
from api.charts.scoring import calculate_scores

# Create routers for missing endpoints
admin_build_router = APIRouter()
admin_publish_router = APIRouter()
admin_health_router = APIRouter()

VALID_REGIONS = ("Eastern", "Northern", "Western")

# Health endpoints
@admin_health_router.get("/health", tags=["Admin"])
def admin_health(_: None = Depends(ensure_admin_allowed)):
    """Admin-only health check with system status"""
    try:
        items = load_items()
        
        # Check region status
        region_status = {}
        for region in VALID_REGIONS:
            region_status[region] = {
                "locked": is_region_locked(region),
                "item_count": len([i for i in items if i.get("region") == region])
            }
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "total_items": len(items),
            "regions": region_status,
            "week_id": get_current_week_id()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# Region build endpoint
@admin_build_router.post("/regions/{region}/build", tags=["Admin"])
def build_region_chart(
    region: str,
    force: bool = False,
    _: None = Depends(ensure_admin_allowed)
):
    """Build a region chart manually"""
    region = region.title()
    
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid region. Valid: {VALID_REGIONS}"
        )
    
    # Check if already locked (and not forcing)
    if is_region_locked(region) and not force:
        return {
            "status": "skipped",
            "reason": "Region already locked",
            "region": region,
            "locked": True
        }
    
    # Unlock if forcing rebuild
    if force and is_region_locked(region):
        unlock_region(region)
    
    try:
        # 1. Load items
        items = load_items()
        
        if not items:
            raise HTTPException(
                status_code=404,
                detail="No items found in database"
            )
        
        # 2. Score items
        scored_items = calculate_scores()
        
        if not scored_items:
            raise HTTPException(
                status_code=500,
                detail="Scoring failed - no scored items returned"
            )
        
        # 3. Filter by region
        region_items = [
            item for item in scored_items 
            if item.get("region", "").title() == region
        ]
        
        if not region_items:
            raise HTTPException(
                status_code=404,
                detail=f"No items found for region: {region}"
            )
        
        # 4. Sort by score
        region_items.sort(
            key=lambda x: float(x.get("score", 0)),
            reverse=True
        )
        
        # 5. Take top 5
        top5 = region_items[:5]
        
        # 6. Format for snapshot
        formatted_items = []
        for idx, item in enumerate(top5, 1):
            formatted_items.append({
                "position": idx,
                "title": item.get("title", "Unknown"),
                "artist": item.get("artist", "Unknown"),
                "score": item.get("score", 0),
                "youtube": item.get("youtube_views", 0),
                "radio": item.get("radio_plays", 0),
                "tv": item.get("tv_appearances", 0),
                "region": item.get("region", region)
            })
        
        # 7. Save snapshot
        week_id = get_current_week_id()
        snapshot_data = {
            "week_id": week_id,
            "region": region,
            "locked": True,
            "created_at": datetime.utcnow().isoformat(),
            "count": len(formatted_items),
            "items": formatted_items
        }
        
        save_region_snapshot(region, snapshot_data)
        
        # 8. Lock region
        lock_region(region)
        
        return {
            "status": "success",
            "region": region,
            "week_id": week_id,
            "count": len(formatted_items),
            "items": formatted_items,
            "snapshot_saved": True,
            "region_locked": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Chart build failed: {str(e)}"
        )

@admin_build_router.post("/regions/{region}/unlock", tags=["Admin"])
def unlock_region_endpoint(
    region: str,
    _: None = Depends(ensure_admin_allowed)
):
    """Unlock a region to allow rebuilding"""
    region = region.title()
    
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
    
    try:
        unlock_region(region)
        return {
            "status": "success",
            "region": region,
            "locked": False,
            "message": "Region unlocked for rebuilding"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unlock region: {str(e)}"
        )

# Publish endpoint
@admin_publish_router.post("/publish/weekly", tags=["Admin"])
def publish_all_regions(
    force: bool = False,
    skip_locked: bool = True,
    _: None = Depends(ensure_admin_allowed)
):
    """Publish charts for all regions"""
    results = []
    week_id = get_current_week_id()
    
    for region in VALID_REGIONS:
        try:
            # Skip already locked regions unless forcing
            if is_region_locked(region) and skip_locked and not force:
                results.append({
                    "region": region,
                    "status": "skipped",
                    "reason": "Already locked",
                    "success": True
                })
                continue
            
            # Build region chart
            try:
                # Call the build function directly
                items = load_items()
                scored_items = calculate_scores()
                region_items = [
                    item for item in scored_items 
                    if item.get("region", "").title() == region
                ]
                
                if not region_items:
                    results.append({
                        "region": region,
                        "status": "skipped",
                        "reason": "No items for region",
                        "success": False
                    })
                    continue
                
                region_items.sort(
                    key=lambda x: float(x.get("score", 0)),
                    reverse=True
                )
                
                top5 = region_items[:5]
                formatted_items = []
                for idx, item in enumerate(top5, 1):
                    formatted_items.append({
                        "position": idx,
                        "title": item.get("title", "Unknown"),
                        "artist": item.get("artist", "Unknown"),
                        "score": item.get("score", 0),
                        "youtube": item.get("youtube_views", 0),
                        "radio": item.get("radio_plays", 0),
                        "tv": item.get("tv_appearances", 0),
                        "region": item.get("region", region)
                    })
                
                # Save snapshot
                snapshot_data = {
                    "week_id": week_id,
                    "region": region,
                    "locked": True,
                    "created_at": datetime.utcnow().isoformat(),
                    "count": len(formatted_items),
                    "items": formatted_items
                }
                
                save_region_snapshot(region, snapshot_data)
                lock_region(region)
                
                results.append({
                    "region": region,
                    "status": "published",
                    "count": len(formatted_items),
                    "success": True
                })
                
            except Exception as e:
                results.append({
                    "region": region,
                    "status": "failed",
                    "error": str(e),
                    "success": False
                })
                continue
                
        except Exception as e:
            results.append({
                "region": region,
                "status": "failed",
                "error": str(e),
                "success": False
            })
    
    # Count successes
    success_count = sum(1 for r in results if r.get("success", False))
    
    return {
        "status": "completed",
        "week_id": week_id,
        "timestamp": datetime.utcnow().isoformat(),
        "regions_processed": len(results),
        "regions_successful": success_count,
        "regions_failed": len(results) - success_count,
        "results": results
    }

# =========================
# Register routers
# =========================

# Health
app.include_router(
    admin_health_router,
    prefix="/admin",
    tags=["Admin"],
)

# Charts
app.include_router(
    top100_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    index_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    regions_router,
    prefix="/charts",
    tags=["Regions"],
)

app.include_router(
    trending_router,
    prefix="/charts",
    tags=["Trending"],
)

# Ingestion
app.include_router(
    youtube_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

app.include_router(
    radio_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

app.include_router(
    tv_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

# Admin routes (if they exist)
if ADMIN_ROUTES_AVAILABLE:
    app.include_router(
        admin_routes_router,
        prefix="/admin",
        tags=["Admin"],
    )

if ADMIN_REGIONS_AVAILABLE:
    app.include_router(
        admin_regions_router,
        prefix="/admin",
        tags=["Admin"],
    )

# Always include our new admin endpoints
app.include_router(
    admin_build_router,
    prefix="/admin",
    tags=["Admin"],
)

app.include_router(
    admin_publish_router,
    prefix="/admin",
    tags=["Admin"],
)

# =========================
# Custom OpenAPI documentation
# =========================

from fastapi.openapi.utils import get_openapi
from datetime import datetime

def custom_openapi():
    """
    Generate custom OpenAPI schema with enhanced documentation.
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="UG Board Engine API",
        version="1.0.0",
        description="""
        # UG Board Music Charting Engine
        
        ## Overview
        Automated music chart system aggregating data from:
        - YouTube videos
        - Radio play data  
        - TV broadcast data
        
        ## Authentication
        1. **Internal/Cloudflare**: `X-Internal-Token` header
        2. **Ingestion Clients**: `Authorization: Bearer <INJECT_TOKEN>`
        3. **Admin Access**: `Authorization: Bearer <ADMIN_TOKEN>`
        
        ## Data Flow
        1. Ingestion → 2. Scoring → 3. Chart Calculation → 4. Weekly Publication
        """,
        routes=app.routes,
    )
    
    # Add security schemes
    openapi_schema["components"]["securitySchemes"] = {
        "InternalToken": {
            "type": "apiKey",
            "in": "header",
            "name": "X-Internal-Token",
            "description": "For Cloudflare Workers and internal automation"
        },
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": "Standard bearer token for ingestion clients"
        },
        "AdminToken": {
            "type": "http",
            "scheme": "bearer", 
            "description": "Admin access for publishing and management"
        }
    }
    
    # Tag endpoints
    for path, methods in openapi_schema["paths"].items():
        for method, details in methods.items():
            if "/admin/" in path:
                details["tags"] = ["Admin"]
            elif "/ingest/" in path:
                details["tags"] = ["Ingestion"]
            elif "/charts/" in path:
                details["tags"] = ["Charts"]
            else:
                details["tags"] = ["Health"]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

# Assign custom OpenAPI function
app.openapi = custom_openapi

# =========================
# Error handlers
# =========================

from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if not IS_PROD else "Contact administrator",
            "path": request.url.path
        }
    )

# =========================
# Startup message
# =========================

if __name__ == "__main__":
    import uvicorn
    print("🚀 UG Board Engine starting...")
    print(f"📊 Environment: {ENV}")
    print(f"📚 Docs: {'Enabled' if not IS_PROD else 'Disabled'}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
