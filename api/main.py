import os
from datetime import datetime
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

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

@app.get("/", summary="Public engine health check", tags=["Health"])
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online",
        "environment": ENV,
        "docs_enabled": not IS_PROD,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health", summary="Public health check endpoint", tags=["Health"])
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

from fastapi import APIRouter
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
                    "success": True,
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
                        "success": False,
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
                    "success": True,
                })
                
            except Exception as e:
                results.append({
                    "region": region,
                    "status": "failed",
                    "error": str(e),
                    "success": False,
                })
                continue
                
        except Exception as e:
            results.append({
                "region": region,
                "status": "failed",
                "error": str(e),
                "success": False,
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
        "results": results,
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
# Custom OpenAPI documentation (OPTION C)
# =========================

def custom_openapi():
    """
    Generate custom OpenAPI schema with ONLY worker auth in Swagger.
    
    Swagger UI will show:
    - 🔒 ONLY worker endpoint requires auth
    - 🔓 All other endpoints are open for testing
    
    Production API still enforces all authentication.
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="UG Board Engine API",
        version="1.0.0",
        description=f"""
        # 🎵 UG Board Music Charting Engine
        
        ## 📊 Overview
        Automated music chart system aggregating data from YouTube, Radio, and TV.
        
        ## 🔐 Authentication (Production)
        | Endpoint Type | Authentication Method | Token |
        |---------------|----------------------|-------|
        | **Worker** | `X-Internal-Token` header | `1994199620002019866` |
        | **Admin** | `Authorization: Bearer` + `scheme` + `credentials` | `admin-ug-board-2025` |
        | **Ingestion** | `Authorization: Bearer` | `inject-ug-board-2025` |
        | **Charts** | No authentication required | - |
        
        ## 🧪 Testing in Swagger
        - **Worker endpoint only** shows authentication requirement
        - **All other endpoints** are open for testing
        - **Production API still requires proper authentication**
        
        ## 📡 Data Flow
        1. Ingestion → 2. Scoring → 3. Chart Calculation → 4. Weekly Publication
        
        *Environment: {ENV} | Last Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}*
        """,
        routes=app.routes,
    )
    
    # =========================
    # SECURITY SCHEMES (Only Worker Auth in Swagger)
    # =========================
    openapi_schema["components"]["securitySchemes"] = {
        "WorkerToken": {
            "type": "apiKey",
            "in": "header", 
            "name": "X-Internal-Token",
            "description": "🔒 Cloudflare Worker authentication token (Required in production)"
        }
    }
    
    # =========================
    # APPLY SECURITY TO ENDPOINTS (Worker Only)
    # =========================
    # ONLY apply security to worker automation endpoint in Swagger
    worker_endpoints = [
        "/automation/weekly/regions",
    ]
    
    for path, methods in openapi_schema["paths"].items():
        for method, details in methods.items():
            # Clear any existing security requirements
            if "security" in details:
                del details["security"]
            
            # Add security ONLY to worker endpoints
            if path in worker_endpoints:
                details["security"] = [{"WorkerToken": []}]
                # Add helpful description
                if "description" not in details:
                    details["description"] = ""
                details["description"] += "\n\n🔒 **Worker Authentication Required**\nUse `X-Internal-Token: 1994199620002019866`"
    
    # =========================
    # ENHANCE ENDPOINT DESCRIPTIONS
    # =========================
    endpoint_categories = {
        "/admin/": {
            "tag": "Admin",
            "description": "🔧 Administrative endpoints for chart management and publishing"
        },
        "/ingest/": {
            "tag": "Ingestion", 
            "description": "📥 Data ingestion endpoints (YouTube, Radio, TV)"
        },
        "/charts/": {
            "tag": "Charts",
            "description": "📊 Chart viewing endpoints (Top 100, Regions, Trending)"
        },
        "/automation/": {
            "tag": "Automation",
            "description": "🤖 Automated worker endpoints"
        },
        "/health": {
            "tag": "Health",
            "description": "❤️ Health check and monitoring endpoints"
        }
    }
    
    for path, methods in openapi_schema["paths"].items():
        for method, details in methods.items():
            # Add category tags
            for prefix, info in endpoint_categories.items():
                if path.startswith(prefix) or path == prefix:
                    details["tags"] = [info["tag"]]
                    if "description" not in details:
                        details["description"] = info["description"]
                    else:
                        details["description"] = info["description"] + "\n\n" + details["description"]
                    break
            
            # Add authentication hints for non-worker endpoints
            if path not in worker_endpoints:
                if "/admin/" in path:
                    auth_hint = "🔐 **Production Authentication:** `Authorization: Bearer admin-ug-board-2025`"
                elif "/ingest/" in path:
                    auth_hint = "🔐 **Production Authentication:** `Authorization: Bearer inject-ug-board-2025`"
                else:
                    auth_hint = "🔓 **No authentication required**"
                
                if "description" not in details:
                    details["description"] = ""
                details["description"] += f"\n\n{auth_hint}"
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

# Assign custom OpenAPI function
app.openapi = custom_openapi

# =========================
# Error handlers
# =========================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if not IS_PROD else "Contact administrator",
            "path": request.url.path,
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# =========================
# Swagger Testing Helper Middleware (Optional)
# =========================

@app.middleware("http")
async def swagger_testing_helper(request: Request, call_next):
    """
    Middleware to help with Swagger testing.
    Adds CORS headers for easier testing.
    """
    response = await call_next(request)
    
    # Add CORS headers for Swagger endpoints
    if request.url.path in ["/docs", "/redoc", "/openapi.json"] and not IS_PROD:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "*"
    
    return response

# =========================
# Startup message
# =========================

if __name__ == "__main__":
    import uvicorn
    print("🚀 UG Board Engine starting...")
    print(f"📊 Environment: {ENV}")
    print(f"📚 Docs: {'Enabled' if not IS_PROD else 'Disabled'}")
    print(f"🔐 Swagger Auth: Worker endpoint only")
    uvicorn.run(app, host="0.0.0.0", port=8000)
