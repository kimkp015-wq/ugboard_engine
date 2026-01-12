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

# =========================
# Startup contract checks
# =========================

def _validate_engine_contracts() -> None:
    """
    Hard fail early if core engine contracts are missing.
    """
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

# Admin
from api.admin.publish import router as publish_router
from api.admin.index import router as admin_index_router
from api.admin.health import router as admin_health_router
from api.admin.regions_build import router as admin_regions_build_router

# =========================
# Register routers (TAGS LIVE HERE ONLY)
# =========================

# Health
app.include_router(
    admin_health_router,
    prefix="/admin",
    tags=["Health"],
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

# Admin
app.include_router(
    publish_router,
    prefix="/admin",
    tags=["Admin"],
)

app.include_router(
    admin_index_router,
    prefix="/admin",
    tags=["Admin"],
)
app.include_router(
    admin_regions_build_router,
    prefix="/admin",
    tags=["Admin"],
)

# =========================
# Custom OpenAPI documentation (MUST BE AFTER app is defined!)
# =========================

from fastapi.openapi.utils import get_openapi

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
