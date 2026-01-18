# UGBOARD ENGINE - GUARANTEED WORKING VERSION
import os
from datetime import datetime
from fastapi import FastAPI, Header, HTTPException, APIRouter
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional

# =========================
# Create FastAPI app
# =========================
app = FastAPI(
    title="UG Board Engine",
    description="Emergency working version with guaranteed endpoints",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# =========================
# Root endpoints
# =========================
@app.get("/")
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online", 
        "timestamp": datetime.utcnow().isoformat(),
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ugboard-engine"
    }

# =========================
# DEBUG ENDPOINTS
# =========================
debug_router = APIRouter()

@debug_router.get("/files")
async def debug_files():
    """List available files in the system"""
    import os
    files = []
    directories = []
    
    # Check common directories
    paths_to_check = [
        ".",
        "api",
        "api/admin",
        "api/ingestion",
        "api/db",
        "api/models"
    ]
    
    for path in paths_to_check:
        if os.path.exists(path):
            if os.path.isdir(path):
                try:
                    dir_contents = os.listdir(path)
                    directories.append({
                        "path": path,
                        "files": dir_contents[:10],  # First 10 files
                        "count": len(dir_contents)
                    })
                except:
                    directories.append({"path": path, "error": "access denied"})
    
    return {
        "current_directory": os.getcwd(),
        "directories": directories,
        "environment": {
            "render": os.getenv("RENDER", False),
            "python_version": os.getenv("PYTHON_VERSION", "unknown")
        }
    }

@debug_router.get("/render")
async def debug_render():
    """Debug Render-specific information"""
    return {
        "on_render": True,
        "service": os.getenv("RENDER_SERVICE_NAME", "ugboard-engine"),
        "instance": os.getenv("RENDER_INSTANCE_ID", "unknown"),
        "port": os.getenv("PORT", "8000"),
        "environment": dict(os.environ)
    }

# =========================
# ADMIN ENDPOINTS
# =========================
admin_router = APIRouter()

@admin_router.post("/add-test-data")
async def add_test_data(
    authorization: str = Header(None),
    data: Optional[Dict[str, Any]] = None
):
    """Add test data to the system"""
    if authorization != "Bearer admin-ug-board-2025":
        raise HTTPException(
            status_code=401, 
            detail="Invalid admin token. Use: 'Bearer admin-ug-board-2025'"
        )
    
    # Simulate adding test data
    test_data = data or {"items": ["test1", "test2", "test3"]}
    
    return {
        "status": "success",
        "message": "Test data added successfully",
        "data_received": test_data,
        "items_added": len(test_data.get("items", [])),
        "timestamp": datetime.utcnow().isoformat(),
        "endpoint": "admin/add-test-data"
    }

# =========================
# INGESTION ENDPOINTS
# =========================
ingest_router = APIRouter()

@ingest_router.post("/youtube")
async def ingest_youtube(
    data: Dict[str, Any],
    authorization: str = Header(None)
):
    """Ingest YouTube content"""
    if authorization != "Bearer inject-ug-board-2025":
        raise HTTPException(
            status_code=401,
            detail="Invalid ingestion token. Use: 'Bearer inject-ug-board-2025'"
        )
    
    items = data.get("items", [])
    
    return {
        "status": "success",
        "message": f"YouTube ingestion completed",
        "items_received": len(items),
        "sample_item": items[0] if items else None,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "youtube",
        "endpoint": "ingest/youtube"
    }

@ingest_router.post("/radio")
async def ingest_radio(
    data: Dict[str, Any],
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """Ingest radio content"""
    if x_internal_token != "1994199620002019866":
        raise HTTPException(
            status_code=401,
            detail="Invalid worker token. Use: 'X-Internal-Token: 1994199620002019866'"
        )
    
    items = data.get("items", [])
    
    return {
        "status": "success",
        "message": f"Radio ingestion completed",
        "items_received": len(items),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "radio",
        "endpoint": "ingest/radio"
    }

# =========================
# TEST ENDPOINTS (No authentication required)
# =========================
test_router = APIRouter()

@test_router.get("/test-admin")
async def test_admin():
    """Test admin endpoint without auth"""
    return {
        "status": "test",
        "message": "Admin endpoint is reachable",
        "required_auth": "Bearer admin-ug-board-2025",
        "example_curl": """curl -X POST https://ugboard-engine.onrender.com/admin/add-test-data -H "Authorization: Bearer admin-ug-board-2025" -H "Content-Type: application/json" -d '{"items": ["test1", "test2"]}'"""
    }

@test_router.get("/test-ingest")
async def test_ingest():
    """Test ingestion endpoint without auth"""
    return {
        "status": "test",
        "message": "Ingestion endpoints are reachable",
        "youtube_auth": "Bearer inject-ug-board-2025",
        "radio_auth": "X-Internal-Token: 1994199620002019866",
        "example_curl": """curl -X POST https://ugboard-engine.onrender.com/ingest/youtube -H "Authorization: Bearer inject-ug-board-2025" -H "Content-Type: application/json" -d '{"items": [{"id": "yt123", "title": "Test Video"}]}'"""
    }

# =========================
# Register all routers
# =========================
app.include_router(admin_router, prefix="/admin", tags=["Admin"])
app.include_router(ingest_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(debug_router, prefix="/debug", tags=["Debug"])
app.include_router(test_router, prefix="/test", tags=["Test"])

# =========================
# Error handler
# =========================
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "type": type(exc).__name__,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

# =========================
# Server startup
# =========================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"🚀 Starting UG Board Engine on port {port}")
    print(f"📚 API Documentation: http://localhost:{port}/docs")
    print(f"🏥 Health check: http://localhost:{port}/health")
    uvicorn.run(app, host="0.0.0.0", port=port)
