# EMERGENCY main.py - Skip radio import if it fails
import os
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Depends, HTTPException, Body
from fastapi.responses import JSONResponse

# =========================
# Create app
# =========================
app = FastAPI(title="UG Board Engine", docs_url="/docs", redoc_url="/redoc")

# =========================
# Add to top of main.py (after app creation)
# =========================
from fastapi import APIRouter

# Create simple admin router
admin_router = APIRouter()

@admin_router.post("/add-test-data")
async def add_test_data():
    return {"status": "test_data_added"}

# Create simple ingestion router  
ingest_router = APIRouter()

@ingest_router.post("/youtube")
async def ingest_youtube():
    return {"status": "youtube_ingestion_working"}

# =========================
# Create emergency radio router INLINE
# =========================
from fastapi import APIRouter, Header
radio_router = APIRouter()

@radio_router.post("/radio")
async def emergency_radio(data: dict, x_internal_token: str = Header(None)):
    """Emergency radio endpoint"""
    if x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"status": "emergency_radio_working"}

# =========================
# Register routers
# =========================
app.include_router(admin_router, prefix="/admin", tags=["Admin"])
app.include_router(ingest_router, prefix="/ingest", tags=["Ingestion"])
app.include_router(radio_router, prefix="/ingest", tags=["Ingestion"])

# =========================
# Root endpoints
# =========================
@app.get("/")
def root():
    return {"status": "online", "service": "UG Board Engine"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# =========================
# Start server
# =========================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
