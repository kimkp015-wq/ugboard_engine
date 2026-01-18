# EMERGENCY main.py - Skip radio import if it fails
import os
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Depends, HTTPException, Body
from fastapi.responses import JSONResponse

# =========================
# Create app
# =========================
app = FastAPI(title="UG Board Engine", docs_url="/docs", redoc_url="/redoc")

@app.get("/")
def root():
    return {"status": "online", "service": "UG Board Engine"}

@app.get("/health")
def health():
    return {"status": "healthy"}

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
app.include_router(radio_router, prefix="/ingest", tags=["Ingestion"])

# =========================
# Start server
# =========================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
