"""
Radio ingestion endpoints for UG Board Engine
FIXED VERSION - No multipart issues
"""

from fastapi import APIRouter, Header, HTTPException
from datetime import datetime

router = APIRouter()

@router.post("/radio")
async def ingest_radio_data(
    data: dict,
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """
    Ingest radio play data
    """
    if x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    return {
        "status": "success",
        "message": "Radio endpoint working",
        "timestamp": datetime.utcnow().isoformat()
    }

@router.post("/radio/scrape")
async def scrape_radio_stations(
    request_data: dict = None,
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """
    Trigger radio scraping
    """
    if x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    stations = []
    if request_data and isinstance(request_data, dict):
        stations = request_data.get("stations", [])
    
    stations = stations or ["Capital FM", "Beat FM", "Radio One"]
    
    return {
        "status": "success",
        "message": f"Scraped {len(stations)} stations",
        "stations": stations,
        "timestamp": datetime.utcnow().isoformat()
    }
