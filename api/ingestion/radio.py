"""
Radio ingestion endpoints for UG Board Engine
FIXED VERSION - No multipart dependency
"""

from fastapi import APIRouter, Header, HTTPException
from datetime import datetime
from typing import Optional, Dict, Any

router = APIRouter()

@router.post("/radio")
async def ingest_radio_data(
    data: dict,
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """
    Ingest radio play data
    
    Worker Authentication: X-Internal-Token: 1994199620002019866
    """
    # Authentication
    if not x_internal_token or x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    try:
        items = data.get("items", [])
        
        if not items:
            return {
                "status": "error",
                "message": "No items provided",
                "received_at": datetime.utcnow().isoformat()
            }
        
        return {
            "status": "success",
            "message": f"Received {len(items)} radio items",
            "count": len(items),
            "processed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Radio ingestion failed: {str(e)}"
        )

@router.post("/radio/scrape")
async def scrape_radio_stations(
    request_data: Optional[Dict[str, Any]] = None,  # ← FIXED: dict instead of list
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """
    Trigger radio scraping for specific stations
    """
    # Authentication
    if not x_internal_token or x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    try:
        # Extract stations from request_data dict
        stations = []
        if request_data and isinstance(request_data, dict):
            stations = request_data.get("stations", [])
        
        stations = stations or ["Capital FM", "Beat FM", "Radio One"]
        
        # Simulate scraping
        scraped_data = []
        for station in stations:
            scraped_data.append({
                "station": station,
                "status": "scraped",
                "songs_found": 5,
                "scraped_at": datetime.utcnow().isoformat()
            })
        
        return {
            "status": "success",
            "message": f"Scraped {len(stations)} radio stations",
            "stations": scraped_data,
            "scraped_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Radio scraping failed: {str(e)}"
        )
