"""
Radio ingestion endpoints for UG Board Engine
Simple version that works with the inline scoring functions
"""

from fastapi import APIRouter, Header, HTTPException
from datetime import datetime

router = APIRouter()

# Simple scoring function (fallback if data.scoring import fails)
def compute_score_simple(item):
    """Simple scoring as fallback"""
    youtube_views = item.get("youtube_views", 0) or 0
    radio_plays = item.get("radio_plays", 0) or 0
    tv_appearances = item.get("tv_appearances", 0) or 0
    return (youtube_views * 1) + (radio_plays * 500) + (tv_appearances * 1000)

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
        
        # Process each item
        processed_items = []
        for item in items:
            # Add score using simple function
            item_copy = dict(item)
            item_copy["score"] = compute_score_simple(item_copy)
            item_copy["processed_at"] = datetime.utcnow().isoformat()
            item_copy["source"] = "radio"
            processed_items.append(item_copy)
        
        # TODO: Save to data store
        # For now, just return success
        
        return {
            "status": "success",
            "message": f"Processed {len(processed_items)} radio items",
            "count": len(processed_items),
            "processed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Radio ingestion failed: {str(e)}"
        )

@router.post("/radio/scrape")
async def scrape_radio_stations(
    stations: list = None,
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """
    Trigger radio scraping for specific stations
    """
    # Authentication
    if not x_internal_token or x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    try:
        stations = stations or ["Capital FM", "Beat FM", "Radio One"]
        
        # Simulate scraping
        scraped_data = []
        for station in stations:
            scraped_data.append({
                "station": station,
                "status": "scraped",
                "songs_found": 5,  # Example
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
