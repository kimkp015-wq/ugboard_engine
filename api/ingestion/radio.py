from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, List

from data.permissions import ensure_injection_allowed
from data.store import upsert_item

router = APIRouter()

VALID_REGIONS = {"Eastern", "Northern", "Western"}


@router.post(
    "/radio",
    summary="Ingest Radio data (validated)",
)
def ingest_radio(
    payload: Dict,
    _: None = Depends(ensure_injection_allowed),
):
    """
    Ingest Radio data.

    Guarantees:
    - song_id is primary key
    - Idempotent (safe to resend)
    - Radio plays merged into existing item
    - Score recalculated centrally
    """

    # -------------------------
    # Payload structure
    # -------------------------
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail="Payload must be a JSON object",
        )

    # -------------------------
    # Required fields
    # -------------------------
    required_fields = {
        "song_id": str,
        "title": str,
        "artist": str,
        "region": str,
        "plays": int,
        "stations": list,
    }

    for field, expected_type in required_fields.items():
        if field not in payload:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required field: {field}",
            )

        if not isinstance(payload[field], expected_type):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid type for '{field}'",
            )

    # -------------------------
    # Normalize & validate region
    # -------------------------
    region = payload["region"].title()
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region",
        )

    # -------------------------
    # Plays sanity check
    # -------------------------
    if payload["plays"] < 0:
        raise HTTPException(
            status_code=400,
            detail="Plays must be >= 0",
        )

    # -------------------------
    # Stations validation
    # -------------------------
    stations: List = payload["stations"]

    if not stations:
        raise HTTPException(
            status_code=400,
            detail="Stations list cannot be empty",
        )

    for station in stations:
        if not isinstance(station, str):
            raise HTTPException(
                status_code=400,
                detail="Each station must be a string",
            )

    # -------------------------
    # Canonical merge payload
    # -------------------------
    item = {
        "song_id": payload["song_id"],
        "title": payload["title"],
        "artist": payload["artist"],
        "region": region,
        "radio_plays": payload["plays"],
        "radio_stations": stations,
    }

    # -------------------------
    # Persist (UPSERT)
    # -------------------------
    try:
        upsert_item(item)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )

    return {
        "status": "ok",
        "source": "radio",
        "stored": True,
        "song_id": item["song_id"],
        "region": region,
        "stations_count": len(stations),
    }
# Add these imports at the top if not there
import os
from datetime import datetime

# Add this function at the end (before the last lines)

@router.post("/radio/scrape")
async def scrape_radio_stations(
    x_internal_token: str = None
):
    """
    Automatically scrape radio stations for current songs.
    Cloudflare Worker will call this every 30 minutes.
    """
    # Verify it's our Cloudflare Worker calling
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Invalid token. Only Cloudflare Worker can call this."
        )
    
    try:
        # Import and run scraper
        from .radio_scraper import UgandaRadioScraper
        
        scraper = UgandaRadioScraper()
        result = await scraper.scrape_and_save()
        
        return {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "data": result
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Radio scraping failed: {str(e)}"
        )

@router.get("/radio/test")
async def test_radio_scraper(
    x_internal_token: str = None
):
    """Test endpoint to see if radio scraping works"""
    INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    
    from .radio_scraper import UgandaRadioScraper
    
    scraper = UgandaRadioScraper()
    songs = await scraper.scrape_all()
    
    return {
        "stations_tested": len(scraper.stations),
        "songs_found": len(songs),
        "songs": songs
    }
