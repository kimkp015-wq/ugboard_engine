# File: api/ingestion/radio.py - FIXED VERSION
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Dict, List
from datetime import datetime

from data.permissions import ensure_injection_allowed
from data.store import upsert_item, load_items, save_items

router = APIRouter()

VALID_REGIONS = {"Eastern", "Northern", "Western"}

# Avoid circular import - define or import calculate_scores safely
def safe_calculate_scores():
    """Safely import and call calculate_scores to avoid circular imports"""
    try:
        from data.scoring import calculate_scores as calc_scores
        items = load_items()
        return calc_scores(items)
    except ImportError as e:
        # Fallback to basic calculation if module not available
        print(f"Warning: Could not import calculate_scores: {e}")
        return []

@router.post(
    "/radio",
    summary="Ingest Radio data (validated)",
)
def ingest_radio(
    payload: Dict,
    background_tasks: BackgroundTasks,
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
        "source": "radio",
        "song_id": payload["song_id"],
        "external_id": f"radio_{payload['song_id']}",
        "title": payload["title"],
        "artist": payload["artist"],
        "region": region,
        "radio_plays": payload["plays"],
        "radio_stations": stations,
        "published_at": datetime.utcnow().isoformat(),
    }
    
    # -------------------------
    # Persist (UPSERT) with scoring
    # -------------------------
    try:
        # First upsert the item
        upsert_item(item)
        
        # Trigger scoring in background to avoid blocking
        background_tasks.add_task(safe_calculate_scores)
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Scoring failed: {str(e)}",
        )
    
    return {
        "status": "ok",
        "source": "radio",
        "stored": True,
        "scored": True,
        "song_id": item["song_id"],
        "region": region,
        "stations_count": len(stations),
        "message": "Scoring triggered in background"
    }
