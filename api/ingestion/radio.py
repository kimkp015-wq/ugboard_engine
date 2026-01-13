from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, List

from data.permissions import ensure_injection_allowed
from data.store import upsert_item
# ADD THIS IMPORT
from data.scoring import calculate_scores

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
        "source": "radio",  # ADD THIS
        "song_id": payload["song_id"],
        "external_id": f"radio_{payload['song_id']}",  # ADD THIS for consistency
        "title": payload["title"],
        "artist": payload["artist"],
        "region": region,
        "radio_plays": payload["plays"],
        "radio_stations": stations,
        "published_at": datetime.utcnow().isoformat(),  # ADD THIS
    }

    # -------------------------
    # Persist (UPSERT) with scoring
    # -------------------------
    try:
        # First upsert the item
        upsert_item(item)
        
        # FIX: Trigger scoring after ingestion
        from data.store import load_items
        all_items = load_items()
        calculate_scores(all_items)  # ← TRIGGER SCORING
        
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
        "scored": True,  # ADD THIS
        "song_id": item["song_id"],
        "region": region,
        "stations_count": len(stations),
    }
