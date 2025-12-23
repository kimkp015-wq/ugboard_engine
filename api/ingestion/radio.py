from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, List

from data.permissions import ensure_injection_allowed

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
    Accepts validated Radio ingestion payload.
    Does NOT write to store yet.
    """

    # -------------------------
    # Basic structure check
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
    # Region validation
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
    # ACCEPTED (no persistence yet)
    # -------------------------
    return {
        "status": "ok",
        "source": "radio",
        "accepted": True,
        "song_id": payload["song_id"],
        "region": region,
        "stations_count": len(stations),
    }