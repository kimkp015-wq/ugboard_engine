from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, List

from data.permissions import ensure_injection_allowed

router = APIRouter()

VALID_REGIONS = {"Eastern", "Northern", "Western"}


@router.post(
    "/tv",
    summary="Ingest TV data (validated)",
)
def ingest_tv(
    payload: Dict,
    _: None = Depends(ensure_injection_allowed),
):
    """
    Accepts validated TV ingestion payload.
    No persistence yet (safe edge).
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
        "appearances": int,
        "channels": list,
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
    # Appearances sanity check
    # -------------------------
    if payload["appearances"] < 0:
        raise HTTPException(
            status_code=400,
            detail="Appearances must be >= 0",
        )

    # -------------------------
    # Channels validation
    # -------------------------
    channels: List = payload["channels"]

    if not channels:
        raise HTTPException(
            status_code=400,
            detail="Channels list cannot be empty",
        )

    for channel in channels:
        if not isinstance(channel, str):
            raise HTTPException(
                status_code=400,
                detail="Each channel must be a string",
            )

    # -------------------------
    # ACCEPTED (no persistence yet)
    # -------------------------
    return {
        "status": "ok",
        "source": "tv",
        "accepted": True,
        "song_id": payload["song_id"],
        "region": region,
        "channels_count": len(channels),
    }