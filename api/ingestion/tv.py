from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, List

from data.permissions import ensure_injection_allowed
from data.store import upsert_item

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
    Ingest TV data.

    Guarantees:
    - song_id is primary key
    - Idempotent (safe to resend)
    - TV appearances merged into existing item
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
    # Normalize & validate region
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
    # Build canonical item
    # -------------------------
    item = {
        "song_id": payload["song_id"],
        "title": payload["title"],
        "artist": payload["artist"],
        "region": region,
        "tv_appearances": payload["appearances"],
        "tv_channels": channels,
        # Temporary scoring (merged later)
        "score": payload["appearances"],
        "source": "tv",
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
        "source": "tv",
        "stored": True,
        "song_id": item["song_id"],
        "region": region,
        "channels_count": len(channels),
    }