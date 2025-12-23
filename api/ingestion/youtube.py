from fastapi import APIRouter, Depends, HTTPException
from typing import Dict

from data.permissions import ensure_injection_allowed

router = APIRouter()

VALID_REGIONS = {"Eastern", "Northern", "Western"}


@router.post(
    "/youtube",
    summary="Ingest YouTube data (validated)",
)
def ingest_youtube(
    payload: Dict,
    _: None = Depends(ensure_injection_allowed),
):
    """
    Accepts validated YouTube ingestion payload.
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
        "views": int,
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
    # Views sanity check
    # -------------------------
    if payload["views"] < 0:
        raise HTTPException(
            status_code=400,
            detail="Views must be >= 0",
        )

    # -------------------------
    # ACCEPTED (no persistence yet)
    # -------------------------
    return {
        "status": "ok",
        "source": "youtube",
        "accepted": True,
        "song_id": payload["song_id"],
        "region": region,
    }