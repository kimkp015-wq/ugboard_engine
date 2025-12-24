# api/ingestion/youtube.py

from fastapi import APIRouter, Depends, HTTPException
from typing import Dict

from data.permissions import ensure_injection_allowed
from data.store import upsert_item

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
    Ingest YouTube data.

    Guarantees:
    - song_id is primary key
    - Idempotent (safe to resend)
    - Auth required (Swagger popup enabled)
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
    # Normalize & validate region
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
    # Build canonical item
    # -------------------------
    item = {
        "song_id": payload["song_id"],
        "title": payload["title"],
        "artist": payload["artist"],
        "region": region,
        "youtube_views": payload["views"],
        # score placeholder (future engine logic)
        "score": payload["views"],
        "source": "youtube",
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
        "source": "youtube",
        "stored": True,
        "song_id": item["song_id"],
        "region": region,
    }