# api/ingestion/youtube.py

from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict

from data.permissions import ensure_ingest_allowed
from data.store import upsert_items

router = APIRouter()

# =========================
# Payload contract
# =========================

class IngestPayload(Dict):
    """
    Expected item shape (already normalized by worker):

    {
      "source": "youtube",
      "video_id": "...",
      "title": "...",
      "channel_id": "...",
      "published_at": "...",
      "score": 0
    }
    """
    pass


# =========================
# Ingestion endpoint
# =========================

@router.post(
    "/youtube",
    summary="Ingest YouTube videos (worker-only)",
)
def ingest_youtube(
    payload: Dict,
    _: None = Depends(ensure_ingest_allowed),
):
    """
    Secure ingestion endpoint.

    Guarantees:
    - Token-protected
    - Idempotent (video_id is key)
    - Never crashes engine
    - Bulk-safe
    """
    items = payload.get("items")

    if not isinstance(items, list):
        raise HTTPException(
            status_code=400,
            detail="Invalid payload: items must be a list",
        )

    accepted = 0

    for item in items:
        if not _is_valid_item(item):
            continue

        try:
            upsert_items([item])
            accepted += 1
        except Exception:
            # Never crash ingestion loop
            continue

    return {
        "status": "ok",
        "received": len(items),
        "accepted": accepted,
    }


# =========================
# Validation (STRICT)
# =========================

REQUIRED_FIELDS = (
    "source",
    "video_id",
    "title",
    "channel_id",
    "published_at",
)

def _is_valid_item(item: Dict) -> bool:
    if not isinstance(item, dict):
        return False

    if item.get("source") != "youtube":
        return False

    for field in REQUIRED_FIELDS:
        if field not in item:
            return False

    return True