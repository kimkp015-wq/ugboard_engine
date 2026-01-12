from fastapi import APIRouter, Depends
from typing import List, Dict

# CHANGE THIS IMPORT
from data.permissions import ensure_internal_allowed  # ← NEW
from data.store import upsert_item

router = APIRouter()

@router.post(
    "/youtube",
    summary="Ingest YouTube videos (idempotent)",
    description="""
    Idempotent ingestion endpoint for internal automation.
    
    Authentication: X-Internal-Token header
    For Cloudflare Workers and cron jobs.
    """,
    # ADD THIS DEPENDENCY
    dependencies=[Depends(ensure_internal_allowed)],  # ← NEW
)
def ingest_youtube(
    payload: Dict,
    # REMOVE THIS PARAMETER
    # _: None = Depends(ensure_injection_allowed),  # ← REMOVE
):
    items: List[Dict] = payload.get("items", [])

    ingested = []

    for item in items:
        try:
            ingested.append(upsert_item(item))
        except Exception:
            # Skip bad items safely
            continue

    return {
        "status": "ok",
        "received": len(items),
        "ingested": len(ingested),
    }
