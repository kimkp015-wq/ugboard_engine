from fastapi import APIRouter, Depends
from typing import List, Dict

from data.permissions import ensure_internal_allowed  # ← CHANGE THIS
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
    dependencies=[Depends(ensure_internal_allowed)],  # ← ADD THIS
)
def ingest_youtube(
    payload: Dict,
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
