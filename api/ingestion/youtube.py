from fastapi import APIRouter, Depends
from typing import List, Dict

from data.permissions import ensure_injection_allowed
from data.store import upsert_item

router = APIRouter()


@router.post(
    "/youtube",
    summary="Ingest YouTube videos (idempotent)",
    description="""
    Idempotent ingestion endpoint.

    Engine guarantees:
    - Duplicate (source, external_id) WILL NOT create duplicates
    - Safe to retry
    - Safe for cron / workers

    Source of truth: engine, not client.
    """,
)
def ingest_youtube(
    payload: Dict,
    _: None = Depends(ensure_injection_allowed),
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