from fastapi import APIRouter, Depends
from typing import List, Dict

from data.permissions import ensure_internal_allowed
from data.store import upsert_item

router = APIRouter()

@router.post(
    "/youtube",
    summary="Ingest YouTube videos (idempotent)",
    description="""
    Idempotent ingestion endpoint for Cloudflare Worker automation.
    
    Authentication: X-Internal-Token header
    For: Cloudflare Workers cron jobs and manual triggers
    
    Engine guarantees:
    - Duplicate (source, external_id) WILL NOT create duplicates
    - Safe to retry
    - Safe for cron / workers
    """,
    dependencies=[Depends(ensure_internal_allowed)],
)
def ingest_youtube(payload: Dict):
    items: List[Dict] = payload.get("items", [])

    ingested = []
    skipped = 0

    for item in items:
        try:
            result = upsert_item(item)
            ingested.append(result)
        except Exception as e:
            # Skip bad items safely - log but don't fail entire batch
            skipped += 1
            continue

    return {
        "status": "ok",
        "received": len(items),
        "ingested": len(ingested),
        "skipped": skipped,
        "message": "Idempotent ingestion complete"
    }
# Add to the end of your ingest_youtube function
def ingest_youtube(payload: Dict):
    # ... existing ingestion code ...
    
    # After successful ingestion, trigger scoring
    try:
        from api.scoring.auto import safe_auto_recalculate
        from data.store import load_items
        
        # Get fresh items including newly ingested
        all_items = load_items()
        safe_auto_recalculate(all_items)
        
        logger.info(f"Auto-scoring triggered after ingestion")
        
    except Exception as e:
        # Don't fail ingestion if scoring fails
        logger.warning(f"Auto-scoring failed: {e}")
    
    return ingestion_result
