from fastapi import APIRouter
from datetime import datetime

router = APIRouter()

# This file is RESERVED for GLOBAL / TOP 100 publishing
# Regional publishing is handled exclusively in regions_publish.py


def _audit(action: str, scope: str):
    from data.audit import log_audit
    log_audit({
        "action": action,
        "scope": scope,
        "timestamp": datetime.utcnow().isoformat()
    })


@router.post("/top100/publish", summary="Publish & freeze Top 100 chart")
def publish_top100():
    from data.top100_store import lock_top100

    lock_top100()
    _audit("publish_top100", "global")

    return {
        "status": "published",
        "chart": "top100",
        "locked": True
    }


@router.post("/top100/unpublish", summary="Unfreeze Top 100 chart")
def unpublish_top100():
    from data.top100_store import unlock_top100

    unlock_top100()
    _audit("unpublish_top100", "global")

    return {
        "status": "unlocked",
        "chart": "top100",
        "locked": False
    }