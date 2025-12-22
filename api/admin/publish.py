from fastapi import APIRouter, HTTPException
from datetime import datetime

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


# -----------------------------
# Internal helpers (SAFE)
# -----------------------------

def _lock_region(region: str):
    from data.region_store import lock_region
    lock_region(region)


def _unlock_region(region: str):
    from data.region_store import unlock_region
    unlock_region(region)


def _is_region_locked(region: str) -> bool:
    from data.region_store import is_region_locked
    return is_region_locked(region)


def _audit(action: str, region: str):
    from data.audit import log_audit
    log_audit({
        "action": action,
        "region": region,
        "timestamp": datetime.utcnow().isoformat()
    })


# -----------------------------
# API Endpoints
# -----------------------------

@router.post("/publish/{region}", summary="Publish & freeze a regional chart")
def publish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if _is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} region already published & locked"
        )

    _lock_region(region)
    _audit("publish_region", region)

    return {
        "status": "published",
        "region": region,
        "locked": True
    }


@router.post("/unpublish/{region}", summary="Unfreeze a regional chart (admin only)")
def unpublish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if not _is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} region is not locked"
        )

    _unlock_region(region)
    _audit("unpublish_region", region)

    return {
        "status": "unlocked",
        "region": region,
        "locked": False
    }