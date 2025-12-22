# api/admin/publish.py

from fastapi import APIRouter, HTTPException
from datetime import datetime

from data.store import load_items
from data.region_store import lock_region, unlock_region, is_region_locked
from data.audit import log_audit

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


# =====================================================
# TOP 100 PUBLISH (GLOBAL)
# =====================================================

@router.post("/publish/top100", summary="Publish & freeze Top 100 chart")
def publish_top100():
    """
    Freezes Top 100 (logical flag only, no deletion).
    """

    log_audit({
        "action": "publish_top100",
        "timestamp": datetime.utcnow().isoformat()
    })

    return {
        "status": "published",
        "chart": "top100",
        "locked": True
    }


# =====================================================
# REGION PUBLISH / UNPUBLISH
# =====================================================

@router.post("/publish/{region}", summary="Publish & freeze a regional chart")
def publish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} region already published & locked"
        )

    lock_region(region)

    log_audit({
        "action": "publish_region",
        "region": region,
        "timestamp": datetime.utcnow().isoformat()
    })

    return {
        "status": "published",
        "region": region,
        "locked": True
    }


@router.post("/unpublish/{region}", summary="Unfreeze a regional chart")
def unpublish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if not is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} region is not locked"
        )

    unlock_region(region)

    log_audit({
        "action": "unpublish_region",
        "region": region,
        "timestamp": datetime.utcnow().isoformat()
    })

    return {
        "status": "unlocked",
        "region": region,
        "locked": False
    }