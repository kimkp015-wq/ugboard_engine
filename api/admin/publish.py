# api/admin/publish.py

from fastapi import APIRouter, HTTPException
from datetime import datetime

from data.region_store import (
    lock_region,
    unlock_region,
    is_region_locked
)
from data.audit import log_audit

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.post("/publish/{region}")
def publish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} already published"
        )

    lock_region(region)

    log_audit({
        "action": "publish_region",
        "region": region,
        "timestamp": datetime.utcnow().isoformat()
    })

    return {
        "status": "ok",
        "region": region,
        "locked": True
    }


@router.post("/unpublish/{region}")
def unpublish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if not is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} is not locked"
        )

    unlock_region(region)

    log_audit({
        "action": "unpublish_region",
        "region": region,
        "timestamp": datetime.utcnow().isoformat()
    })

    return {
        "status": "ok",
        "region": region,
        "locked": False
    }