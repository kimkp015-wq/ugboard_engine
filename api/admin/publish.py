# api/admin/publish.py

from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime

from data.region_store import (
    lock_region,
    unlock_region,
    is_region_locked,
)
from data.audit import log_audit
from data.chart_week import is_tracking_open
from data.permissions import ensure_admin_allowed

router = APIRouter()

# Canonical region list (single source)
VALID_REGIONS = {"Eastern", "Northern", "Western"}


def normalize_region(region: str) -> str:
    return region.strip().title()


# =========================
# PUBLISH (LOCK)
# =========================
@router.post(
    "/publish/{region}",
    summary="Publish & lock a regional chart",
)
def publish_region(
    region: str,
    _: None = Depends(ensure_admin_allowed),
):
    region = normalize_region(region)

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    # Guardrail: never publish while tracking is open
    if is_tracking_open():
        raise HTTPException(
            status_code=409,
            detail="Tracking window still open",
        )

    # Idempotent safety
    if is_region_locked(region):
        return {
            "status": "ok",
            "region": region,
            "locked": True,
            "note": "Already published",
        }

    lock_region(region)

    log_audit(
        action="publish_region",
        region=region,
        timestamp=datetime.utcnow().isoformat(),
    )

    return {
        "status": "ok",
        "region": region,
        "locked": True,
    }


# =========================
# UNPUBLISH (UNLOCK)
# =========================
@router.post(
    "/unpublish/{region}",
    summary="Unpublish & unlock a regional chart (ADMIN ONLY)",
)
def unpublish_region(
    region: str,
    _: None = Depends(ensure_admin_allowed),
):
    region = normalize_region(region)

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    # Idempotent safety
    if not is_region_locked(region):
        return {
            "status": "ok",
            "region": region,
            "locked": False,
            "note": "Already unlocked",
        }

    unlock_region(region)

    log_audit(
        action="unpublish_region",
        region=region,
        timestamp=datetime.utcnow().isoformat(),
    )

    return {
        "status": "ok",
        "region": region,
        "locked": False,
    }