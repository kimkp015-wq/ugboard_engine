from fastapi import APIRouter, Depends, HTTPException
from data.permissions import ensure_admin_allowed
from data.region_store import lock_region, is_region_locked
from data.region_snapshots import save_region_snapshot

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.post(
    "/publish/region/{region}",
    summary="Publish (lock) a region chart",
)
def publish_region(
    region: str,
    _: None = Depends(ensure_admin_allowed),
):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if is_region_locked(region):
        raise HTTPException(status_code=409, detail="Region already published")

    save_region_snapshot(region)
    lock_region(region)

    return {
        "status": "ok",
        "region": region,
        "published": True,
    }