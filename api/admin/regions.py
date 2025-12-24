# api/admin/regions.py

from fastapi import APIRouter, Depends, HTTPException

from data.permissions import ensure_admin_allowed
from data.region_store import is_region_locked
from data.region_snapshots import load_region_snapshot
from data.chart_week import get_current_week_id

router = APIRouter()

VALID_REGIONS = ("Eastern", "Northern", "Western")


@router.get(
    "/regions",
    summary="(Admin) List region publish status",
)
def list_regions(
    _: None = Depends(ensure_admin_allowed),
):
    """
    Admin audit endpoint.

    Shows:
    - Current chart week
    - Lock status per region
    - Snapshot availability
    """
    week_id = get_current_week_id()

    regions = []

    for region in VALID_REGIONS:
        locked = is_region_locked(region)
        snapshot = load_region_snapshot(region) if locked else None

        regions.append(
            {
                "region": region,
                "week_id": week_id,
                "locked": locked,
                "snapshot_ready": snapshot is not None,
                "count": snapshot.get("count", 0) if snapshot else 0,
            }
        )

    return {
        "status": "ok",
        "week_id": week_id,
        "regions": regions,
    }


@router.get(
    "/regions/{region}",
    summary="(Admin) Inspect single region snapshot",
)
def inspect_region(
    region: str,
    _: None = Depends(ensure_admin_allowed),
):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region name",
        )

    week_id = get_current_week_id()
    locked = is_region_locked(region)

    if not locked:
        return {
            "status": "ok",
            "week_id": week_id,
            "region": region,
            "locked": False,
            "snapshot_ready": False,
            "items": [],
        }

    snapshot = load_region_snapshot(region)

    if snapshot is None:
        return {
            "status": "ok",
            "week_id": week_id,
            "region": region,
            "locked": True,
            "snapshot_ready": False,
            "items": [],
        }

    return {
        "status": "ok",
        "week_id": snapshot.get("week_id", week_id),
        "region": region,
        "locked": True,
        "snapshot_ready": True,
        "count": snapshot.get("count", 0),
        "items": snapshot.get("items", []),
    }