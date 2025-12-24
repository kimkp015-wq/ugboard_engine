from fastapi import APIRouter, Depends, HTTPException

from data.permissions import ensure_admin_allowed
from data.region_store import is_region_locked
from data.chart_week import get_current_week_id

router = APIRouter()

VALID_REGIONS = ("Eastern", "Northern", "Western")


def _load_items_safe():
    try:
        from data.store import load_items
        return load_items()
    except Exception:
        return []


@router.post(
    "/regions/{region}/build",
    summary="(Admin) Build & preview region chart (no publish)",
)
def build_region_chart(
    region: str,
    _: None = Depends(ensure_admin_allowed),
):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail="Region is already locked (published)",
        )

    week_id = get_current_week_id()
    items = _load_items_safe()

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True,
    )

    top5 = region_items[:5]

    return {
        "status": "ok",
        "week_id": week_id,
        "region": region,
        "locked": False,
        "count": len(top5),
        "items": top5,
        "note": "Preview only -- not published",
    }