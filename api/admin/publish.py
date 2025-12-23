from fastapi import APIRouter, Depends

from data.permissions import ensure_admin_allowed
from data.region_store import publish_region

router = APIRouter()


@router.post(
    "/publish/{region}",
    summary="Publish (lock) a region chart",
)
def publish_region_chart(
    region: str,
    _: None = Depends(ensure_admin_allowed),
):
    region = region.title()

    if region not in ("Eastern", "Northern", "Western"):
        return {
            "status": "error",
            "message": "Invalid region",
        }

    publish_region(region)

    return {
        "status": "ok",
        "region": region,
        "published": True,
    }