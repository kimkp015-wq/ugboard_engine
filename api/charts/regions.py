from fastapi import APIRouter, HTTPException
from data.region_snapshots import load_region_snapshot

router = APIRouter()

@router.get("/regions/{region}")
def get_region_chart(region: str):
    snapshot = load_region_snapshot(region)
    if not snapshot:
        raise HTTPException(
            status_code=404,
            detail="Region chart not yet published"
        )
    return snapshot