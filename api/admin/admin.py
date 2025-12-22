# api/admin/admin.py

from fastapi import APIRouter, HTTPException
from data.store import load_region_locks, save_region_locks

router = APIRouter()

VALID_REGIONS = {"eastern", "northern", "western"}


@router.get("/admin/regions/locks")
def get_region_locks():
    return {
        "status": "ok",
        "locks": load_region_locks()
    }


@router.post("/admin/regions/{region}/lock")
def lock_region(region: str):
    region = region.lower()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    locks = load_region_locks()
    locks[region] = True
    save_region_locks(locks)

    return {"status": "locked", "region": region}


@router.post("/admin/regions/{region}/unlock")
def unlock_region(region: str):
    region = region.lower()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    locks = load_region_locks()
    locks[region] = False
    save_region_locks(locks)

    return {"status": "unlocked", "region": region}