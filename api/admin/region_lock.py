# api/admin/region_lock.py

from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

LOCK_FILE = "data/region_locks.json"
REGIONS = ["Eastern", "Northern", "Western"]


def load_locks():
    if not os.path.exists(LOCK_FILE):
        return {r: False for r in REGIONS}

    try:
        with open(LOCK_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {r: False for r in REGIONS}


def save_locks(data):
    os.makedirs("data", exist_ok=True)
    with open(LOCK_FILE, "w") as f:
        json.dump(data, f, indent=2)


@router.post("/admin/regions/lock")
def lock_region(payload: dict):
    region = payload.get("region")

    if region not in REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    locks = load_locks()
    locks[region] = True
    save_locks(locks)

    return {"status": "locked", "region": region}


@router.post("/admin/regions/unlock")
def unlock_region(payload: dict):
    region = payload.get("region")

    if region not in REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    locks = load_locks()
    locks[region] = False
    save_locks(locks)

    return {"status": "unlocked", "region": region}