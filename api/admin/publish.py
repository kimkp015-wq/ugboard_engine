from fastapi import APIRouter, HTTPException
from datetime import datetime
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.post("/publish/top100")
def publish_top100(payload: dict):
    os.makedirs("data", exist_ok=True)

    if "items" not in payload:
        raise HTTPException(status_code=400, detail="items missing")

    data = {
        "updated_at": datetime.utcnow().isoformat(),
        "items": payload["items"]
    }

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "published",
        "count": len(payload["items"])
    }


@router.post("/publish/top100/lock")
def lock_top100():
    if not os.path.exists(DATA_FILE):
        raise HTTPException(status_code=404, detail="Top100 not found")

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    data["locked"] = True

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {"status": "locked"}