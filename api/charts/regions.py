# api/charts/regions.py

from fastapi import APIRouter
import json
import os
from data.store import load_items

router = APIRouter()

REGIONS = ["Eastern", "Northern", "Western"]


def load_region_locks():
    path = "data/region_locks.json"
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


@router.get("/regions")
def get_regions():
    items = load_items()
    locks = load_region_locks()

    response = {}

    for region in REGIONS:
        # 🔒 If region is locked → frozen output
        if locks.get(region) is True:
            response[region] = {
                "locked": True,
                "count": 0,
                "items": []
            }
            continue

        # 🧠 Region logic (ADMIN + future tagging will improve this)
        region_items = [
            i for i in items
            if i.get("region") == region
        ]

        region_items = sorted(
            region_items,
            key=lambda x: x.get("score", 0),
            reverse=True
        )

        response[region] = {
            "locked": False,
            "count": min(5, len(region_items)),
            "items": region_items[:5]
        }

    return {
        "status": "ok",
        "regions": response
    }