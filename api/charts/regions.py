# api/charts/regions.py

def load_region_locks():
    path = "data/region_locks.json"
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}
from fastapi import APIRouter
from data.store import load_items
import json
import os

router = APIRouter()

TOP100_PATH = "data/top100.json"
REGIONS = ["Eastern", "Northern", "Western"]


def load_top100_items():
    if not os.path.exists(TOP100_PATH):
        return []

    try:
        with open(TOP100_PATH, "r") as f:
            data = json.load(f)
            return data.get("items", [])
    except Exception:
        return []


@router.get("/regions")
def get_regions():
    all_items = load_items()
    top100_items = load_top100_items()

    response = {}

    for region in REGIONS:
        # 1️⃣ From Top 100
        region_top = [
            i for i in top100_items
            if i.get("region") == region
        ]

        region_top = sorted(
            region_top,
            key=lambda x: x.get("score", 0),
            reverse=True
        )

        # 2️⃣ Fallback: admin injected
        if len(region_top) < 5:
            needed = 5 - len(region_top)

            injected = [
                i for i in all_items
                if i.get("region") == region
                and i.get("admin_injected") is True
                and i not in region_top
            ]

            injected = sorted(
                injected,
                key=lambda x: x.get("score", 0),
                reverse=True
            )

            region_top.extend(injected[:needed])

        response[region] = {
            "count": len(region_top),
            "items": region_top[:5]
        }

    return {
        "status": "ok",
        "regions": locks = load_region_locks()
response = {}
    }