# api/charts/regions.py

from fastapi import APIRouter
from data.store import load_items, load_region_locks

router = APIRouter()

SUPPORTED_REGIONS = {
    "eastern": "Eastern Region",
    "northern": "Northern Region",
    "western": "Western Region",
}


def normalize_region(value):
    if not value:
        return None
    return value.strip().lower()


@router.get("/regions")
def get_regions():
    items = load_items()
    locks = load_region_locks()

    region_buckets = {key: [] for key in SUPPORTED_REGIONS}

    for item in items:
        region = normalize_region(item.get("region"))
        if region in region_buckets:
            region_buckets[region].append(item)

    response = {}

    for key, label in SUPPORTED_REGIONS.items():
        songs = region_buckets[key]

        if not locks.get(key, False):
            # Only sort if NOT locked
            songs = sorted(
                songs,
                key=lambda x: (
                    not x.get("admin_injected", False),
                    -float(x.get("score", 0)),
                ),
            )

        top = []
        for index, song in enumerate(songs[:5], start=1):
            top.append(
                {
                    "position": index,
                    "title": song.get("title"),
                    "artist": song.get("artist"),
                    "score": song.get("score", 0),
                    "admin_injected": song.get("admin_injected", False),
                }
            )

        response[key] = {
            "region": label,
            "locked": locks.get(key, False),
            "count": len(top),
            "items": top,
        }

    return {
        "status": "ok",
        "regions": response,
    }