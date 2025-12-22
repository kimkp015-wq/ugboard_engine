# api/charts/regions.py

from fastapi import APIRouter
from data.store import load_items

router = APIRouter()

SUPPORTED_REGIONS = {
    "eastern": "Eastern Region",
    "northern": "Northern Region",
    "western": "Western Region",
}


def normalize_region(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().lower()


@router.get("/regions")
def get_all_regions():
    """
    Returns Top 5 songs per region.
    Safe, read-only, never crashes.
    """

    items = load_items()

    # Prepare buckets
    region_buckets = {key: [] for key in SUPPORTED_REGIONS}

    for item in items:
        region = normalize_region(item.get("region"))

        if region in region_buckets:
            region_buckets[region].append(item)

    response = {}

    for key, label in SUPPORTED_REGIONS.items():
        songs = region_buckets[key]

        # Sort by score DESC, admin-injected always included
        songs = sorted(
            songs,
            key=lambda x: (
                not x.get("admin_injected", False),
                -float(x.get("score", 0)),
            ),
        )

        top_five = []

        for index, song in enumerate(songs[:5], start=1):
            top_five.append(
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
            "count": len(top_five),
            "items": top_five,
        }

    return {
        "status": "ok",
        "regions": response,
    }