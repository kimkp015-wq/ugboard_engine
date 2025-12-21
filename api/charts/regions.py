from fastapi import APIRouter
from data.store import load_items

router = APIRouter()


@router.get("/regions")
def get_regions():
    items = load_items()

    region_map = {}

    for item in items:
        regions = item.get("regions", {})

        for region, plays in regions.items():
            if region not in region_map:
                region_map[region] = []

            region_map[region].append({
                "title": item["title"],
                "artist": item["artist"],
                "score": item.get("score", 0),
                "plays": plays
            })

    # Sort each region
    for region in region_map:
        region_map[region] = sorted(
            region_map[region],
            key=lambda x: x["plays"],
            reverse=True
        )

    return {
        "status": "ok",
        "regions": region_map
    }