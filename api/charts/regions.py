from data.store import load_items

REGIONS = ["Eastern", "Northern", "Western"]

def build_region_chart(region: str):
    items = load_items()

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    return region_items[:5]