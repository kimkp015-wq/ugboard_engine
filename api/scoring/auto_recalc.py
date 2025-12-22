from data.region_store import is_region_locked
from data.store import load_items, save_items
from data.regions_live import save_regions_live

def recalc_regions():
    items = load_items()

    regions = {
        "Eastern": [],
        "Northern": [],
        "Western": []
    }

    for item in items:
        region = item.get("region")
        if region in regions and not is_region_locked(region):
            regions[region].append(item)

    for region in regions:
        regions[region] = sorted(
            regions[region],
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:5]

    save_regions_live(regions)