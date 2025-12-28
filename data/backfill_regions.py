from data.store import load_items, save_items

def backfill_regions():
    items = load_items()
    changed = False

    for item in items:
        if "regions" not in item:
            radio = item.get("radio", 0)
            tv = item.get("tv", 0)

            item["regions"] = {
                "Uganda": radio + tv
            }
            changed = True

    if changed:
        save_items(items)
