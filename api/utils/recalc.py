# api/utils/recalc.py

import json
import os
from api.scoring.scoring import recalculate_all

TOP100_PATH = "data/top100.json"


def auto_recalculate():
    """
    Safely recalculates Top100 scores after ingestion.
    This should be called in background.
    """

    # Load current Top100
    if not os.path.exists(TOP100_PATH):
        return

    with open(TOP100_PATH, "r") as f:
        data = json.load(f)

    items = data.get("items", [])
    if not isinstance(items, list):
        return

    # Merge ingestion metrics
    from data.store import load_items
    metrics = load_items()

    for chart_item in items:
        match = next(
            (
                m for m in metrics
                if m.get("title") == chart_item.get("title")
                and m.get("artist") == chart_item.get("artist")
            ),
            None
        )
        if match:
            chart_item["youtube"] = match.get("youtube", 0)
            chart_item["radio"] = match.get("radio", 0)
            chart_item["tv"] = match.get("tv", 0)

    # Recalculate scores
    items = recalculate_all(items)

    # Sort by updated score
    items = sorted(items, key=lambda x: float(x.get("score", 0)), reverse=True)

    for idx, item in enumerate(items, start=1):
        item["position"] = idx

    data["items"] = items

    # Save back
    with open(TOP100_PATH, "w") as f:
        json.dump(data, f, indent=2)