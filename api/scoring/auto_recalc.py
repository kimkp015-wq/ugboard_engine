# api/scoring/auto_recalc.py

import json
import os
from data.store import load_items, save_items
from api.scoring.scoring import recalculate_all

TOP100_PATH = "data/top100.json"


def safe_auto_recalculate():
    """
    Safely update the Top100 chart based on current items metrics.
    Should NOT crash the server.
    """

    try:
        # Load current chart
        if not os.path.exists(TOP100_PATH):
            return

        with open(TOP100_PATH, "r") as f:
            data = json.load(f)

        # Respect chart lock
        if data.get("locked"):
            return

        # Load latest metrics
        items = load_items()

        # Recalculate scores
        items = recalculate_all(items)

        # Sort by score
        items.sort(key=lambda x: float(x.get("score", 0)), reverse=True)

        # Reassign positions
        for idx, item in enumerate(items, 1):
            item["position"] = idx

        data["items"] = items

        # Write atomically (safe write pattern)
        temp_path = TOP100_PATH + ".tmp"
        with open(temp_path, "w") as tf:
            json.dump(data, tf, indent=2)
        os.replace(temp_path, TOP100_PATH)

    except Exception:
        # Fail silently -- never crash
        return