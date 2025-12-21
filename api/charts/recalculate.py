# api/charts/recalculate.py

import json
import os
from api.scoring.scoring import calculate_score

TOP100_PATH = "data/top100.json"
ITEMS_PATH = "data/items.json"


def safe_recalculate_top100():
    """
    Recalculates Top100 safely.
    NEVER raises errors.
    """

    try:
        if not os.path.exists(ITEMS_PATH):
            return

        with open(ITEMS_PATH, "r") as f:
            items = json.load(f)

        if not isinstance(items, list):
            return

        # calculate scores
        for item in items:
            try:
                item["score"] = calculate_score(item)
            except Exception:
                item["score"] = 0

        # sort by score
        items = sorted(
            items,
            key=lambda x: x.get("score", 0),
            reverse=True
        )

        # assign positions
        for idx, item in enumerate(items, start=1):
            item["position"] = idx

        os.makedirs("data", exist_ok=True)

        with open(TOP100_PATH, "w") as f:
            json.dump(
                {
                    "locked": False,
                    "items": items
                },
                f,
                indent=2
            )

    except Exception:
        # SILENT FAIL (by design)
        return