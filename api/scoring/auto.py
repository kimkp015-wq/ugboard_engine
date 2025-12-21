# api/scoring/auto.py

import json
import os
from api.scoring.scoring import calculate_score

TOP100_PATH = "data/top100.json"


def safe_auto_recalculate(items: list) -> None:
    """
    Safely recalculates scores and Top100.
    NEVER raises.
    """

    try:
        # -------------------------
        # Recalculate item scores
        # -------------------------
        for item in items:
            try:
                item["score"] = calculate_score(item)
            except Exception:
                item["score"] = 0

        # -------------------------
        # If no Top100 → stop safely
        # -------------------------
        if not os.path.exists(TOP100_PATH):
            return

        with open(TOP100_PATH, "r") as f:
            data = json.load(f)

        # -------------------------
        # Respect lock
        # -------------------------
        if data.get("locked") is True:
            return

        top_items = data.get("items", [])
        if not isinstance(top_items, list):
            return

        # -------------------------
        # Match & update scores
        # -------------------------
        for t in top_items:
            for i in items:
                if (
                    i.get("title") == t.get("title")
                    and i.get("artist") == t.get("artist")
                ):
                    t["score"] = i.get("score", 0)
                    t["youtube"] = i.get("youtube", 0)
                    t["radio"] = i.get("radio", 0)
                    t["tv"] = i.get("tv", 0)
                    break

        # -------------------------
        # Sort & re-rank
        # -------------------------
        top_items.sort(
            key=lambda x: float(x.get("score", 0)),
            reverse=True
        )

        for idx, item in enumerate(top_items, start=1):
            item["position"] = idx

        data["items"] = top_items

        with open(TOP100_PATH, "w") as f:
            json.dump(data, f, indent=2)

    except Exception:
        # ABSOLUTE SILENCE
        return