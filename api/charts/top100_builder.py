import json
import os
from datetime import datetime

from api.charts.scoring import calculate_scores

TOP100_FILE = "data/top100.json"


def build_top100():
    scored = calculate_scores()

    if not scored:
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    top_items = scored[:100]

    ranked = []
    position = 1

    for item in top_items:
        ranked.append({
            "position": position,
            "title": item["title"],
            "artist": item["artist"],
            "score": item["score"],
            "youtube": item["youtube"],
            "radio": item["radio"],
            "tv": item["tv"]
        })
        position += 1

    os.makedirs("data", exist_ok=True)

    data = {
        "updated_at": datetime.utcnow().isoformat(),
        "count": len(ranked),
        "items": ranked
    }

    with open(TOP100_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return data