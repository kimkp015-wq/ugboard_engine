# data/scoring.py

from typing import Dict

# -------------------------
# Weight configuration
# -------------------------
WEIGHTS = {
    "youtube_views": 0.6,
    "radio_plays": 0.3,
    "tv_appearances": 0.1,
}


def compute_score(item: Dict) -> int:
    """
    Compute final chart score for a song.

    Rules:
    - Missing signals default to 0
    - Deterministic
    - No side effects
    """

    youtube = int(item.get("youtube_views", 0))
    radio = int(item.get("radio_plays", 0))
    tv = int(item.get("tv_appearances", 0))

    score = (
        youtube * WEIGHTS["youtube_views"]
        + radio * WEIGHTS["radio_plays"]
        + tv * WEIGHTS["tv_appearances"]
    )

    return int(score)