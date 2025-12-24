from typing import Dict

def compute_score(item: Dict) -> int:
    """
    Canonical scoring function.
    Single source of truth.

    Weights are explicit and auditable.
    """

    youtube_views = item.get("youtube_views", 0)
    radio_plays = item.get("radio_plays", 0)
    tv_appearances = item.get("tv_appearances", 0)

    # Defensive typing
    youtube_views = int(youtube_views or 0)
    radio_plays = int(radio_plays or 0)
    tv_appearances = int(tv_appearances or 0)

    score = (
        youtube_views * 1 +
        radio_plays * 500 +
        tv_appearances * 1000
    )

    return score