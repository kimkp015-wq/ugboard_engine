from typing import List, Dict


YT_WEIGHT = 0.60
RADIO_WEIGHT = 0.25
TV_WEIGHT = 0.15


def _safe_max(items: List[Dict], key: str) -> int:
    values = [i.get(key, 0) for i in items if isinstance(i.get(key, 0), int)]
    return max(values) if values else 0


def calculate_scores(items: List[Dict]) -> List[Dict]:
    """
    Calculate and attach score for each chart item.
    Does NOT mutate input list.
    """

    yt_max = _safe_max(items, "youtube_views")
    radio_max = _safe_max(items, "radio_plays")
    tv_max = _safe_max(items, "tv_appearances")

    scored_items = []

    for item in items:
        yt = item.get("youtube_views", 0)
        radio = item.get("radio_plays", 0)
        tv = item.get("tv_appearances", 0)

        yt_norm = yt / yt_max if yt_max > 0 else 0
        radio_norm = radio / radio_max if radio_max > 0 else 0
        tv_norm = tv / tv_max if tv_max > 0 else 0

        score = (
            yt_norm * YT_WEIGHT +
            radio_norm * RADIO_WEIGHT +
            tv_norm * TV_WEIGHT
        )

        scored_items.append({
            **item,
            "score": round(score, 6),
        })

    return scored_items