# api/scoring/scoring.py

def calculate_score(item: dict) -> int:
    """
    Single item score calculation.
    Must NEVER throw.
    """
    try:
        youtube = int(item.get("youtube", 0))
        radio = int(item.get("radio", 0))
        tv = int(item.get("tv", 0))

        # Simple, predictable weights
        score = (youtube * 1) + (radio * 3) + (tv * 2)
        return score
    except Exception:
        return 0


def recalculate_all(items: list) -> list:
    """
    Recalculate scores for all items.
    Safe, silent, no crashes.
    """
    if not isinstance(items, list):
        return []

    for item in items:
        try:
            item["score"] = calculate_score(item)
        except Exception:
            item["score"] = 0

    return items