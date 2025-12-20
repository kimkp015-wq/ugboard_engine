def calculate_score(item: dict) -> float:
    """
    Calculate score using weighted sources.
    Safe defaults prevent crashes.
    """

    youtube = float(item.get("youtube", 0) or 0)
    radio = float(item.get("radio", 0) or 0)
    tv = float(item.get("tv", 0) or 0)

    score = (
        youtube * 0.6 +
        radio * 0.3 +
        tv * 0.1
    )

    return round(score, 2)