def calculate_score(item: dict) -> int:
    """
    Calculate weighted score for a Top100 item.
    Safe, pure function (no side effects).
    """

    youtube = int(item.get("youtube", 0))
    radio = int(item.get("radio", 0))
    tv = int(item.get("tv", 0))

    score = (
        youtube * 0.5 +
        radio * 0.3 +
        tv * 0.2
    )

    return int(score)