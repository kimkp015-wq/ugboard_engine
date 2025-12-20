def apply_boosts(items: list) -> list:
    """
    Final scoring logic (safe, crash-proof)

    Score formula:
    score = youtube*1 + radio*3 + tv*5
    """

    for item in items:
        youtube = item.get("youtube", 0)
        radio = item.get("radio", 0)
        tv = item.get("tv", 0)

        # Final score calculation
        score = (youtube * 1) + (radio * 3) + (tv * 5)

        item["score"] = score

    # Sort by score (highest first)
    items.sort(key=lambda x: x.get("score", 0), reverse=True)

    # Re-assign positions after scoring
    for index, item in enumerate(items, start=1):
        item["position"] = index

    return items