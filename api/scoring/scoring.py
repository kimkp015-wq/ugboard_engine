def calculate_score(item: dict) -> int:
    youtube = int(item.get("youtube", 0))
    radio = int(item.get("radio", 0))
    tv = int(item.get("tv", 0))

    score = (youtube * 1) + (radio * 3) + (tv * 2)
    return score


def recalculate_all(items: list) -> list:
    for item in items:
        item["score"] = calculate_score(item)
    return items