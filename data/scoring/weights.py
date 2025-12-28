import math


def youtube_view_score(delta_views: int) -> float:
    if delta_views <= 0:
        return 0.0

    return round(math.log10(delta_views + 1) * 100, 2)
