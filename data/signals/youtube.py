# data/signals/youtube.py

from typing import List, Dict

YOUTUBE_UNIT = 5
YOUTUBE_CAP = 20


def compute_youtube_score(videos: List[Dict]) -> Dict:
    """
    videos = list of official uploads tied to a song
    within the active chart week.
    """

    uploads = len(videos)
    raw_score = uploads * YOUTUBE_UNIT

    score = min(raw_score, YOUTUBE_CAP)

    return {
        "uploads_this_week": uploads,
        "score": score,
    }
