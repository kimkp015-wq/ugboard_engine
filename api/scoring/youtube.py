# data/scoring/youtube.py

import math
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")

YOUTUBE_WEIGHT = 10.0
MAX_AGE_DAYS = 60
MIN_AGE_FACTOR = 0.4


def compute_youtube_score(
    *,
    current_views: int,
    previous_views: int | None,
    published_at: str,
    now: datetime | None = None,
) -> float:
    """
    Deterministic YouTube scoring function.

    Inputs MUST be factual.
    Never raises.
    """

    try:
        now = now or datetime.now(EAT)

        delta = max(
            0,
            current_views - (previous_views or 0),
        )

        if delta == 0:
            return 0.0

        published_dt = datetime.fromisoformat(
            published_at.replace("Z", "+00:00")
        )

        age_days = max(
            0,
            (now - published_dt).days,
        )

        age_factor = max(
            MIN_AGE_FACTOR,
            1.0 - (age_days / MAX_AGE_DAYS),
        )

        compressed = math.log10(delta + 1)

        score = compressed * age_factor * YOUTUBE_WEIGHT

        return round(score, 4)

    except Exception:
        return 0.0
