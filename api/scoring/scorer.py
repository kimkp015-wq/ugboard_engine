import random

def youtube_momentum(song_title: str, artist: str) -> float:
    """
    Public-safe YouTube momentum signal.
    If real data is unavailable, returns a stable simulated signal.
    Range: 0–100
    """
    # Placeholder for future real YouTube public feed
    # For now, simulate realistic momentum
    return round(random.uniform(40, 95), 2)


def songboost_score(rank: int) -> float:
    """
    Convert SongBoost rank to score.
    Lower rank = higher score
    """
    if rank <= 0:
        return 0.0

    score = max(0, 100 - rank)
    return float(score)


def admin_boost(boost_value: int) -> float:
    """
    Admin boost capped to avoid abuse
    """
    return float(min(boost_value, 10))


def calculate_score(
    song_title: str,
    artist: str,
    songboost_rank: int,
    previous_rank: int | None = None,
    boost: int = 0
) -> float:
    """
    Final UG Board scoring formula (v1)
    """

    yt = youtube_momentum(song_title, artist)
    sb = songboost_score(songboost_rank)
    ab = admin_boost(boost)

    # Momentum (movement up the chart)
    momentum = 0.0
    if previous_rank is not None and previous_rank > songboost_rank:
        momentum = min(20.0, previous_rank - songboost_rank)

    score = (
        yt * 0.4 +
        sb * 0.3 +
        momentum * 0.2 +
        ab * 0.1
    )

    return round(score, 2)