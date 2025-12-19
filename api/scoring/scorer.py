from api.scoring.config import WEIGHTS


def calculate_score(youtube: int = 0, radio: int = 0, tv: int = 0) -> float:
    """
    Calculates final weighted score for a song.
    All inputs must be >= 0
    """

    youtube = max(0, youtube)
    radio = max(0, radio)
    tv = max(0, tv)

    score = (
        youtube * WEIGHTS["youtube"]
        + radio * WEIGHTS["radio"]
        + tv * WEIGHTS["tv"]
    )

    return round(score, 2)