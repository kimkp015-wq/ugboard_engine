def calculate_score(youtube=0, radio=0, tv=0):
    """
    Unified scoring formula.
    All inputs must be >= 0.
    """

    youtube = max(0, int(youtube))
    radio = max(0, int(radio))
    tv = max(0, int(tv))

    score = (
        youtube * 1.0 +
        radio * 1.5 +
        tv * 2.0
    )

    return round(score, 2)