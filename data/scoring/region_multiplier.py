from typing import Dict


REGION_MULTIPLIERS: Dict[str, float] = {
    "Central": 1.00,
    "Eastern": 1.25,
    "Western": 1.20,
    "Northern": 1.30,
}


def apply_region_multiplier(
    *,
    base_score: float,
    region: str,
) -> Dict:
    """
    Apply regional fairness multiplier.

    Returns:
    - adjusted_score
    - multiplier_used
    """

    multiplier = REGION_MULTIPLIERS.get(region, 1.0)

    adjusted = base_score * multiplier

    return {
        "adjusted_score": round(adjusted, 4),
        "multiplier": multiplier,
    }
