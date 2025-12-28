from typing import Dict


def apply_fraud_guards(
    *,
    delta_views: int,
    avg_past_delta: int,
    max_daily_cap: int = 500_000,
) -> Dict:
    """
    Apply non-destructive fraud guards.

    Returns:
    - adjusted_delta
    - flags (list)
    """

    flags = []
    adjusted = delta_views

    # -------------------------
    # Rule B: velocity cap
    # -------------------------
    if adjusted > max_daily_cap:
        adjusted = max_daily_cap
        flags.append("velocity_capped")

    # -------------------------
    # Rule A: spike ratio
    # -------------------------
    if avg_past_delta > 0:
        ratio = delta_views / avg_past_delta

        if ratio > 20:
            adjusted = int(adjusted * 0.2)
            flags.append("hard_spike_dampened")
        elif ratio > 5:
            adjusted = int(adjusted * 0.5)
            flags.append("soft_spike_dampened")

    return {
        "adjusted_delta": max(adjusted, 0),
        "flags": flags,
    }
