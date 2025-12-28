from typing import List, Dict


def detect_burst(snapshots: List[Dict]) -> bool:
    """
    Detect if most growth happened in <24h.
    """
    if len(snapshots) < 2:
        return False

    snapshots = sorted(
        snapshots,
        key=lambda s: s["captured_at"]
    )

    total = snapshots[-1]["view_count"] - snapshots[0]["view_count"]
    if total <= 0:
        return False

    for i in range(len(snapshots) - 1):
        delta = (
            snapshots[i + 1]["view_count"]
            - snapshots[i]["view_count"]
        )
        if delta / total >= 0.7:
            return True

    return False
