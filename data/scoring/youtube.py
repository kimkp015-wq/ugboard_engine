from typing import List, Dict


def compute_view_delta(
    snapshots: List[Dict],
) -> int:
    """
    Compute incremental view delta from ordered snapshots.

    snapshots:
    - Must be sorted ascending by captured_at
    - Must belong to same external_id

    Returns:
    - Non-negative delta
    """

    if len(snapshots) < 2:
        return 0

    prev = snapshots[-2].get("view_count")
    curr = snapshots[-1].get("view_count")

    if not isinstance(prev, int):
        return 0

    if not isinstance(curr, int):
        return 0

    delta = curr - prev

    if delta < 0:
        return 0

    return delta
