from fastapi import APIRouter
from data.top100_snapshot import load_top100_snapshot
from data.store import load_items

router = APIRouter()


@router.get("/top100", summary="Get Top 100 songs")
def get_top100():
    snapshot = load_top100_snapshot()

    if snapshot:
        return {
            "status": "ok",
            "locked": True,
            "week_id": snapshot["week_id"],
            "count": snapshot["count"],
            "items": snapshot["items"],
        }

    # Fallback (live)
    items = load_items()
    items.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "status": "ok",
        "locked": False,
        "count": len(items[:100]),
        "items": items[:100],
    }