from fastapi import APIRouter
from data.store import load_items

router = APIRouter()

@router.get("/trending")
def get_trending():
    items = load_items()

    # Sort by score, highest first
    items = sorted(items, key=lambda x: x.get("score", 0), reverse=True)

    return {
        "status": "ok",
        "count": min(10, len(items)),
        "items": items[:10]
    }