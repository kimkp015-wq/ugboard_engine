from fastapi import APIRouter, HTTPException
from pathlib import Path
import json

router = APIRouter()

LOCKED_DIR = Path("data/top100_locked")


@router.get(
    "/explain/{week_id}/{rank}",
    summary="Explain why a song ranked at a position",
)
def explain_rank(week_id: str, rank: int):
    path = LOCKED_DIR / f"{week_id}.json"

    if not path.exists():
        raise HTTPException(status_code=404, detail="Week not found")

    try:
        chart = json.loads(path.read_text())
    except Exception:
        raise HTTPException(status_code=500, detail="Corrupt chart data")

    if rank < 1 or rank > len(chart):
        raise HTTPException(status_code=400, detail="Invalid rank")

    item = chart[rank - 1]

    return {
        "status": "ok",
        "week_id": week_id,
        "rank": rank,
        "explanation": {
            "song": {
                "title": item.get("title"),
                "artist": item.get("artist"),
                "region": item.get("region"),
            },
            "score_breakdown": item.get("score", {}),
            "adjustments": item.get("adjustments", {}),
            "locked": True,
        },
    }
