# api/admin/admin.py

from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import Dict

from data.store import load_items, save_items
from data.admin_injection_log import can_inject_today, record_injection
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.post("/inject")
def admin_inject_song(
    payload: Dict,
    background_tasks: BackgroundTasks
):
    """
    Admin manual injection.
    Enforces 10/day limit.
    Triggers safe Top 100 recalculation.
    """

    if not can_inject_today():
        raise HTTPException(
            status_code=429,
            detail="Daily admin injection limit (10/day) reached"
        )

    title = payload.get("title")
    artist = payload.get("artist")
    region = payload.get("region")

    if not title or not artist:
        raise HTTPException(
            status_code=400,
            detail="title and artist are required"
        )

    items = load_items()

    song = next(
        (i for i in items if i["title"] == title and i["artist"] == artist),
        None
    )

    if not song:
        song = {
            "title": title,
            "artist": artist,
            "youtube": 0,
            "radio": 0,
            "tv": 0,
            "score": 0,
            "region": region,
            "admin_injected": True
        }
        items.append(song)
    else:
        if region:
            song["region"] = region
        song["admin_injected"] = True

    save_items(items)

    record_injection()
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "message": "Admin injection successful",
        "title": title,
        "artist": artist
    }