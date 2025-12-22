# api/admin/admin.py

from fastapi import APIRouter, HTTPException, BackgroundTasks
from data.store import load_items, save_items
from data.admin_injection_log import (
    can_inject_today,
    log_injection,
    injections_today
)
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.post("/admin/inject")
def admin_inject_song(payload: dict, background_tasks: BackgroundTasks):
    """
    Admin injection:
    - Max 10 per day
    - Can inject songs not yet in Top100
    - Supports region tagging
    """

    if not can_inject_today():
        raise HTTPException(
            status_code=403,
            detail="Daily admin injection limit reached (10)"
        )

    title = payload.get("title")
    artist = payload.get("artist")
    region = payload.get("region")

    if not title or not artist or not region:
        raise HTTPException(
            status_code=400,
            detail="title, artist, and region are required"
        )

    items = load_items()

    existing = next(
        (i for i in items if i["title"] == title and i["artist"] == artist),
        None
    )

    if not existing:
        existing = {
            "title": title,
            "artist": artist,
            "region": region,
            "youtube": 0,
            "radio": 0,
            "tv": 0,
            "score": 0,
            "admin_injected": True
        }
        items.append(existing)
    else:
        existing["region"] = region
        existing["admin_injected"] = True

    save_items(items)
    log_injection(existing)

    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "message": "Song injected",
        "remaining_today": 10 - injections_today()
    }