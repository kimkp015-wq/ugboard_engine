# api/admin/admin.py

from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import Dict

from data.store import load_items, save_items
from data.admin_injection_log import can_inject_today, record_injection
from data.region_store import is_region_locked
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.post("/inject")
def admin_inject_song(payload: Dict, background_tasks: BackgroundTasks):

    if not can_inject_today():
        raise HTTPException(429, "Daily admin injection limit (10/day) reached")

    title = payload.get("title")
    artist = payload.get("artist")
    region = payload.get("region")

    if not title or not artist:
        raise HTTPException(400, "title and artist required")

    if region and is_region_locked(region):
        raise HTTPException(
            status_code=423,
            detail=f"{region} region is locked (published)"
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

    return {"status": "ok", "message": "Admin injection successful"}