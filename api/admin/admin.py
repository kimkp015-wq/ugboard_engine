# api/admin/admin.py

from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import Dict

from data.store import load_items, save_items
from data.admin_injection_log import (
    can_inject_today,
    record_injection
)
from api.scoring.auto_recalc import (
    safe_auto_recalculate,
    mark_ingestion
)

router = APIRouter()


@router.post("/inject")
def admin_inject_song(
    payload: Dict,
    background_tasks: BackgroundTasks
):
    """
    Admin manual injection.

    Rules:
    - Max 10 injections per day (EAT)
    - Can introduce new songs OR tag existing ones
    - Optional region assignment
    - Triggers SAFE auto-recalculation (never blocks / never crashes)
    """

    # -----------------------------
    # Enforce daily injection limit
    # -----------------------------
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

    # -----------------------------
    # Load current items
    # -----------------------------
    items = load_items()

    # -----------------------------
    # Find existing song
    # -----------------------------
    song = next(
        (
            i for i in items
            if i.get("title") == title and i.get("artist") == artist
        ),
        None
    )

    # -----------------------------
    # Create or update song
    # -----------------------------
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

    # -----------------------------
    # Persist
    # -----------------------------
    save_items(items)

    # -----------------------------
    # Audit + auto recalc
    # -----------------------------
    record_injection()
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "message": "Admin injection successful",
        "title": title,
        "artist": artist,
        "region": region
    }