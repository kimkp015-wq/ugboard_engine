# api/admin/admin.py

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List, Dict

from data.store import load_items, save_items
from data.admin_injection_log import (
    can_inject_today,
    record_injection,
    read_injections_today,
)

from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.get("/admin/injection/status")
def admin_injection_status():
    """
    Returns how many admin injections have been used today.
    """
    return {
        "daily_limit": 10,
        "used_today": read_injections_today(),
        "remaining": max(0, 10 - read_injections_today()),
    }


@router.post("/admin/inject")
def admin_inject_songs(
    payload: Dict,
    background_tasks: BackgroundTasks,
):
    """
    Admin manual song injection.
    MAX: 10 songs per day (strict).
    """

    songs: List[Dict] = payload.get("items", [])

    if not isinstance(songs, list) or not songs:
        raise HTTPException(
            status_code=400,
            detail="items must be a non-empty list",
        )

    if not can_inject_today(len(songs)):
        raise HTTPException(
            status_code=403,
            detail="Daily admin injection limit (10) exceeded",
        )

    items = load_items()
    injected = 0

    for entry in songs:
        title = entry.get("title")
        artist = entry.get("artist")
        region = entry.get("region")  # optional

        if not title or not artist:
            continue

        exists = any(
            i["title"] == title and i["artist"] == artist
            for i in items
        )

        if exists:
            continue

        song = {
            "title": title,
            "artist": artist,
            "region": region,  # Eastern / Northern / Western / Central
            "youtube": 0,
            "radio": 0,
            "tv": 0,
            "score": 0,
            "admin_injected": True,
        }

        items.append(song)
        injected += 1

    if injected == 0:
        raise HTTPException(
            status_code=400,
            detail="No valid new songs were injected",
        )

    save_items(items)

    # --- LOG + SAFE AUTO-RECALC ---
    record_injection(injected)
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "injected": injected,
        "used_today": read_injections_today(),
    }