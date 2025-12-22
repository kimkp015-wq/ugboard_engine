# api/admin/admin.py

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict

from data.store import load_items, save_items
from data.admin_injection_log import (
    can_inject_today,
    remaining_injections_today,
    log_admin_injection
)
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()

# -----------------------------
# Allowed regions (STRICT)
# -----------------------------
ALLOWED_REGIONS = {
    "eastern",
    "northern",
    "western"
}


# -----------------------------
# Admin inject song
# -----------------------------
@router.post("/inject")
def admin_inject_song(
    payload: Dict,
    background_tasks: BackgroundTasks
):
    """
    Admin-only song injection.
    HARD LIMIT: 10/day (EAT)
    """

    title = payload.get("title")
    artist = payload.get("artist")
    region = payload.get("region")

    # -----------------------------
    # Basic validation
    # -----------------------------
    if not title or not artist or not region:
        raise HTTPException(
            status_code=400,
            detail="title, artist, and region are required"
        )

    region = region.lower().strip()

    if region not in ALLOWED_REGIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid region. Allowed: {sorted(ALLOWED_REGIONS)}"
        )

    # -----------------------------
    # Enforce DAILY LIMIT
    # -----------------------------
    if not can_inject_today():
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Daily admin injection limit reached",
                "limit": 10,
                "remaining": 0
            }
        )

    # -----------------------------
    # Load existing items
    # -----------------------------
    items = load_items()

    # -----------------------------
    # Find or create song
    # -----------------------------
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
            "score": 0
        }
        items.append(song)

    # -----------------------------
    # Assign region (ADMIN OVERRIDE)
    # -----------------------------
    song["region"] = region

    # -----------------------------
    # Persist items
    # -----------------------------
    save_items(items)

    # -----------------------------
    # Audit log (DATA LAYER)
    # -----------------------------
    log_admin_injection(
        title=title,
        artist=artist,
        region=region
    )

    # -----------------------------
    # Trigger SAFE AUTO-RECALC
    # -----------------------------
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    # -----------------------------
    # Success response
    # -----------------------------
    return {
        "status": "ok",
        "message": "Song injected successfully",
        "region": region,
        "remaining_today": remaining_injections_today()
    }