from fastapi import APIRouter, HTTPException, BackgroundTasks
from datetime import datetime
from typing import Dict

from data.store import load_items, save_items
from data.admin_injection_log import (
    can_inject_today,
    log_admin_injection
)
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()

# Single source of truth
VALID_REGIONS = {"eastern", "northern", "western", "central"}


@router.post("/admin/inject")
def admin_inject_song(
    payload: Dict,
    background_tasks: BackgroundTasks
):
    title = payload.get("title")
    artist = payload.get("artist")
    region = payload.get("region")

    if not title or not artist:
        raise HTTPException(
            status_code=400,
            detail="title and artist are required"
        )

    # Enforce daily admin injection limit
    if not can_inject_today():
        raise HTTPException(
            status_code=403,
            detail="Daily admin injection limit reached (10/day)"
        )

    # Normalize & validate region (optional)
    if region is not None:
        if not isinstance(region, str):
            raise HTTPException(
                status_code=400,
                detail="region must be a string"
            )

        region = region.strip().lower()

        if region not in VALID_REGIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid region. Must be one of {sorted(VALID_REGIONS)}"
            )

    items = load_items()

    # Prevent duplicates
    existing = next(
        (i for i in items if i["title"] == title and i["artist"] == artist),
        None
    )

    if existing:
        raise HTTPException(
            status_code=409,
            detail="Song already exists"
        )

    song = {
        "title": title,
        "artist": artist,
        "youtube": 0,
        "radio": 0,
        "tv": 0,
        "score": 0,
        "region": region,
        "injected_by_admin": True,
        "created_at": datetime.utcnow().isoformat()
    }

    items.append(song)
    save_items(items)

    # Log admin injection
    log_admin_injection(
        title=title,
        artist=artist,
        region=region
    )

    # Trigger safe recalculation
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "message": "Song injected successfully",
        "region": region
    }