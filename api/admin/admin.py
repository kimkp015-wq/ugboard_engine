from fastapi import APIRouter, HTTPException
from data.store import load_items, save_items
from data.admin_injection_log import (
    can_inject_today,
    record_injection,
    injections_today,
)
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.post("/admin/inject")
def admin_inject(payload: dict):
    if not can_inject_today():
        raise HTTPException(
            status_code=403,
            detail="Daily admin injection limit (10) reached",
        )

    title = payload.get("title")
    artist = payload.get("artist")
    region = payload.get("region")

    if not title or not artist or not region:
        raise HTTPException(status_code=400, detail="Missing fields")

    items = load_items()

    items.append({
        "title": title,
        "artist": artist,
        "region": region,
        "youtube": 0,
        "radio": 0,
        "tv": 0,
        "score": 0,
        "admin": True
    })

    save_items(items)

    record_injection()
    mark_ingestion()
    safe_auto_recalculate()

    return {
        "status": "ok",
        "injected_today": injections_today()
    }