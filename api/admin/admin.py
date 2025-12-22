from fastapi import APIRouter, HTTPException
from data.admin_injection_log import can_inject_today, log_injection
from pathlib import Path
import json

router = APIRouter()

ITEMS_FILE = Path("data/items.json")


@router.post("/inject")
def admin_inject(song_id: str, region: str, admin: str = "system"):
    if not can_inject_today():
        raise HTTPException(
            status_code=403,
            detail="Daily admin injection limit (10/day) reached"
        )

    if not ITEMS_FILE.exists():
        raise HTTPException(status_code=500, detail="Items store missing")

    items = json.loads(ITEMS_FILE.read_text())

    if song_id not in items:
        raise HTTPException(status_code=404, detail="Song not found")

    # Assign region (metadata only)
    items[song_id]["region"] = region.lower()
    items[song_id]["admin_injected"] = True

    ITEMS_FILE.write_text(json.dumps(items, indent=2))

    log_injection(song_id, region, admin)

    return {
        "status": "ok",
        "song_id": song_id,
        "region": region.lower()
    }