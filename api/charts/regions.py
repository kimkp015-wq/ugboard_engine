from fastapi import APIRouter, HTTPException
from typing import List, Dict

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


# -----------------------------
# Internal SAFE helpers
# -----------------------------

def _load_top100() -> List[dict]:
    from data.store import load_top100
    return load_top100()


def _is_region_locked(region: str) -> bool:
    from data.region_store import is_region_locked
    return is_region_locked(region)


def _load_admin_injections() -> List[dict]:
    from data.admin_injection_log import load_admin_injections
    return load_admin_injections()


# -----------------------------
# Core region logic
# -----------------------------

def _build_region_chart(region: str) -> List[dict]:
    """
    Build Top 5 for a region using:
    - Top 100 songs
    - Admin-injected regional songs
    """

    top100 = _load_top100()
    injections = _load_admin_injections()

    region_songs: List[dict] = []

    # 1️⃣ Songs already in Top 100 with region tag
    for song in top100:
        if song.get("region") == region:
            region_songs.append(song)

    # 2️⃣ Admin-injected songs (even if not in Top 100)
    for inject in injections:
        if inject.get("region") == region:
            region_songs.append({
                "song_id": inject["song_id"],
                "title": inject["title"],
                "artist": inject["artist"],
                "region": region,
                "source": "admin_injection",
                "injected_at": inject["timestamp"]
            })

    # 3️⃣ Deduplicate by song_id (Top 100 wins)
    seen = set()
    unique: List[dict] = []
    for song in region_songs:
        sid = song.get("song_id")
        if sid and sid not in seen:
            seen.add(sid)
            unique.append(song)

    # 4️⃣ Limit to Top 5
    return unique[:5]


# -----------------------------
# API Endpoints
# -----------------------------

@router.get("/regions", summary="Get all regional charts")
def get_all_regions():
    """
    Returns Top 5 for Eastern, Northern, Western
    """
    charts: Dict[str, List[dict]] = {}

    for region in VALID_REGIONS:
        charts[region] = _build_region_chart(region)

    return {
        "locked": {
            region: _is_region_locked(region)
            for region in VALID_REGIONS
        },
        "charts": charts
    }


@router.get("/regions/{region}", summary="Get a single region chart")
def get_region(region: str):
    """
    Returns Top 5 for a specific region
    """
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    return {
        "region": region,
        "locked": _is_region_locked(region),
        "chart": _build_region_chart(region)
    }