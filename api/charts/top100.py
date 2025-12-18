from fastapi import APIRouter

router = APIRouter()

UG_TOP_SONGS = [
    {"artist": "Artist A", "title": "Song A", "streams_score": 50, "radio_score": 20, "admin_boost": 0},
    {"artist": "Artist B", "title": "Song B", "streams_score": 40, "radio_score": 30, "admin_boost": 10},
]

def calculate_score(song):
    return (
        song.get("streams_score", 0)
        + song.get("radio_score", 0)
        + song.get("admin_boost", 0)
    )

@router.get("/top100")
def build_top_100():
    scored = []

    for song in UG_TOP_SONGS:
        s = song.copy()
        s["score"] = calculate_score(song)
        scored.append(s)

    scored.sort(key=lambda x: x["score"], reverse=True)

    for i, song in enumerate(scored, start=1):
        song["rank"] = i

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "total": len(scored),
        "data": scored
    }