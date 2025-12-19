from fastapi import APIRouter

router = APIRouter()

@router.get("/top100")
def get_top_100():
    songs = []

    # Temporary deterministic Top 100 (safe mode)
    for i in range(1, 101):
        songs.append({
            "rank": i,
            "title": f"UG Song {i}",
            "artist": "Ugandan Artist",
            "score": round(100 - (i * 0.5), 2)
        })

    return {
        "chart": "UG Board Top 100",
        "count": len(songs),
        "data": songs
    }