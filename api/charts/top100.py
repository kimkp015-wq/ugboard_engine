from fastapi import APIRouter

router = APIRouter(prefix="/charts", tags=["Charts"])


@router.get("/top100")
def top_100():
    songs = []

    for i in range(1, 101):
        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": f"Artist {i}"
        })

    return {
        "status": "ok",
        "count": len(songs),
        "data": songs
    }