from fastapi import APIRouter

router = APIRouter()

@router.get("/top100")
def top_100_chart():
    songs = []

    for i in range(1, 101):
        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": "Ugandan Artist",
            "score": 100 - i
        })

    return {
        "chart": "UG Top 100",
        "total": len(songs),
        "songs": songs
    }