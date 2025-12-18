from fastapi import APIRouter

router = APIRouter()

@router.get("/charts/top")
def top_charts():
    return {
        "status": "ok",
        "type": "top",
        "data": [
            {"rank": 1, "title": "Song One", "artist": "Artist A"},
            {"rank": 2, "title": "Song Two", "artist": "Artist B"},
            {"rank": 3, "title": "Song Three", "artist": "Artist C"}
        ]
    }