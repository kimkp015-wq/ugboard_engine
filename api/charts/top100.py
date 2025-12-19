from fastapi import APIRouter

router = APIRouter()


def calculate_score(rank: int) -> int:
    return 1000 - (rank * 5)


@router.get("/top100")
def get_top_100():
    songs = []

    for i in range(1, 101):
        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": "Ugandan Artist",
            "score": calculate_score(i)
        })

    return {
        "status": "ok",
        "chart": "UG Top 100",
        "total": len(songs),
        "data": songs
    }