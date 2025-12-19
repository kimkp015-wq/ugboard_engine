from fastapi import APIRouter
from api.scoring.scorer import calculate_score

router = APIRouter()

@router.get("/top100")
def top_100_chart():
    songs = []

    for i in range(1, 101):
        score = calculate_score(
            song_title=f"Song {i}",
            artist="Ugandan Artist",
            songboost_rank=i,
            previous_rank=i + 3,
            boost=0
        )

        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": "Ugandan Artist",
            "score": score
        })

    return {
        "chart": "UG Top 100",
        "total": len(songs),
        "songs": songs
    }