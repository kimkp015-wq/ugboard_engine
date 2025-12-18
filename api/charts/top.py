from fastapi import APIRouter

router = APIRouter(prefix="/charts", tags=["Charts"])

categories = [
    "Pop",
    "RnB & Soul",
    "Hip-Hop",
    "Dancehall",
    "Gospel"
]

TOP_CHART = []

rank = 1
for category in categories:
    for i in range(1, 21):  # 20 songs per category = 100 total
        TOP_CHART.append({
            "rank": rank,
            "title": f"Demo Song {rank}",
            "artist": f"Ugandan Artist {rank}",
            "category": category,
            "score": round(120 - rank * 0.9, 2)
        })
        rank += 1

@router.get("/top")
def top_chart():
    return {
        "status": "ok",
        "chart": "Uganda Top 100 (Demo)",
        "total_songs": len(TOP_CHART),
        "data": TOP_CHART
    }