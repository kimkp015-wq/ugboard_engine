from fastapi import APIRouter

router = APIRouter(prefix="/charts", tags=["Charts"])

TOP_CHART = []

categories = [
    "Pop",
    "RnB & Soul",
    "Hip-Hop",
    "Dancehall",
    "Gospel"
]

# Generate demo Top 20
rank = 1
for category in categories:
    for i in range(1, 5):
        TOP_CHART.append({
            "rank": rank,
            "title": f"Demo Song {rank}",
            "artist": f"Ugandan Artist {rank}",
            "category": category,
            "score": round(100 - rank * 1.3, 2)
        })
        rank += 1

@router.get("/top")
def top_chart():
    return {
        "status": "ok",
        "chart": "Uganda Top 20 (Demo)",
        "total_songs": len(TOP_CHART),
        "data": TOP_CHART
    }