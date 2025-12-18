from fastapi import APIRouter

router = APIRouter(prefix="/charts", tags=["Charts"])

# Demo Top 100 Uganda songs (static test data)
TOP_100 = [
    {
        "rank": 1,
        "title": "Demo Hit One",
        "artist": "Ugandan Artist A",
        "category": "Pop",
        "score": 98.5
    },
    {
        "rank": 2,
        "title": "Demo Hit Two",
        "artist": "Ugandan Artist B",
        "category": "RnB & Soul",
        "score": 96.2
    },
    {
        "rank": 3,
        "title": "Demo Hit Three",
        "artist": "Ugandan Artist C",
        "category": "Hip-Hop",
        "score": 94.1
    }
]

@router.get("/top")
def top_chart():
    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "total_songs": len(TOP_100),
        "data": TOP_100
    }