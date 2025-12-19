from fastapi import APIRouter

router = APIRouter()

@router.get("/trending")
def get_trending():
    return {
        "status": "ok",
        "chart": "Trending Uganda",
        "songs": []
    }