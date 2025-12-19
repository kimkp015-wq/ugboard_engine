from fastapi import APIRouter

router = APIRouter()

@router.get("/trending")
def get_trending():
    return {
        "chart": "Trending",
        "status": "ok",
        "data": []
    }