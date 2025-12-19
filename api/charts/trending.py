from fastapi import APIRouter

router = APIRouter()

@router.get("/trending")
def get_trending():
    return {
        "chart": "trending",
        "items": []
    }