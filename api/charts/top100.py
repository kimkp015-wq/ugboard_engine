from fastapi import APIRouter

router = APIRouter()

@router.get("/top100")
def get_top100():
    return {
        "chart": "Top 100",
        "status": "ok",
        "data": []
    }