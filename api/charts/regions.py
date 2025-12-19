from fastapi import APIRouter

router = APIRouter()

@router.get("/regions")
def get_regions():
    return {
        "status": "ok",
        "chart": "Regional Charts",
        "regions": []
    }