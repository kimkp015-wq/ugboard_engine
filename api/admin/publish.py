from fastapi import APIRouter

router = APIRouter()

@router.post("/publish/top100")
def publish_top100():
    return {"published": "top100"}