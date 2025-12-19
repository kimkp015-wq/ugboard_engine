from fastapi import APIRouter, HTTPException
from typing import List

router = APIRouter()

@router.get("/top100", response_model=List[dict])
def get_top_100():
    try:
        # TEMP SAFE RESPONSE (until DB confirmed)
        return [
            {"rank": 1, "title": "Sample Song", "artist": "Sample Artist"},
            {"rank": 2, "title": "Another Song", "artist": "Another Artist"}
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))