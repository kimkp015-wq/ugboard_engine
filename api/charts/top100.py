import json
import os
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/charts", tags=["Charts"])

DATA_PATH = "data/top100.json"


@router.get("/top100")
def get_top100():
    if not os.path.exists(DATA_PATH):
        raise HTTPException(
            status_code=404,
            detail="Top 100 not published yet"
        )

    with open(DATA_PATH, "r") as f:
        return json.load(f)