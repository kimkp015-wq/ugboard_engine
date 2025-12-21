from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

TOP100_PATH = "data/top100.json"


@router.get("/admin/top100")
def admin_get_top100():
    if not os.path.exists(TOP100_PATH):
        raise HTTPException(
            status_code=404,
            detail="Top100 not published yet"
        )

    with open(TOP100_PATH, "r") as f:
        return json.load(f)