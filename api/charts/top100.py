from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

# Absolute path (this is the key fix)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(BASE_DIR, "data", "top100.json")


@router.get("/charts/top100")
def get_top100():
    # 1. Check file exists
    if not os.path.exists(DATA_FILE):
        raise HTTPException(
            status_code=404,
            detail=f"Top100 file not found at {DATA_FILE}"
        )

    # 2. Read file safely
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Invalid JSON in top100.json: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error reading top100.json: {str(e)}"
        )

    # 3. Validate structure
    items = data.get("items", [])
    if not isinstance(items, list):
        raise HTTPException(
            status_code=500,
            detail="Invalid data format: items is not a list"
        )

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }