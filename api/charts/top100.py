from fastapi import APIRouter
import json
import os

router = APIRouter(prefix="/charts", tags=["charts"])

DATA_FILE = "data/top100.json"


@router.get("/top100")
def get_top100():
    # If file does not exist, return safe empty
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    # Try reading the file safely
    try:
        with open(DATA_FILE, "r") as f:
            raw = f.read().strip()
            if not raw:
                # Empty file
                return {
                    "status": "ok",
                    "count": 0,
                    "items": []
                }

            data = json.loads(raw)

    except json.JSONDecodeError:
        # Malformed JSON
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }
    except Exception as e:
        # Unexpected error (still not crashing)
        return {
            "status": "error",
            "message": f"Could not read Top100: {str(e)}"
        }

    # Grab list safely
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        items = []

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }