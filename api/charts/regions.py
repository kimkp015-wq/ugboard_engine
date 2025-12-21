from fastapi import APIRouter

from data.store import load_items, save_items
router = APIRouter()

@router.get("/regions")
def get_regions():
    return {
        "chart": "regions",
        "items": []
    }
    items = load_items()
save_items(items)