from fastapi import APIRouter

router = APIRouter()

@router.get("/ingest/free")
def ingest_free():
    return {
        "status": "ok",
        "songs": [
            {"song": "Che Che", "artist": "Jose Chameleone"},
            {"song": "Sitya Loss", "artist": "Eddy Kenzo"},
            {"song": "Nkubakyeyo", "artist": "Spice Diana"}
        ]
    }