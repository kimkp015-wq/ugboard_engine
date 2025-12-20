import json
import os
from fastapi import APIRouter, HTTPException

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.get("/charts/top100")
def get_top100():
    return {
        "exists": os.path.exists(DATA_FILE),
        "cwd": os.getcwd(),
        "files_in_data": os.listdir("data") if os.path.exists("data") else "NO_DATA_DIR"
    }