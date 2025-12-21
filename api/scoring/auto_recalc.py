# api/scoring/auto_recalc.py

import time
import json
import os
from api.scoring.scoring import recalculate_all
from data.store import load_items, save_items

TOP100_PATH = "data/top100.json"
DEBOUNCE_SECONDS = 10

_last_recalc = 0


def _is_top100_locked() -> bool:
    if not os.path.exists(TOP100_PATH):
        return False

    try:
        with open(TOP100_PATH, "r") as f:
            data = json.load(f)
        return bool(data.get("locked", False))
    except Exception:
        return False


def try_auto_recalculate():
    global _last_recalc

    now = time.time()

    # debounce protection
    if now - _last_recalc < DEBOUNCE_SECONDS:
        return

    # lock protection
    if _is_top100_locked():
        return

    try:
        items = load_items()
        items = recalculate_all(items)
        save_items(items)
        _last_recalc = now
    except Exception:
        # MUST NEVER crash ingestion
        pass