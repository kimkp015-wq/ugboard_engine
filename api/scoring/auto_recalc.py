# api/scoring/auto_recalc.py

import json
import os
import time
from data.store import load_items
from api.scoring.scoring import recalculate_all

TOP100_PATH = "data/top100.json"
STATE_PATH = "data/recalc_state.json"
DEBOUNCE_SECONDS = 10


def _read_state():
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_state(state: dict):
    os.makedirs("data", exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


def mark_ingestion():
    """
    Call this on EVERY ingestion.
    """
    state = _read_state()
    state["last_ingestion"] = time.time()
    _write_state(state)


def safe_auto_recalculate():
    """
    Debounced, lock-aware, crash-proof auto recalculation.
    """

    try:
        # --- Debounce check ---
        state = _read_state()
        last = state.get("last_ingestion")

        if not last:
            return

        if time.time() - last < DEBOUNCE_SECONDS:
            # Too soon -- skip recalculation
            return

        # --- Load chart ---
        if not os.path.exists(TOP100_PATH):
            return

        with open(TOP100_PATH, "r") as f:
            chart = json.load(f)

        # Respect lock
        if chart.get("locked"):
            return

        # --- Load & recalc ---
        items = load_items()
        items = recalculate_all(items)
        items.sort(key=lambda x: float(x.get("score", 0)), reverse=True)

        for idx, item in enumerate(items, 1):
            item["position"] = idx

        chart["items"] = items

        # --- Atomic write ---
        temp = TOP100_PATH + ".tmp"
        with open(temp, "w") as f:
            json.dump(chart, f, indent=2)
        os.replace(temp, TOP100_PATH)

    except Exception:
        # ABSOLUTE SAFETY: never crash
        return