# api/scoring/auto_recalc.py

import time
import threading
from pathlib import Path
from data.store import load_items, save_items
from api.scoring.scoring import recalculate_all

# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

LAST_RUN_FILE = DATA_DIR / "last_recalc.txt"
INGEST_FLAG_FILE = DATA_DIR / "pending_ingestion.flag"

DEBOUNCE_SECONDS = 5
_LOCK = threading.Lock()


# -----------------------------
# MARK INGESTION
# -----------------------------
def mark_ingestion():
    """
    Signals that new ingestion has happened.
    This is crash-safe and non-blocking.
    """
    try:
        INGEST_FLAG_FILE.write_text(str(time.time()))
    except Exception:
        pass


# -----------------------------
# SAFE AUTO RECALCULATE
# -----------------------------
def safe_auto_recalculate():
    """
    Runs recalculation ONLY when needed.
    Debounced, locked, crash-safe.
    """
    with _LOCK:
        # If no ingestion happened, skip
        if not INGEST_FLAG_FILE.exists():
            return

        now = time.time()

        try:
            last = float(LAST_RUN_FILE.read_text())
        except Exception:
            last = 0.0

        # Debounce
        if now - last < DEBOUNCE_SECONDS:
            return

        # Mark recalc time first (crash-safe)
        try:
            LAST_RUN_FILE.write_text(str(now))
        except Exception:
            pass

        # Clear ingestion flag
        try:
            INGEST_FLAG_FILE.unlink()
        except Exception:
            pass

        # Recalculate safely
        try:
            items = load_items()
            items = recalculate_all(items)
            save_items(items)
        except Exception:
            # NEVER crash the engine
            pass