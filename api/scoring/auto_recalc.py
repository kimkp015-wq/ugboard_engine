# api/scoring/auto_recalc.py

import threading
from datetime import datetime
from data.store import load_items, save_items

# -------------------------------------------------
# Debounce / lock to prevent concurrent recalcs
# -------------------------------------------------
_recalc_lock = threading.Lock()
_last_ingestion_at = None


def mark_ingestion():
    """
    Marks that new data has arrived.
    Used to debounce recalculation.
    """
    global _last_ingestion_at
    _last_ingestion_at = datetime.utcnow()


def safe_auto_recalculate():
    """
    Safely recompute scores for all songs.
    - Thread-safe
    - Never crashes the engine
    - Can be run in background tasks
    """

    if not _recalc_lock.acquire(blocking=False):
        # Another recalculation is running
        return

    try:
        items = load_items()

        for song in items:
            youtube = song.get("youtube", 0)
            radio = song.get("radio", 0)
            tv = song.get("tv", 0)

            # Transparent scoring logic
            song["score"] = (
                youtube * 0.6 +
                radio * 0.25 +
                tv * 0.15
            )

        save_items(items)

    except Exception as e:
        # IMPORTANT: never crash the engine
        print("Auto recalculation error:", str(e))

    finally:
        _recalc_lock.release()