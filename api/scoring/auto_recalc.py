# api/scoring/auto_recalc.py

from data.store import load_items, save_items

_recalc_flag = False


def mark_ingestion():
    global _recalc_flag
    _recalc_flag = True


def safe_auto_recalculate():
    global _recalc_flag

    if not _recalc_flag:
        return

    items = load_items()

    # simple safe scoring
    for i in items:
        i["score"] = (
            i.get("youtube", 0)
            + i.get("radio", 0)
            + i.get("tv", 0)
        )

    save_items(items)
    _recalc_flag = False