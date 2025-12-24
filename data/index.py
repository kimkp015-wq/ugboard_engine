# data/index.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List

# =========================
# Timezone (Uganda)
# =========================
EAT = ZoneInfo("Africa/Kampala")

# =========================
# Index file (immutable)
# =========================
INDEX_FILE = Path("data/index.json")


def _now() -> str:
    """
    Current timestamp in Africa/Kampala timezone.
    """
    return datetime.now(EAT).isoformat()


def _load_index() -> List[Dict]:
    """
    Load index safely.
    Never crashes the engine.
    """
    if not INDEX_FILE.exists():
        return []

    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        # Corrupt index must never crash the engine
        return []


def _save_index(entries: List[Dict]) -> None:
    """
    Persist index to disk.
    """
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    INDEX_FILE.write_text(json.dumps(entries, indent=2))


def record_week_publish(
    *,
    week_id: str,
    regions: List[str],
    trigger: str,
) -> Dict:
    """
    Append an immutable weekly publish record.

    This is the SOURCE OF TRUTH for:
    - What week was published
    - Which regions were locked
    - When it happened
    - What triggered it

    Never mutates previous entries.
    Safe for Cloudflare retries.
    """

    entries = _load_index()

    record = {
        "week_id": week_id,
        "regions": regions,
        "trigger": trigger,
        "published_at": _now(),
        "snapshots_path": f"data/region_snapshots/{week_id}/",
    }

    entries.append(record)
    _save_index(entries)

    return record


def get_index() -> List[Dict]:
    """
    Read-only access to the full publish history.
    """
    return _load_index()