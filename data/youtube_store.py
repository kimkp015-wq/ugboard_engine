# data/youtube_store.py

import json
from pathlib import Path
from typing import List, Dict

STORE_FILE = Path("data/youtube_uploads.json")


def _load() -> List[Dict]:
    if not STORE_FILE.exists():
        return []

    try:
        data = json.loads(STORE_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(data: List[Dict]) -> None:
    STORE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(STORE_FILE)


def upsert_youtube_uploads(uploads: List[Dict]) -> int:
    """
    Idempotent upsert by video_id.
    """
    existing = _load()
    seen = {u["video_id"] for u in existing if "video_id" in u}

    added = 0

    for upload in uploads:
        vid = upload.get("video_id")
        if not vid or vid in seen:
            continue

        existing.append(upload)
        seen.add(vid)
        added += 1

    if added:
        _save(existing)

    return added
