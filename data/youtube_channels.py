# data/youtube_channels.py

import json
from pathlib import Path
from typing import List, Dict

CHANNELS_FILE = Path("data/youtube_channels.json")


def _load() -> Dict:
    if not CHANNELS_FILE.exists():
        return {"version": 1, "channels": []}

    try:
        data = json.loads(CHANNELS_FILE.read_text())
        if not isinstance(data, dict):
            return {"version": 1, "channels": []}
        return data
    except Exception:
        return {"version": 1, "channels": []}


def get_active_channels() -> List[Dict]:
    """
    Read-only list of active YouTube channels.
    """
    data = _load()
    return [
        c for c in data.get("channels", [])
        if isinstance(c, dict) and c.get("active") is True
    ]
