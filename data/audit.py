# data/audit.py

import json
import time
import os

LOG_PATH = "data/audit_log.jsonl"
MAX_LINES = 5000


def _rotate_if_needed():
    if not os.path.exists(LOG_PATH):
        return

    try:
        with open(LOG_PATH, "r") as f:
            lines = f.readlines()

        if len(lines) > MAX_LINES:
            with open(LOG_PATH, "w") as f:
                f.writelines(lines[-MAX_LINES:])
    except Exception:
        pass


def log_event(event_type: str, data: dict | None = None):
    try:
        os.makedirs("data", exist_ok=True)

        event = {
            "ts": int(time.time()),
            "type": event_type,
            **(data or {})
        }

        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(event) + "\n")

        _rotate_if_needed()

    except Exception:
        # Logging must NEVER crash the app
        pass