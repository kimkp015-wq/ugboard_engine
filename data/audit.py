# data/audit.py

import json
from pathlib import Path
from datetime import datetime

AUDIT_FILE = Path("data/audit_log.json")


def log_audit(entry: dict):
    entry["timestamp"] = entry.get(
        "timestamp",
        datetime.utcnow().isoformat()
    )
def get_last_publish_event(week: str) -> dict | None:
    """
    Returns last publish audit record for the given chart week,
    or None if not found.
    """
    if AUDIT_FILE.exists():
        data = json.loads(AUDIT_FILE.read_text())
    else:
        data = []

    data.append(entry)
    AUDIT_FILE.write_text(json.dumps(data, indent=2))