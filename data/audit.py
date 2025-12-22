# data/audit.py

import json
from pathlib import Path
from datetime import datetime

AUDIT_FILE = Path("data/audit_log.json")


def log_audit(entry: dict):
    """
    Append an audit event to audit_log.json.
    Never crashes the engine.
    """

    entry["logged_at"] = datetime.utcnow().isoformat()

    try:
        if AUDIT_FILE.exists():
            data = json.loads(AUDIT_FILE.read_text())
            if not isinstance(data, list):
                data = []
        else:
            data = []

        data.append(entry)

        AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        AUDIT_FILE.write_text(json.dumps(data, indent=2))

    except Exception:
        # ABSOLUTE SAFETY: audit must NEVER crash engine
        pass