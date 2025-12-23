# data/index.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")
INDEX_FILE = Path("data/index.json")


def _load() -> dict:
    if not INDEX_FILE.exists():
        return {}
    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        return {}


def _save(state: dict) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    INDEX_FILE.write_text(json.dumps(state, indent=2))


# -------------------------
# CHART WEEK
# -------------------------
def update_chart_week(**updates):
    state = _load()
    state.setdefault("chart_week", {})
    state["chart_week"].update(updates)
    _save(state)


# -------------------------
# REGIONS
# -------------------------
def lock_region(region: str):
    state = _load()
    state.setdefault("regions", {})
    state["regions"].setdefault(region, {})
    state["regions"][region]["locked"] = True
    state["regions"][region]["last_snapshot"] = datetime.now(EAT).isoformat()
    _save(state)


# -------------------------
# SCHEDULER
# -------------------------
def record_scheduler(source: str):
    state = _load()
    state.setdefault("scheduler", {})
    state["scheduler"]["last_run"] = datetime.now(EAT).isoformat()
    state["scheduler"]["source"] = source
    _save(state)