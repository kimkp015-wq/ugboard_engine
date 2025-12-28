from pathlib import Path
import json
from datetime import datetime
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")
REPLAY_DIR = Path("data/replays")
LOG_FILE = REPLAY_DIR / "logs.json"


def replay_week(week_id: str, chart: list, config_version: str) -> None:
    """
    Save a replayed chart.
    NEVER publishes.
    NEVER mutates history.
    """
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "week_id": week_id,
        "mode": "replay",
        "generated_at": datetime.now(EAT).isoformat(),
        "config_version": config_version,
        "items": chart,
    }

    path = REPLAY_DIR / f"{week_id}.json"
    path.write_text(json.dumps(payload, indent=2))

    _log_replay(week_id, config_version)


def _log_replay(week_id: str, config_version: str):
    logs = []
    if LOG_FILE.exists():
        try:
            logs = json.loads(LOG_FILE.read_text())
        except Exception:
            logs = []

    logs.append({
        "week_id": week_id,
        "config_version": config_version,
        "at": datetime.now(EAT).isoformat(),
    })

    LOG_FILE.write_text(json.dumps(logs, indent=2))