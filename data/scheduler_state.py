from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")
FILE = Path("data/last_scheduler_run.txt")


def record_scheduler_run():
    FILE.parent.mkdir(parents=True, exist_ok=True)
    FILE.write_text(datetime.now(EAT).isoformat())


def get_last_scheduler_run() -> str | None:
    if not FILE.exists():
        return None
    return FILE.read_text().strip()