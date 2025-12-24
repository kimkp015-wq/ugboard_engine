# data/chart_week.py

from datetime import datetime
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")

# =========================
# PUBLIC CONTRACT (STABLE)
# =========================

def get_current_week_id() -> str:
    """
    Canonical chart week identifier.
    This function MUST NEVER be removed.
    """
    now = datetime.now(EAT)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


# -------------------------
# BACKWARD COMPATIBILITY
# -------------------------

def current_chart_week() -> str:
    """
    Backward-compatible alias.
    DO NOT REMOVE.
    """
    return get_current_week_id()