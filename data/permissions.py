# data/permissions.py

from fastapi import HTTPException
from data.chart_week import is_tracking_open, is_frozen_period

def ensure_injection_allowed():
    """
    Raise HTTPException if injections are NOT allowed by time rules.
    """
    if not is_tracking_open():
        raise HTTPException(
            status_code=403,
            detail="Write operations (inject/publish) are forbidden outside tracking period"
        )