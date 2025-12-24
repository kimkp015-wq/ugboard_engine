# api/admin/index.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_admin_allowed
from data.index import get_index

router = APIRouter()

# =========================
# Admin index (audit)
# =========================

@router.get(
    "/index",
    summary="(Admin) Read-only weekly publish index",
    tags=["Admin"],
)
def read_index(
    _: None = Depends(ensure_admin_allowed),
):
    """
    Human-only audit endpoint.

    Shows:
    - Published weeks
    - Regions locked
    - Snapshot paths
    - Trigger source
    """
    entries = get_index()

    return {
        "status": "ok",
        "count": len(entries),
        "entries": entries,
    }