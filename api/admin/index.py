# api/admin/index.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_admin_allowed
from data.index import get_index

router = APIRouter()


@router.get(
    "/index",
    summary="(Admin) Read-only weekly publish index",
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

    return {
        "status": "ok",
        "count": len(get_index()),
        "entries": get_index(),
    }