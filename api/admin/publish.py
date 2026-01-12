# api/admin/publish.py
from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime

from data.permissions import ensure_admin_allowed
from data.region_store import is_region_locked, lock_region
from data.region_snapshots import save_region_snapshot
from data.chart_week import get_current_week_id
from api.admin.build import build_region_chart

router = APIRouter()

VALID_REGIONS = ("Eastern", "Northern", "Western")

@router.post(
    "/publish/weekly",
    summary="(Admin) Publish all regions weekly",
    tags=["Admin"]
)
def publish_all_regions(
    force: bool = False,
    skip_locked: bool = True,
    _: None = Depends(ensure_admin_allowed)
):
    """
    Publish charts for all regions.
    
    This is the endpoint that's currently returning 500.
    """
    results = []
    week_id = get_current_week_id()
    
    for region in VALID_REGIONS:
        try:
            # Skip already locked regions unless forcing
            if is_region_locked(region) and skip_locked and not force:
                results.append({
                    "region": region,
                    "status": "skipped",
                    "reason": "Already locked",
                    "success": True
                })
                continue
            
            # Build region chart
            build_result = build_region_chart(
                region=region,
                force=force,
                _=None  # Auth already checked
            )
            
            results.append({
                "region": region,
                "status": "published",
                **build_result
            })
            
        except Exception as e:
            results.append({
                "region": region,
                "status": "failed",
                "error": str(e),
                "success": False
            })
    
    # Count successes
    success_count = sum(1 for r in results if r.get("success", False))
    
    return {
        "status": "completed",
        "week_id": week_id,
        "timestamp": datetime.utcnow().isoformat(),
        "regions_processed": len(results),
        "regions_successful": success_count,
        "regions_failed": len(results) - success_count,
        "results": results
    }
