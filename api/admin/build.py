# api/admin/build.py
from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
import json

from data.permissions import ensure_admin_allowed
from data.store import load_items, save_items
from data.region_store import lock_region, unlock_region, is_region_locked
from data.region_snapshots import save_region_snapshot
from data.chart_week import get_current_week_id
from api.charts.scoring import calculate_scores

router = APIRouter()

VALID_REGIONS = ("Eastern", "Northern", "Western")

@router.post(
    "/regions/{region}/build",
    summary="(Admin) Build region chart manually",
    tags=["Admin"]
)
def build_region_chart(
    region: str,
    force: bool = False,
    _: None = Depends(ensure_admin_allowed)
):
    """
    Build a region chart manually.
    
    Steps:
    1. Load all items
    2. Score them
    3. Filter by region
    4. Sort by score
    5. Take top 5
    6. Save snapshot
    7. Lock region (optional)
    """
    region = region.title()
    
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid region. Valid: {VALID_REGIONS}"
        )
    
    # Check if already locked (and not forcing)
    if is_region_locked(region) and not force:
        return {
            "status": "skipped",
            "reason": "Region already locked",
            "region": region,
            "locked": True
        }
    
    # Unlock if forcing rebuild
    if force and is_region_locked(region):
        unlock_region(region)
    
    try:
        # 1. Load items
        items = load_items()
        
        if not items:
            raise HTTPException(
                status_code=404,
                detail="No items found in database"
            )
        
        # 2. Score items
        scored_items = calculate_scores()
        
        if not scored_items:
            raise HTTPException(
                status_code=500,
                detail="Scoring failed - no scored items returned"
            )
        
        # 3. Filter by region
        region_items = [
            item for item in scored_items 
            if item.get("region", "").title() == region
        ]
        
        if not region_items:
            raise HTTPException(
                status_code=404,
                detail=f"No items found for region: {region}"
            )
        
        # 4. Sort by score
        region_items.sort(
            key=lambda x: float(x.get("score", 0)),
            reverse=True
        )
        
        # 5. Take top 5
        top5 = region_items[:5]
        
        # 6. Format for snapshot
        formatted_items = []
        for idx, item in enumerate(top5, 1):
            formatted_items.append({
                "position": idx,
                "title": item.get("title", "Unknown"),
                "artist": item.get("artist", "Unknown"),
                "score": item.get("score", 0),
                "youtube": item.get("youtube_views", 0),
                "radio": item.get("radio_plays", 0),
                "tv": item.get("tv_appearances", 0),
                "region": item.get("region", region)
            })
        
        # 7. Save snapshot
        week_id = get_current_week_id()
        snapshot_data = {
            "week_id": week_id,
            "region": region,
            "locked": True,
            "created_at": datetime.utcnow().isoformat(),
            "count": len(formatted_items),
            "items": formatted_items
        }
        
        save_region_snapshot(region, snapshot_data)
        
        # 8. Lock region
        lock_region(region)
        
        return {
            "status": "success",
            "region": region,
            "week_id": week_id,
            "count": len(formatted_items),
            "items": formatted_items,
            "snapshot_saved": True,
            "region_locked": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Chart build failed: {str(e)}"
        )

@router.post(
    "/regions/{region}/unlock",
    summary="(Admin) Unlock region for rebuild",
    tags=["Admin"]
)
def unlock_region_endpoint(
    region: str,
    _: None = Depends(ensure_admin_allowed)
):
    """Unlock a region to allow rebuilding"""
    region = region.title()
    
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
    
    try:
        unlock_region(region)
        return {
            "status": "success",
            "region": region,
            "locked": False,
            "message": "Region unlocked for rebuilding"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unlock region: {str(e)}"
        )
