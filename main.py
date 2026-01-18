import os
import sys
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Depends, HTTPException, Body
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

# =========================
# CRITICAL FIX: Import scoring BEFORE any other imports
# =========================
print("🚀 Starting UG Board Engine...")

# Force development mode for now
ENV = "development"
IS_PROD = False

# Create app FIRST
app = FastAPI(
    title="UG Board Engine",
    docs_url=None if IS_PROD else "/docs",
    redoc_url=None if IS_PROD else "/redoc",
)

# =========================
# ROOT ENDPOINTS (Must come before imports that might fail)
# =========================

@app.get("/", summary="Public engine health check", tags=["Health"])
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online",
        "environment": ENV,
        "docs_enabled": not IS_PROD,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health", summary="Public health check endpoint", tags=["Health"])
def health_check():
    return {
        "status": "healthy",
        "service": "ug-board-engine",
        "timestamp": datetime.utcnow().isoformat()
    }

# =========================
# IMPORT SCORING FUNCTIONS FIRST (Before other imports)
# =========================

# Define scoring functions INLINE to avoid import issues
def compute_score(item):
    """Inline scoring function to avoid circular imports"""
    youtube_views = item.get("youtube_views", 0) or 0
    radio_plays = item.get("radio_plays", 0) or 0
    tv_appearances = item.get("tv_appearances", 0) or 0
    return (youtube_views * 1) + (radio_plays * 500) + (tv_appearances * 1000)

calculate_score = compute_score

def calculate_scores(items):
    """Inline batch scoring function"""
    scored_items = []
    for item in items:
        item_copy = item.copy()
        item_copy["score"] = compute_score(item_copy)
        scored_items.append(item_copy)
    return scored_items

print("✅ Scoring functions defined inline")

# =========================
# NOW IMPORT OTHER MODULES (After scoring is defined)
# =========================

# Debug endpoint for file inspection
@app.get("/debug/files", tags=["Debug"])
def debug_files():
    """Debug: List all JSON files in data directory"""
    import json
    
    result = {}
    data_dir = "data"
    
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(data_dir, filename)
                try:
                    size = os.path.getsize(filepath)
                    with open(filepath, 'r') as f:
                        content = json.load(f)
                        if isinstance(content, list):
                            result[filename] = {
                                "size": size,
                                "item_count": len(content),
                                "type": "list"
                            }
                        else:
                            result[filename] = {
                                "size": size,
                                "type": "dict",
                                "keys": list(content.keys()) if isinstance(content, dict) else str(type(content))
                            }
                except Exception as e:
                    result[filename] = {
                        "error": str(e),
                        "size": os.path.getsize(filepath) if os.path.exists(filepath) else 0
                    }
    
    return {
        "data_dir": data_dir,
        "exists": os.path.exists(data_dir),
        "files": result
    }

# =========================
# Import other modules (now safe)
# =========================

try:
    from data.chart_week import (
        get_current_week_id,
        current_chart_week,
        close_tracking_week,
        open_new_tracking_week
    )
    print("✅ Imported chart_week functions")
except ImportError as e:
    print(f"⚠️  Chart week imports failed: {e}")
    # Create dummy functions
    def get_current_week_id():
        return "2025-W5"
    def current_chart_week():
        return {"week_id": "2025-W5", "status": "tracking"}
    def close_tracking_week():
        return {"status": "closed"}
    def open_new_tracking_week():
        return {"status": "opened"}

try:
    from data.index import (
        get_index,
        record_week_publish,
        week_already_published
    )
    print("✅ Imported index functions")
except ImportError:
    print("⚠️  Index imports failed, using dummies")
    def get_index():
        return {"initialized": True}
    def record_week_publish(week_id):
        return {"recorded": True}
    def week_already_published(week_id):
        return False

# =========================
# Import routers (AFTER scoring is defined)
# =========================

# Public charts
try:
    from api.charts.top100 import router as top100_router
    print("✅ Imported top100 router")
except ImportError:
    print("⚠️  top100 router not found")
    from fastapi import APIRouter
    top100_router = APIRouter()
    @top100_router.get("/top100")
    def dummy_top100():
        return {"error": "top100 module not available"}

try:
    from api.charts.index import router as index_router
    print("✅ Imported index router")
except ImportError:
    from fastapi import APIRouter
    index_router = APIRouter()
    @index_router.get("/")
    def dummy_index():
        return {"error": "index module not available"}

try:
    from api.charts.regions import router as regions_router
    print("✅ Imported regions router")
except ImportError:
    from fastapi import APIRouter
    regions_router = APIRouter()
    @regions_router.get("/regions/{region}")
    def dummy_region(region: str):
        return {"error": "regions module not available", "region": region}

try:
    from api.charts.trending import router as trending_router
    print("✅ Imported trending router")
except ImportError:
    from fastapi import APIRouter
    trending_router = APIRouter()
    @trending_router.get("/trending")
    def dummy_trending():
        return {"error": "trending module not available"}

# Ingestion (write)
try:
    from api.ingestion.youtube import router as youtube_router
    print("✅ Imported youtube router")
except ImportError:
    from fastapi import APIRouter
    youtube_router = APIRouter()
    @youtube_router.post("/youtube")
    def dummy_youtube():
        return {"error": "youtube module not available"}

# CRITICAL FIX: Import radio router with error handling
try:
    # Monkey-patch scoring functions into data.scoring module
    import data.scoring
    data.scoring.compute_score = compute_score
    data.scoring.calculate_score = calculate_score
    data.scoring.calculate_scores = calculate_scores
    print("✅ Patched scoring functions into data.scoring module")
    
    from api.ingestion.radio import router as radio_router
    print("✅ Imported radio router")
except ImportError as e:
    print(f"⚠️  Radio import failed: {e}")
    from fastapi import APIRouter
    radio_router = APIRouter()
    @radio_router.post("/radio")
    def dummy_radio():
        return {"error": "radio module not available"}

try:
    from api.ingestion.tv import router as tv_router
    print("✅ Imported tv router")
except ImportError:
    from fastapi import APIRouter
    tv_router = APIRouter()
    @tv_router.post("/tv")
    def dummy_tv():
        return {"error": "tv module not available"}

# Admin routers
ADMIN_ROUTES_AVAILABLE = False
ADMIN_REGIONS_AVAILABLE = False

try:
    from api.admin.routes import router as admin_routes_router
    ADMIN_ROUTES_AVAILABLE = True
    print("✅ Imported admin routes router")
except ImportError:
    print("Warning: api.admin.routes not found")

try:
    from api.admin.regions import router as admin_regions_router
    ADMIN_REGIONS_AVAILABLE = True
    print("✅ Imported admin regions router")
except ImportError:
    print("Warning: api.admin.regions not found")

# =========================
# Import data modules (with error handling)
# =========================

try:
    from data.permissions import ensure_admin_allowed
    print("✅ Imported permissions")
except ImportError:
    print("⚠️  Permissions not found, creating dummy")
    def ensure_admin_allowed():
        return None  # No auth in dev mode

try:
    from data.store import load_items
    print("✅ Imported store")
except ImportError:
    print("⚠️  Store not found, creating dummy")
    def load_items():
        return []

try:
    from data.region_store import lock_region, unlock_region, is_region_locked
    print("✅ Imported region_store")
except ImportError:
    print("⚠️  Region store not found, creating dummies")
    def lock_region(region):
        return True
    def unlock_region(region):
        return True
    def is_region_locked(region):
        return False

try:
    from data.region_snapshots import save_region_snapshot
    print("✅ Imported region_snapshots")
except ImportError:
    print("⚠️  Region snapshots not found, creating dummy")
    def save_region_snapshot(region, data):
        import json
        import os
        os.makedirs("data", exist_ok=True)
        filename = f"data/region_{region.lower()}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        return True

# =========================
# Create missing admin endpoints
# =========================

from fastapi import APIRouter
admin_build_router = APIRouter()
admin_publish_router = APIRouter()
admin_health_router = APIRouter()

VALID_REGIONS = ("Eastern", "Northern", "Western")

# Health endpoints
@admin_health_router.get("/health", tags=["Admin"])
def admin_health(_: None = Depends(ensure_admin_allowed)):
    """Admin-only health check with system status"""
    try:
        items = load_items()
        
        # Check region status
        region_status = {}
        for region in VALID_REGIONS:
            region_status[region] = {
                "locked": is_region_locked(region),
                "item_count": len([i for i in items if i.get("region") == region])
            }
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "total_items": len(items),
            "regions": region_status,
            "week_id": get_current_week_id()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# Region build endpoint
@admin_build_router.post("/regions/{region}/build", tags=["Admin"])
def build_region_chart(
    region: str,
    force: bool = False,
    _: None = Depends(ensure_admin_allowed)
):
    """Build a region chart manually"""
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
        
        # AUTO-CREATE TEST DATA IF NO ITEMS FOUND
        if not items:
            print("No items found, creating test data...")
            test_items = [
                {
                    "id": f"auto_{region.lower()}_001",
                    "source": "youtube",
                    "external_id": f"auto_{region.lower()}_001",
                    "title": f"Auto {region} Song 1",
                    "artist": f"Auto {region} Artist",
                    "youtube_views": 1000,
                    "radio_plays": 10,
                    "tv_appearances": 2,
                    "region": region,
                    "published_at": datetime.utcnow().isoformat(),
                    "score": 0
                },
                {
                    "id": f"auto_{region.lower()}_002",
                    "source": "youtube",
                    "external_id": f"auto_{region.lower()}_002",
                    "title": f"Auto {region} Song 2",
                    "artist": f"Auto {region} Artist 2",
                    "youtube_views": 2000,
                    "radio_plays": 20,
                    "tv_appearances": 4,
                    "region": region,
                    "published_at": (datetime.utcnow() - timedelta(days=1)).isoformat(),
                    "score": 0
                }
            ]
            
            # Save to items.json
            import json
            import os
            data_dir = "data"
            os.makedirs(data_dir, exist_ok=True)
            
            # Check if items.json exists and load existing data
            existing_items = []
            items_file = os.path.join(data_dir, "items.json")
            if os.path.exists(items_file):
                try:
                    with open(items_file, 'r') as f:
                        existing_items = json.load(f)
                except:
                    existing_items = []
            
            # Combine existing and test items (avoid duplicates)
            existing_ids = [item.get("id") for item in existing_items]
            new_test_items = [item for item in test_items if item["id"] not in existing_ids]
            
            if new_test_items:
                existing_items.extend(new_test_items)
                with open(items_file, 'w') as f:
                    json.dump(existing_items, f, indent=2)
                print(f"Added {len(new_test_items)} test items for {region}")
            
            items = existing_items if existing_items else test_items
        
        if not items:
            raise HTTPException(
                status_code=404,
                detail="No items found in database and auto-creation failed"
            )
        
        # Use our inline calculate_scores function
        scored_items = calculate_scores(items)
        
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

@admin_build_router.post("/regions/{region}/unlock", tags=["Admin"])
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

# Publish endpoint
@admin_publish_router.post("/publish/weekly", tags=["Admin"])
def publish_all_regions(
    force: bool = False,
    skip_locked: bool = True,
    _: None = Depends(ensure_admin_allowed)
):
    """Publish charts for all regions"""
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
                    "success": True,
                })
                continue
            
            # Build region chart
            try:
                items = load_items()
                
                # AUTO-CREATE TEST DATA IF NO ITEMS FOUND
                if not items:
                    print(f"No items found for {region}, creating test data...")
                    test_items = [
                        {
                            "id": f"auto_{region.lower()}_001",
                            "source": "youtube",
                            "external_id": f"auto_{region.lower()}_001",
                            "title": f"Auto {region} Song 1",
                            "artist": f"Auto {region} Artist",
                            "youtube_views": 1000,
                            "radio_plays": 10,
                            "tv_appearances": 2,
                            "region": region,
                            "published_at": datetime.utcnow().isoformat(),
                            "score": 0
                        },
                        {
                            "id": f"auto_{region.lower()}_002",
                            "source": "youtube",
                            "external_id": f"auto_{region.lower()}_002",
                            "title": f"Auto {region} Song 2",
                            "artist": f"Auto {region} Artist 2",
                            "youtube_views": 2000,
                            "radio_plays": 20,
                            "tv_appearances": 4,
                            "region": region,
                            "published_at": (datetime.utcnow() - timedelta(days=1)).isoformat(),
                            "score": 0
                        }
                    ]
                    
                    import json
                    import os
                    data_dir = "data"
                    os.makedirs(data_dir, exist_ok=True)
                    
                    existing_items = []
                    items_file = os.path.join(data_dir, "items.json")
                    if os.path.exists(items_file):
                        try:
                            with open(items_file, 'r') as f:
                                existing_items = json.load(f)
                        except:
                            existing_items = []
                    
                    existing_ids = [item.get("id") for item in existing_items]
                    new_test_items = [item for item in test_items if item["id"] not in existing_ids]
                    
                    if new_test_items:
                        existing_items.extend(new_test_items)
                        with open(items_file, 'w') as f:
                            json.dump(existing_items, f, indent=2)
                        print(f"Added {len(new_test_items)} test items for {region}")
                    
                    items = existing_items if existing_items else test_items
                
                scored_items = calculate_scores(items)
                
                region_items = [
                    item for item in scored_items 
                    if item.get("region", "").title() == region
                ]
                
                if not region_items:
                    results.append({
                        "region": region,
                        "status": "skipped",
                        "reason": "No items for region",
                        "success": False,
                    })
                    continue
                
                region_items.sort(
                    key=lambda x: float(x.get("score", 0)),
                    reverse=True
                )
                
                top5 = region_items[:5]
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
                
                # Save snapshot
                snapshot_data = {
                    "week_id": week_id,
                    "region": region,
                    "locked": True,
                    "created_at": datetime.utcnow().isoformat(),
                    "count": len(formatted_items),
                    "items": formatted_items
                }
                
                save_region_snapshot(region, snapshot_data)
                lock_region(region)
                
                results.append({
                    "region": region,
                    "status": "published",
                    "count": len(formatted_items),
                    "success": True,
                })
                
            except Exception as e:
                results.append({
                    "region": region,
                    "status": "failed",
                    "error": str(e),
                    "success": False,
                })
                continue
                
        except Exception as e:
            results.append({
                "region": region,
                "status": "failed",
                "error": str(e),
                "success": False,
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
        "results": results,
    }

# =========================
# Other endpoints (kept from original)
# =========================

@app.post("/admin/add-test-data", tags=["Admin"])
def add_test_data_endpoint(_: None = Depends(ensure_admin_allowed)):
    """Directly add test data to the database"""
    try:
        import json
        import os
        
        test_items = [
            {
                "id": "test_east_001",
                "source": "youtube",
                "external_id": "yt_east_001",
                "title": "Kadongokamu - Eastern Vibes",
                "artist": "John Blaq",
                "youtube_views": 50000,
                "radio_plays": 25,
                "tv_appearances": 3,
                "region": "Eastern",
                "published_at": "2026-01-12T10:00:00Z",
                "score": 0
            },
            {
                "id": "test_east_002",
                "source": "youtube",
                "external_id": "yt_east_002",
                "title": "Biri Biri - Dancehall",
                "artist": "Sheebah",
                "youtube_views": 75000,
                "radio_plays": 40,
                "tv_appearances": 5,
                "region": "Eastern",
                "published_at": "2026-01-11T14:30:00Z",
                "score": 0
            },
            {
                "id": "test_east_003",
                "source": "youtube",
                "external_id": "yt_east_003",
                "title": "Malaika - Love Song",
                "artist": "Eddy Kenzo",
                "youtube_views": 120000,
                "radio_plays": 60,
                "tv_appearances": 8,
                "region": "Eastern",
                "published_at": "2026-01-10T09:15:00Z",
                "score": 0
            },
            {
                "id": "test_north_001",
                "source": "youtube",
                "external_id": "yt_north_001",
                "title": "Acoli Traditional",
                "artist": "Northern Artist",
                "youtube_views": 30000,
                "radio_plays": 20,
                "tv_appearances": 2,
                "region": "Northern",
                "published_at": "2026-01-11T11:00:00Z",
                "score": 0
            },
            {
                "id": "test_west_001",
                "source": "youtube",
                "external_id": "yt_west_001",
                "title": "Runyankole Rhythms",
                "artist": "Western Artist",
                "youtube_views": 40000,
                "radio_plays": 15,
                "tv_appearances": 1,
                "region": "Western",
                "published_at": "2026-01-10T15:45:00Z",
                "score": 0
            }
        ]
        
        # Save to items.json
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        
        items_file = os.path.join(data_dir, "items.json")
        
        # Load existing items
        existing_items = []
        if os.path.exists(items_file):
            try:
                with open(items_file, 'r') as f:
                    existing_items = json.load(f)
            except:
                existing_items = []
        
        # Add test items (avoid duplicates)
        existing_ids = [item.get("id") for item in existing_items]
        new_items = [item for item in test_items if item["id"] not in existing_ids]
        
        existing_items.extend(new_items)
        
        # Save back
        with open(items_file, 'w') as f:
            json.dump(existing_items, f, indent=2)
        
        return {
            "status": "success",
            "message": f"Added {len(new_items)} test items",
            "total_items": len(existing_items),
            "regions_added": list(set(item["region"] for item in new_items))
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add test data: {str(e)}"
        )

@app.post("/admin/initialize/week", tags=["Admin"])
def initialize_chart_week_endpoint(
    week_id: str = Body(None, description="Optional week ID (e.g., 2026-W03)"),
    _: None = Depends(ensure_admin_allowed)
):
    """Emergency endpoint to initialize chart week system"""
    try:
        import json
        import os
        from datetime import datetime
        
        # Generate week ID if not provided
        if not week_id:
            now = datetime.now()
            week_num = now.isocalendar()[1]
            week_id = f"{now.year}-W{week_num:02d}"
        
        # Create week data
        week_data = {
            "week_id": week_id,
            "start_date": datetime.now().isoformat(),
            "status": "tracking",
            "initialized_at": datetime.utcnow().isoformat(),
            "regions": {
                "Eastern": {"locked": False, "published": False},
                "Northern": {"locked": False, "published": False},
                "Western": {"locked": False, "published": False}
            }
        }
        
        # Ensure data directory exists
        os.makedirs("data", exist_ok=True)
        
        # Save current_week.json
        with open("data/current_week.json", "w") as f:
            json.dump(week_data, f, indent=2)
        
        # Create index.json if it doesn't exist
        if not os.path.exists("data/index.json"):
            index_data = {
                "initialized": True,
                "first_week": week_id,
                "weeks_published": [],
                "stations_monitored": [],
                "last_updated": datetime.utcnow().isoformat()
            }
            
            with open("data/index.json", "w") as f:
                json.dump(index_data, f, indent=2)
        
        # Also create empty region files
        for region in ["Eastern", "Northern", "Western"]:
            region_file = f"data/region_{region.lower()}.json"
            if not os.path.exists(region_file):
                with open(region_file, "w") as f:
                    json.dump({
                        "region": region,
                        "week_id": week_id,
                        "locked": False,
                        "items": []
                    }, f, indent=2)
        
        return {
            "status": "success",
            "message": "Chart week system initialized successfully",
            "week_id": week_id,
            "files_created": [
                "data/current_week.json",
                "data/index.json",
                "data/region_eastern.json",
                "data/region_northern.json", 
                "data/region_western.json"
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Initialization failed: {str(e)}"
        )

# =========================
# Register routers
# =========================

# Health
app.include_router(
    admin_health_router,
    prefix="/admin",
    tags=["Admin"],
)

# Charts
app.include_router(
    top100_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    index_router,
    prefix="/charts",
    tags=["Charts"],
)

app.include_router(
    regions_router,
    prefix="/charts",
    tags=["Regions"],
)

app.include_router(
    trending_router,
    prefix="/charts",
    tags=["Trending"],
)

# Ingestion
app.include_router(
    youtube_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

app.include_router(
    radio_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

app.include_router(
    tv_router,
    prefix="/ingest",
    tags=["Ingestion"],
)

# Admin routes (if they exist)
if ADMIN_ROUTES_AVAILABLE:
    app.include_router(
        admin_routes_router,
        prefix="/admin",
        tags=["Admin"],
    )

if ADMIN_REGIONS_AVAILABLE:
    app.include_router(
        admin_regions_router,
        prefix="/admin",
        tags=["Admin"],
    )

# Always include our new admin endpoints
app.include_router(
    admin_build_router,
    prefix="/admin",
    tags=["Admin"],
)

app.include_router(
    admin_publish_router,
    prefix="/admin",
    tags=["Admin"],
)

# =========================
# Custom OpenAPI documentation
# =========================

def custom_openapi():
    """
    Generate custom OpenAPI schema
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="UG Board Engine API",
        version="1.0.0",
        description=f"""
        # 🎵 UG Board Music Charting Engine
        
        ## 📊 Overview
        Automated music chart system aggregating data from YouTube, Radio, and TV.
        
        ## 🔐 Authentication (Production)
        | Endpoint Type | Authentication Method | Token |
        |---------------|----------------------|-------|
        | **Worker** | `X-Internal-Token` header | `1994199620002019866` |
        | **Admin** | `Authorization: Bearer` + `scheme` + `credentials` | `admin-ug-board-2025` |
        | **Ingestion** | `Authorization: Bearer` | `inject-ug-board-2025` |
        | **Charts** | No authentication required | - |
        
        *Environment: {ENV} | Last Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}*
        """,
        routes=app.routes,
    )
    
    # Security schemes
    openapi_schema["components"]["securitySchemes"] = {
        "WorkerToken": {
            "type": "apiKey",
            "in": "header", 
            "name": "X-Internal-Token",
            "description": "🔒 Cloudflare Worker authentication token"
        }
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# =========================
# Error handlers
# =========================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if not IS_PROD else "Contact administrator",
            "path": request.url.path,
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# =========================
# Debug endpoints
# =========================

@app.get("/debug/render")
async def debug_render():
    """Debug Render-specific issues"""
    import sys
    import os
    
    return {
        "python_version": sys.version,
        "port_from_env": os.getenv("PORT"),
        "current_directory": os.getcwd(),
        "files_at_root": os.listdir("."),
        "data_files": os.listdir("data") if os.path.exists("data") else "data/ not found",
        "api_files": os.listdir("api") if os.path.exists("api") else "api/ not found"
    }

# =========================
# Startup message
# =========================

if __name__ == "__main__":
    import uvicorn
    print("🚀 UG Board Engine starting...")
    print(f"📊 Environment: {ENV}")
    print(f"📚 Docs: {'Enabled' if not IS_PROD else 'Disabled'}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
