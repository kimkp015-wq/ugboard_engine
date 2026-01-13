"""
SCORING LOGIC - PUT THIS IN /data/scoring.py
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent
ITEMS_FILE = DATA_DIR / "items.json"

def compute_score(item):
    """Calculate score for one song"""
    try:
        views = float(item.get("youtube_views", 0) or 0)
        radio = float(item.get("radio_plays", 0) or 0)
        tv = float(item.get("tv_appearances", 0) or 0)
        
        # Score formula: views×1 + radio×500 + tv×1000
        score = (views * 1) + (radio * 500) + (tv * 1000)
        
        # Add time bonus for new songs (max 1000, decreases 100 per day)
        published = item.get("published_at") or item.get("timestamp")
        if published:
            try:
                # Parse timestamp
                if isinstance(published, str):
                    if 'Z' in published:
                        pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                    else:
                        pub_date = datetime.fromisoformat(published)
                else:
                    pub_date = published
                
                # Ensure timezone
                if pub_date.tzinfo is None:
                    pub_date = pub_date.replace(tzinfo=timezone.utc)
                
                # Calculate days old
                now = datetime.now(timezone.utc)
                days_old = (now - pub_date).total_seconds() / 86400
                
                # Time bonus: max 1000, decreases 100 per day
                time_bonus = max(0, 1000 - (days_old * 100))
                score += time_bonus
                
            except Exception as e:
                logger.debug(f"Time bonus calculation failed: {e}")
                # Continue without time bonus
        
        return max(0, score)
        
    except Exception as e:
        logger.error(f"Error computing score for item: {e}")
        return 0.0

def calculate_scores(items):
    """Calculate scores for items and update database"""
    if not items:
        logger.warning("calculate_scores called with empty items list")
        return []
    
    logger.info(f"Calculating scores for {len(items)} items")
    
    try:
        # Load existing items
        existing_items = []
        if ITEMS_FILE.exists():
            try:
                with open(ITEMS_FILE, 'r', encoding='utf-8') as f:
                    existing_items = json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"{ITEMS_FILE} is corrupted, starting fresh")
                existing_items = []
        
        # Create lookup for existing items by unique key
        item_lookup = {}
        for item in existing_items:
            key = create_item_key(item)
            item_lookup[key] = item
        
        # Process new items
        scored_items = []
        for item in items:
            try:
                # Create or update item
                key = create_item_key(item)
                existing_item = item_lookup.get(key, {})
                
                # Merge data (radio_plays accumulate)
                merged_item = merge_items(existing_item, item)
                
                # Calculate score
                merged_item["score"] = compute_score(merged_item)
                merged_item["last_scored"] = datetime.now(timezone.utc).isoformat()
                
                # Update lookup
                item_lookup[key] = merged_item
                scored_items.append(merged_item)
                
            except Exception as e:
                logger.error(f"Failed to score item: {e}")
                continue
        
        # Save all items back
        all_items = list(item_lookup.values())
        with open(ITEMS_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_items, f, indent=2, default=str)
        
        logger.info(f"Successfully scored {len(scored_items)} items, total: {len(all_items)}")
        return scored_items
        
    except Exception as e:
        logger.error(f"Failed to calculate scores: {e}")
        raise

def create_item_key(item):
    """Create unique key for an item"""
    source = item.get("source", "unknown")
    external_id = item.get("external_id", item.get("song_id", "unknown"))
    region = item.get("region", "unknown")
    
    # For radio: include station and timestamp hour for deduplication
    if source == "radio":
        station = item.get("station", "unknown")
        timestamp = item.get("timestamp", "")
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                hour = dt.strftime("%Y-%m-%d-%H")  # Group by hour
                return f"radio:{station}:{hour}:{external_id}"
            except:
                pass
    
    return f"{source}:{external_id}:{region}"

def merge_items(existing, new):
    """Merge existing and new item data"""
    merged = existing.copy() if existing else {}
    merged.update(new)
    
    # Accumulate radio plays
    if existing.get("source") == "radio" and new.get("source") == "radio":
        existing_plays = existing.get("radio_plays", 0)
        new_plays = new.get("radio_plays", 0)
        merged["radio_plays"] = existing_plays + new_plays
    
    # Merge radio stations list
    if existing.get("radio_stations") and new.get("radio_stations"):
        existing_stations = set(existing.get("radio_stations", []))
        new_stations = set(new.get("radio_stations", []))
        merged["radio_stations"] = list(existing_stations.union(new_stations))
    
    return merged

def get_top_items_by_region(region, limit=10):
    """Get top items for a region"""
    try:
        if not ITEMS_FILE.exists():
            return []
        
        with open(ITEMS_FILE, 'r', encoding='utf-8') as f:
            all_items = json.load(f)
        
        # Filter by region and sort by score
        region_items = [item for item in all_items if item.get("region") == region]
        region_items.sort(key=lambda x: x.get("score", 0), reverse=True)
        
        return region_items[:limit]
        
    except Exception as e:
        logger.error(f"Failed to get top items for {region}: {e}")
        return []
