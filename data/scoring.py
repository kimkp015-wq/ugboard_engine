"""SCORING LOGIC - Pure computation only, no side effects"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any
from dataclasses import dataclass
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class ScoringWeights:
    """Configurable scoring weights"""
    YOUTUBE_VIEWS: float = 1.0
    RADIO_PLAYS: float = 500.0
    TV_APPEARANCES: float = 1000.0
    TIME_BONUS_MAX: float = 1000.0
    TIME_BONUS_DECAY_PER_DAY: float = 100.0

class ScoreCalculator:
    """Pure scoring calculator with no side effects"""
    
    def __init__(self, weights: ScoringWeights = None):
        self.weights = weights or ScoringWeights()
    
    def compute_item_score(self, item: Dict[str, Any]) -> float:
        """Calculate score for one song - pure function"""
        try:
            # Extract metrics with safe defaults
            views = float(item.get("youtube_views", 0) or 0)
            radio = float(item.get("radio_plays", 0) or 0)
            tv = float(item.get("tv_appearances", 0) or 0)
            
            # Base score
            base_score = (
                views * self.weights.YOUTUBE_VIEWS +
                radio * self.weights.RADIO_PLAYS +
                tv * self.weights.TV_APPEARANCES
            )
            
            # Time bonus
            time_bonus = self._calculate_time_bonus(item)
            
            return max(0.0, base_score + time_bonus)
            
        except (TypeError, ValueError) as e:
            logger.error(f"Error computing score: {e}, item: {item.get('id', 'unknown')}")
            return 0.0
    
    def _calculate_time_bonus(self, item: Dict[str, Any]) -> float:
        """Calculate time-based freshness bonus"""
        published = item.get("published_at") or item.get("timestamp")
        if not published:
            return 0.0
        
        try:
            # Parse timestamp
            pub_date = self._parse_datetime(published)
            if not pub_date:
                return 0.0
            
            # Ensure UTC
            if pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            
            # Calculate age in days
            now = datetime.now(timezone.utc)
            days_old = (now - pub_date).total_seconds() / 86400
            
            # Apply decay
            time_bonus = max(
                0.0,
                self.weights.TIME_BONUS_MAX - 
                (days_old * self.weights.TIME_BONUS_DECAY_PER_DAY)
            )
            
            return time_bonus
            
        except Exception as e:
            logger.debug(f"Time bonus calculation failed: {e}")
            return 0.0
    
    def _parse_datetime(self, timestamp) -> datetime:
        """Parse various timestamp formats"""
        if isinstance(timestamp, datetime):
            return timestamp
        
        if isinstance(timestamp, str):
            # Handle ISO format with/without Z
            timestamp = timestamp.replace('Z', '+00:00')
            return datetime.fromisoformat(timestamp)
        
        return None

class ItemDeduplicator:
    """Handle item deduplication and merging"""
    
    @staticmethod
    def create_item_key(item: Dict[str, Any]) -> str:
        """Create deterministic key for deduplication"""
        source = item.get("source", "unknown").lower()
        external_id = str(item.get("external_id", item.get("song_id", "unknown")))
        region = item.get("region", "unknown").lower()
        title = str(item.get("title", "")).lower().replace(" ", "_")
        artist = str(item.get("artist", "")).lower().replace(" ", "_")
        
        # Create consistent hash-based key
        key_parts = f"{source}:{external_id}:{region}:{title}:{artist}"
        return hashlib.md5(key_parts.encode()).hexdigest()
    
    @staticmethod
    def merge_items(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two items with accumulation logic"""
        if not base:
            return update.copy()
        
        merged = base.copy()
        merged.update(update)
        
        # Accumulate countable metrics
        for field in ["radio_plays", "youtube_views", "tv_appearances"]:
            base_val = base.get(field, 0) or 0
            update_val = update.get(field, 0) or 0
            merged[field] = base_val + update_val
        
        # Merge lists
        for field in ["radio_stations", "tv_channels"]:
            base_list = base.get(field, []) or []
            update_list = update.get(field, []) or []
            if isinstance(base_list, list) and isinstance(update_list, list):
                merged[field] = list(set(base_list) | set(update_list))
        
        return merged

def calculate_scores(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Calculate scores for items - PURE FUNCTION, NO SIDE EFFECTS
    
    Args:
        items: List of item dictionaries
        
    Returns:
        List of items with calculated scores
    """
    if not items:
        logger.warning("calculate_scores called with empty items list")
        return []
    
    logger.info(f"Calculating scores for {len(items)} items")
    
    calculator = ScoreCalculator()
    deduplicator = ItemDeduplicator()
    
    # Deduplicate items
    item_dict = {}
    for item in items:
        key = deduplicator.create_item_key(item)
        item_dict[key] = deduplicator.merge_items(
            item_dict.get(key, {}), 
            item
        )
    
    # Calculate scores
    scored_items = []
    for key, item in item_dict.items():
        try:
            item["score"] = calculator.compute_item_score(item)
            item["last_scored"] = datetime.now(timezone.utc).isoformat()
            scored_items.append(item)
        except Exception as e:
            logger.error(f"Failed to score item {key}: {e}")
            continue
    
    logger.info(f"Successfully scored {len(scored_items)} unique items")
    return scored_items

# Legacy function for backward compatibility
def get_top_items_by_region(region: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Legacy function - should be moved to a separate query module
    This creates a circular dependency with data store
    """
    logger.warning("get_top_items_by_region called - consider moving to data.store")
    return []
