"""
UG Board Engine Scoring System - SIMPLIFIED VERSION
No complex dependencies, just pure functions
"""

def compute_score(item):
    """
    Calculate score for single item.
    Formula: youtube_views*1 + radio_plays*500 + tv_appearances*1000
    """
    try:
        youtube_views = item.get("youtube_views", 0) or 0
        radio_plays = item.get("radio_plays", 0) or 0
        tv_appearances = item.get("tv_appearances", 0) or 0
        
        return (youtube_views * 1) + (radio_plays * 500) + (tv_appearances * 1000)
    except Exception:
        return 0

# Aliases for compatibility
calculate_score =
