# data/scoring.py
"""UG Board Engine Scoring System"""

def compute_score(item):
    """
    Calculate score for single item.
    Formula: youtube_views*1 + radio_plays*500 + tv_appearances*1000
    """
    youtube_views = item.get("youtube_views", 0) or 0
    radio_plays = item.get("radio_plays", 0) or 0
    tv_appearances = item.get("tv_appearances", 0) or 0
    
    return (youtube_views * 1) + (radio_plays * 500) + (tv_appearances * 1000)

# Aliases for compatibility
calculate_score = compute_score

def calculate_scores(items):
    """
    Calculate scores for multiple items.
    Returns items with 'score' field added.
    """
    scored_items = []
    for item in items:
        item_copy = item.copy()
        item_copy["score"] = compute_score(item_copy)
        scored_items.append(item_copy)
    
    return scored_items
