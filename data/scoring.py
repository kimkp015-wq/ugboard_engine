"""
SCORING LOGIC - PUT THIS IN /data/scoring.py
"""
import json
import time
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent
ITEMS_FILE = DATA_DIR / "items.json"

def compute_score(item):
    """Calculate score for one song"""
    views = item.get("youtube_views", 0) or 0
    radio = item.get("radio_plays", 0) or 0
    tv = item.get("tv_appearances", 0) or 0
    
    # Score formula
    score = (views * 1) + (radio * 500) + (tv * 1000)
    
    # Add time bonus for new songs
    published = item.get("published_at")
    if published:
        try:
            pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
            days_old = (datetime.now() - pub_date).days
            time_bonus = max(0, 1000 - (days_old * 100))
            score += time_bonus
        except:
            pass
    
    return max(0, score)

def calculate_scores(items):
    """MAIN FIX: Add items parameter here"""
    if not items:
        return []
    
    # Load existing items
    if ITEMS_FILE.exists():
        with open(ITEMS_FILE, 'r') as f:
            all_items = json.load(f)
    else:
        all_items = []
    
    # Process new items
    for item in items:
        # Calculate score
        item["score"] = compute_score(item)
        item["last_scored"] = time.time()
        
        # Add to list (or update if exists)
        all_items.append(item)
    
    # Save back
    with open(ITEMS_FILE, 'w') as f:
        json.dump(all_items, f, indent=2)
    
    return items
