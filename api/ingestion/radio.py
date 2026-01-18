# Line 7 - Replace with this
try:
    from data.scoring import calculate_scores
except ImportError:
    # Create a simple calculate_scores function
    from data.scoring import compute_score
    from datetime import datetime
    
    def calculate_scores(items):
        """Simple scoring wrapper"""
        for item in items:
            item["score"] = compute_score(item)
            item["last_scored"] = datetime.utcnow().isoformat()
        return items
