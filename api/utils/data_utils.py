# utils/data_utils.py - Pandas alternative for Python 3.13
"""
Pandas-free data utilities for Python 3.13 compatibility
"""

import json
from datetime import datetime
from typing import List, Dict, Any
import csv

class SimpleDataFrame:
    """Minimal pandas.DataFrame replacement"""
    
    def __init__(self, data: List[Dict[str, Any]] = None):
        self.data = data or []
        self.columns = list(data[0].keys()) if data else []
    
    @classmethod
    def read_json(cls, filepath: str):
        with open(filepath, 'r') as f:
            data = json.load(f)
        return cls(data)
    
    def to_dict(self, orient: str = "records") -> List[Dict]:
        return self.data
    
    def sort_values(self, by: str, ascending: bool = True):
        sorted_data = sorted(
            self.data,
            key=lambda x: x.get(by, 0),
            reverse=not ascending
        )
        return SimpleDataFrame(sorted_data)
    
    def head(self, n: int = 5):
        return SimpleDataFrame(self.data[:n])
    
    def __len__(self):
        return len(self.data)

def calculate_chart_stats(songs: List[Dict]) -> Dict[str, Any]:
    """Calculate chart statistics without pandas"""
    if not songs:
        return {}
    
    total_plays = sum(s.get("plays", 0) for s in songs)
    avg_score = sum(s.get("score", 0) for s in songs) / len(songs)
    ugandan_count = sum(1 for s in songs if s.get("is_ugandan", False))
    
    # Find top artist
    artist_counts = {}
    for song in songs:
        artist = song.get("artist", "Unknown")
        artist_counts[artist] = artist_counts.get(artist, 0) + 1
    
    top_artist = max(artist_counts.items(), key=lambda x: x[1])[0] if artist_counts else "None"
    
    return {
        "total_songs": len(songs),
        "total_plays": total_plays,
        "average_score": round(avg_score, 2),
        "ugandan_songs": ugandan_count,
        "ugandan_percentage": round((ugandan_count / len(songs)) * 100, 1),
        "top_artist": top_artist,
        "unique_artists": len(artist_counts)
    }

def filter_ugandan_songs(songs: List[Dict]) -> List[Dict]:
    """Filter for Ugandan songs using simple rules"""
    ugandan_keywords = [
        "bobi", "kenzo", "sheebah", "daddy", "gravity", 
        "fik", "spice", "pallaso", "navio", "zamba"
    ]
    
    def is_ugandan(song: Dict) -> bool:
        artist = song.get("artist", "").lower()
        return any(keyword in artist for keyword in ugandan_keywords)
    
    return [s for s in songs if is_ugandan(s)]
