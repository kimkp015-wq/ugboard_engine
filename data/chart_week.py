# data/chart_week.py - WORKING VERSION
import json
import os
from datetime import datetime
from typing import Dict, Optional, Any

def ensure_data_dir():
    """Ensure data directory exists"""
    os.makedirs("data", exist_ok=True)

def get_current_week_id() -> str:
    """Get current week ID (e.g., 2026-W03) - SIMPLE VERSION"""
    try:
        # Try to read from file first
        if os.path.exists("data/current_week.json"):
            with open("data/current_week.json", "r") as f:
                data = json.load(f)
                return data.get("week_id", "2026-W03")
        
        # If file doesn't exist, create it
        now = datetime.now()
        week_num = now.isocalendar()[1]
        week_id = f"{now.year}-W{week_num:02d}"
        
        week_data = {
            "week_id": week_id,
            "start_date": now.isoformat(),
            "status": "tracking",
            "initialized_at": datetime.utcnow().isoformat()
        }
        
        ensure_data_dir()
        with open("data/current_week.json", "w") as f:
            json.dump(week_data, f, indent=2)
        
        return week_id
        
    except Exception as e:
        # Fallback to calculated week
        now = datetime.now()
        week_num = now.isocalendar()[1]
        return f"{now.year}-W{week_num:02d}"

def current_chart_week() -> Dict[str, Any]:
    """Get current chart week data - SIMPLE VERSION"""
    try:
        ensure_data_dir()
        
        if os.path.exists("data/current_week.json"):
            with open("data/current_week.json", "r") as f:
                return json.load(f)
        
        # Create default
        week_id = get_current_week_id()
        week_data = {
            "week_id": week_id,
            "start_date": datetime.now().isoformat(),
            "status": "tracking",
            "initialized_at": datetime.utcnow().isoformat()
        }
        
        with open("data/current_week.json", "w") as f:
            json.dump(week_data, f, indent=2)
        
        return week_data
        
    except Exception as e:
        # Return minimal data
        week_id = get_current_week_id()
        return {
            "week_id": week_id,
            "status": "tracking",
            "error": str(e)[:100]
        }

def open_new_tracking_week() -> Dict[str, Any]:
    """Open a new tracking week - SIMPLE VERSION"""
    week_id = get_current_week_id()
    week_data = {
        "week_id": week_id,
        "start_date": datetime.now().isoformat(),
        "status": "tracking",
        "opened_at": datetime.utcnow().isoformat()
    }
    
    ensure_data_dir()
    with open("data/current_week.json", "w") as f:
        json.dump(week_data, f, indent=2)
    
    return week_data

def close_tracking_week() -> Dict[str, Any]:
    """Close current tracking week - SIMPLE VERSION"""
    week_data = current_chart_week()
    week_data["status"] = "closed"
    week_data["end_date"] = datetime.now().isoformat()
    week_data["closed_at"] = datetime.utcnow().isoformat()
    
    with open("data/current_week.json", "w") as f:
        json.dump(week_data, f, indent=2)
    
    return week_data

def get_index() -> Dict[str, Any]:
    """Get system index - SIMPLE VERSION"""
    ensure_data_dir()
    
    if os.path.exists("data/index.json"):
        with open("data/index.json", "r") as f:
            return json.load(f)
    
    # Create default index
    index_data = {
        "initialized": True,
        "first_week": get_current_week_id(),
        "weeks_published": [],
        "created_at": datetime.utcnow().isoformat()
    }
    
    with open("data/index.json", "w") as f:
        json.dump(index_data, f, indent=2)
    
    return index_data

def record_week_publish(week_id: str):
    """Record that a week was published - SIMPLE VERSION"""
    try:
        index_data = get_index()
        
        if "weeks_published" not in index_data:
            index_data["weeks_published"] = []
        
        if week_id not in index_data["weeks_published"]:
            index_data["weeks_published"].append(week_id)
        
        index_data["last_publish"] = datetime.utcnow().isoformat()
        
        with open("data/index.json", "w") as f:
            json.dump(index_data, f, indent=2)
    except:
        pass  # Silently fail

def week_already_published(week_id: str) -> bool:
    """Check if week was already published - SIMPLE VERSION"""
    try:
        index_data = get_index()
        return week_id in index_data.get("weeks_published", [])
    except:
        return False

# Add these functions for compatibility
def is_week_initialized() -> bool:
    """Check if week is initialized"""
    return os.path.exists("data/current_week.json")

def update_week_index(week_id: str):
    """Update week index - alias for record_week_publish"""
    record_week_publish(week_id)
