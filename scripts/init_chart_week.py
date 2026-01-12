#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.chart_week import current_chart_week, open_new_tracking_week
from datetime import datetime

def initialize_chart_week():
    """Initialize chart week system"""
    print("Initializing chart week system...")
    
    try:
        # Check current week
        current_week = current_chart_week()
        print(f"Current chart week: {current_week}")
        
        # If not initialized, create new week
        if not current_week or "week_id" not in current_week:
            print("Creating new chart week...")
            week_data = open_new_tracking_week()
            print(f"New week created: {week_data}")
        else:
            print("Chart week already initialized.")
            
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        
        # Try alternative initialization
        try:
            print("Trying alternative initialization...")
            
            # Manually create week file
            import json
            week_data = {
                "week_id": f"{datetime.now().year}-W{datetime.now().isocalendar()[1]:02d}",
                "start_date": datetime.now().isoformat(),
                "status": "tracking"
            }
            
            os.makedirs("data", exist_ok=True)
            with open("data/current_week.json", "w") as f:
                json.dump(week_data, f, indent=2)
            
            print(f"Manually created week: {week_data['week_id']}")
            return True
            
        except Exception as e2:
            print(f"Alternative init also failed: {e2}")
            return False

if __name__ == "__main__":
    success = initialize_chart_week()
    sys.exit(0 if success else 1)
