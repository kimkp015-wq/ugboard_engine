# scripts/monitor_tv_scraper.py
"""
Monitor TV Scraper and Engine Status
"""

import requests
import json
from datetime import datetime
import os

def check_system():
    engine_url = "https://ugboard-engine.onrender.com"
    
    print("=" * 60)
    print("UG BOARD SYSTEM MONITOR")
    print("=" * 60)
    
    # Check engine health
    print("\n🔧 Engine Status:")
    try:
        health = requests.get(f"{engine_url}/health", timeout=10)
        if health.status_code == 200:
            print(f"  ✅ Engine: ONLINE ({health.json()['status']})")
        else:
            print(f"  ❌ Engine: OFFLINE (HTTP {health.status_code})")
    except:
        print("  ❌ Engine: UNREACHABLE")
    
    # Check scraper logs
    print("\n📡 Scraper Status:")
    log_file = "logs/tv_scraper.log"
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                print(f"  ✅ Logs available (last: {last_line[:80]}...)")
            else:
                print("  ⚠️  Log file empty")
    else:
        print("  ⚠️  No log file found")
    
    # Check recent scrapes
    history_file = "logs/scraping_history.json"
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history = json.load(f)
            last_scrape = history[-1] if history else None
            
            if last_scrape:
                total_songs = last_scrape.get('total_songs', 0)
                successful = last_scrape.get('successful_stations', 0)
                total_stations = last_scrape.get('total_stations', 0)
                
                print(f"  📊 Last scrape: {successful}/{total_stations} stations, {total_songs} songs")
    
    print("\n🔗 Quick Links:")
    print(f"  Engine: {engine_url}")
    print(f"  API Docs: {engine_url}/docs")
    print(f"  Admin: {engine_url}/admin/status (requires token)")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    check_system()
