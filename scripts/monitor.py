# scripts/monitor.py
import requests
import time

def check_health():
    try:
        response = requests.get("https://ugboard-engine.onrender.com/health", timeout=5)
        return response.status_code == 200
    except:
        return False

if __name__ == "__main__":
    if check_health():
        print("✅ Service is healthy")
    else:
        print("❌ Service is down")
