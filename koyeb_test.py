#!/usr/bin/env python3
"""
Test script for Koyeb deployment
"""

import requests
import json

# Your Koyeb URL
BASE_URL = "https://ugboard-1ubboardengine1-cf8eb3a3.koyeb.app"

# Tokens from your config
TOKENS = {
    "admin": "admin-ug-board-2025",
    "ingest": "1994199620002019866"
}

def test_koyeb():
    print(f"🔗 Testing Koyeb Deployment: {BASE_URL}")
    print("=" * 60)
    
    # Test public endpoints
    endpoints = [
        ("GET", "/", None),
        ("GET", "/health", None),
        ("GET", "/charts/top100?limit=3", None),
        ("GET", "/worker/status", None),
    ]
    
    for method, endpoint, token in endpoints:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {TOKENS.get(token)}"
        
        try:
            if method == "GET":
                response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, timeout=10)
            elif method == "POST":
                response = requests.post(f"{BASE_URL}{endpoint}", headers=headers, timeout=10)
            
            status = "✅" if response.status_code in [200, 201] else "❌"
            print(f"{status} {method} {endpoint} - Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"   Response: {response.text[:100]}")
        
        except requests.exceptions.RequestException as e:
            print(f"❌ {method} {endpoint} - Connection Error: {e}")
    
    print("\n📋 Quick Test Summary:")
    print("1. Open in browser: https://ugboard-1ubboardengine1-cf8eb3a3.koyeb.app/")
    print("2. API Docs: https://ugboard-1ubboardengine1-cf8eb3a3.koyeb.app/docs")
    print("3. Health: https://ugboard-1ubboardengine1-cf8eb3a3.koyeb.app/health")

if __name__ == "__main__":
    test_koyeb()
