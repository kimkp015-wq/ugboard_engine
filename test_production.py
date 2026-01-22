#!/usr/bin/env python3
"""
Complete production testing script for UG Board Engine
"""

import requests
import json
import sys

BASE_URL = "https://ugboard-1ubboardengine1-cf8eb3a3.koyeb.app"
TOKENS = {
    "admin": "admin-ug-board-2025",
    "ingest": "1994199620002019866",
    "youtube": "1994199620002019866"
}

def test_endpoint(name, method, endpoint, token_type=None, data=None):
    """Test a single endpoint"""
    headers = {}
    if token_type and token_type in TOKENS:
        headers["Authorization"] = f"Bearer {TOKENS[token_type]}"
    
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=30)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=30)
        
        success = response.status_code in [200, 201]
        status = "✅" if success else "❌"
        
        print(f"{status} {name}")
        
        if not success:
            print(f"   Status: {response.status_code}")
            print(f"   Error: {response.text[:200]}")
        
        return success, response
    
    except requests.exceptions.RequestException as e:
        print(f"❌ {name} - Connection Error: {e}")
        return False, None
    except Exception as e:
        print(f"❌ {name} - Exception: {e}")
        return False, None

def run_comprehensive_production_tests():
    """Run all production tests"""
    print(f"🚀 UG Board Engine - Production Test Suite")
    print(f"📡 Testing: {BASE_URL}")
    print("=" * 60)
    
    results = []
    
    # Public endpoints (no auth required)
    print("\n📡 Testing Public Endpoints:")
    print("-" * 40)
    
    results.append(test_endpoint("Root", "GET", "/"))
    results.append(test_endpoint("Health", "GET", "/health"))
    results.append(test_endpoint("Top 100", "GET", "/charts/top100?limit=5"))
    results.append(test_endpoint("Trending", "GET", "/charts/trending?limit=3"))
    results.append(test_endpoint("Regions", "GET", "/charts/regions"))
    results.append(test_endpoint("Central Region", "GET", "/charts/regions/central"))
    
    # Admin endpoints (admin token required)
    print("\n🔐 Testing Admin Endpoints:")
    print("-" * 40)
    
    results.append(test_endpoint("Admin Stats", "GET", "/admin/stats", "admin"))
    
    # Scraper endpoints (ingest token required)
    print("\n🕸️ Testing Scraper Endpoints:")
    print("-" * 40)
    
    # Test TV scraper
    results.append(test_endpoint("TV Scraper Status", "GET", "/scrapers/tv?station_id=ntv", "ingest"))
    
    # Test Radio scraper
    results.append(test_endpoint("Radio Scraper Status", "GET", "/scrapers/radio?station_id=capital", "ingest"))
    
    # YouTube endpoints
    print("\n📹 Testing YouTube Endpoints:")
    print("-" * 40)
    
    results.append(test_endpoint("YouTube Status", "GET", "/youtube/status", "ingest"))
    
    # Ingestion tests
    print("\n📥 Testing Ingestion Endpoints:")
    print("-" * 40)
    
    youtube_payload = {
        "items": [{
            "title": "Production Test Song",
            "artist": "Bobi Wine",
            "plays": 5000,
            "score": 85.5,
            "region": "central",
            "station": "YouTube Test"
        }],
        "source": "production_test",
        "channel_id": "test_channel_prod"
    }
    
    results.append(test_endpoint("YouTube Ingestion", "POST", "/ingest/youtube", "youtube", youtube_payload))
    
    # Verify data was added
    print("\n📊 Verifying Data:")
    print("-" * 40)
    
    success, response = test_endpoint("Verify Top 100", "GET", "/charts/top100?limit=10")
    if success and response:
        data = response.json()
        print(f"   Total songs: {len(data.get('entries', []))}")
        if data.get('entries'):
            print(f"   Top song: {data['entries'][0].get('title')} by {data['entries'][0].get('artist')}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 PRODUCTION TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for success, _ in results if success)
    total = len(results)
    
    print(f"Passed: {passed}/{total} ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 ALL PRODUCTION TESTS PASSED!")
        print("   Your UG Board Engine is fully operational!")
    else:
        print(f"\n⚠️ {total - passed} test(s) failed")
        print("   Check the Koyeb logs for details")
    
    print(f"\n🔗 Production URLs:")
    print(f"   Main: {BASE_URL}")
    print(f"   Docs: {BASE_URL}/docs")
    print(f"   Health: {BASE_URL}/health")
    print(f"   Charts: {BASE_URL}/charts/top100")
    
    print(f"\n🔐 Authentication Tokens:")
    print(f"   Admin: {TOKENS['admin'][:10]}...")
    print(f"   YouTube/Ingest: {TOKENS['ingest'][:10]}...")
    
    print(f"\n🎯 Next Steps:")
    print("   1. Visit the API documentation")
    print("   2. Test scrapers with real data")
    print("   3. Monitor system logs in Koyeb")
    print("   4. Set up automated testing")

if __name__ == "__main__":
    run_comprehensive_production_tests()
