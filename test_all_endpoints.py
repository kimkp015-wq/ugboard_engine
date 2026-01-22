#!/usr/bin/env python3
"""
Complete endpoint testing script for UG Board Engine
"""

import requests
import json
import time

BASE_URL = "http://localhost:8000"
TOKENS = {
    "admin": "admin-ug-board-2025",
    "ingest": "1994199620002019866",
    "youtube": "1994199620002019866"
}

def test_endpoint(method, endpoint, token_type=None, json_data=None):
    """Test a single endpoint"""
    headers = {}
    if token_type and token_type in TOKENS:
        headers["Authorization"] = f"Bearer {TOKENS[token_type]}"
    
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=json_data)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers)
        
        status = "✅" if response.status_code in [200, 201] else "❌"
        
        print(f"{status} {method} {endpoint} - Status: {response.status_code}")
        
        if response.status_code not in [200, 201]:
            print(f"   Error: {response.text[:100]}")
        
        return response.status_code in [200, 201]
    
    except Exception as e:
        print(f"❌ {method} {endpoint} - Exception: {e}")
        return False

def run_comprehensive_tests():
    """Run all comprehensive tests"""
    print("🚀 UG Board Engine - Comprehensive Test Suite")
    print("=" * 60)
    
    test_results = []
    
    # Public endpoints
    print("\n📡 Testing Public Endpoints:")
    print("-" * 40)
    test_results.append(("GET", "/", test_endpoint("GET", "/")))
    test_results.append(("GET", "/health", test_endpoint("GET", "/health")))
    test_results.append(("GET", "/charts/top100?limit=5", test_endpoint("GET", "/charts/top100?limit=5")))
    test_results.append(("GET", "/charts/regions", test_endpoint("GET", "/charts/regions")))
    test_results.append(("GET", "/charts/regions/central", test_endpoint("GET", "/charts/regions/central")))
    test_results.append(("GET", "/charts/trending?limit=3", test_endpoint("GET", "/charts/trending?limit=3")))
    test_results.append(("GET", "/worker/status", test_endpoint("GET", "/worker/status")))
    
    # Authentication tests
    print("\n🔐 Testing Authentication:")
    print("-" * 40)
    
    # Test without token (should fail)
    print("Testing admin without token (should fail):")
    test_endpoint("GET", "/admin/health")
    
    # Test with wrong token (should fail)
    print("Testing admin with wrong token (should fail):")
    test_endpoint("GET", "/admin/health", token_type="ingest")
    
    # Test with correct tokens
    test_results.append(("GET", "/admin/health (admin)", test_endpoint("GET", "/admin/health", token_type="admin")))
    test_results.append(("GET", "/admin/status", test_endpoint("GET", "/admin/status", token_type="admin")))
    test_results.append(("GET", "/admin/scrapers", test_endpoint("GET", "/admin/scrapers", token_type="admin")))
    test_results.append(("GET", "/scrapers/cache", test_endpoint("GET", "/scrapers/cache", token_type="ingest")))
    test_results.append(("GET", "/scrapers/active", test_endpoint("GET", "/scrapers/active", token_type="ingest")))
    
    # Ingestion tests
    print("\n📥 Testing Ingestion Endpoints:")
    print("-" * 40)
    
    youtube_payload = {
        "items": [{
            "title": "Test YouTube Song",
            "artist": "Bobi Wine",
            "plays": 1000,
            "score": 85.5,
            "region": "central",
            "station": "YouTube"
        }],
        "source": "test_script",
        "channel_id": "test_channel_001"
    }
    
    tv_payload = {
        "items": [{
            "title": "Test TV Song",
            "artist": "Eddy Kenzo",
            "plays": 800,
            "score": 82.0,
            "region": "central",
            "station": "NTV Uganda"
        }],
        "source": "test_script"
    }
    
    radio_payload = {
        "items": [{
            "title": "Test Radio Song",
            "artist": "Azawi",
            "plays": 600,
            "score": 78.5,
            "region": "central",
            "station": "CBS FM"
        }],
        "source": "test_script"
    }
    
    test_results.append(("POST", "/ingest/youtube", test_endpoint("POST", "/ingest/youtube", token_type="youtube", json_data=youtube_payload)))
    test_results.append(("POST", "/ingest/tv", test_endpoint("POST", "/ingest/tv", token_type="ingest", json_data=tv_payload)))
    test_results.append(("POST", "/ingest/radio", test_endpoint("POST", "/ingest/radio", token_type="ingest", json_data=radio_payload)))
    
    # Scraper tests (if scripts exist)
    print("\n🕸️ Testing Scraper Endpoints:")
    print("-" * 40)
    
    scraper_payload = {
        "station_id": "ntv",
        "scraper_type": "tv",
        "force_refresh": False
    }
    
    test_results.append(("POST", "/scrapers/tv/run", test_endpoint("POST", "/scrapers/tv/run", token_type="ingest", json_data=scraper_payload)))
    
    # Check data after ingestion
    time.sleep(2)  # Wait a bit for data processing
    print("\n📊 Verifying Data After Ingestion:")
    print("-" * 40)
    
    response = requests.get(f"{BASE_URL}/charts/top100?limit=10")
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Top 100 now has {len(data.get('entries', []))} songs")
        if data.get('entries'):
            print(f"   Top song: {data['entries'][0].get('title', 'N/A')} by {data['entries'][0].get('artist', 'N/A')}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, _, success in test_results if success)
    total = len(test_results)
    
    for method, endpoint, success in test_results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} | {method} {endpoint}")
    
    print(f"\nTotal: {passed}/{total} passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Your UG Board Engine is working perfectly!")
    else:
        print(f"\n⚠️ {total - passed} test(s) failed. Check the logs for details.")
    
    # Display important URLs
    print("\n🔗 Important URLs:")
    print(f"   API Documentation: {BASE_URL}/docs")
    print(f"   ReDoc Documentation: {BASE_URL}/redoc")
    print(f"   Health Check: {BASE_URL}/health")
    print(f"   Monitor: {BASE_URL}/monitor (if monitor.html exists)")

if __name__ == "__main__":
    run_comprehensive_tests()
