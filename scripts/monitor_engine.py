# scripts/monitor_engine.py
import requests
import time
from datetime import datetime
import json

class EngineMonitor:
    def __init__(self, engine_url: str):
        self.engine_url = engine_url.rstrip('/')
    
    def check_all_endpoints(self):
        """Check all critical endpoints"""
        endpoints = [
            ("/", "Root"),
            ("/health", "Health"),
            ("/charts/top100", "Top 100"),
            ("/charts/regions/ug", "Region UG"),
            ("/charts/trending", "Trending"),
        ]
        
        results = []
        for endpoint, name in endpoints:
            result = self.check_endpoint(endpoint, name)
            results.append(result)
            
            # Small delay between requests
            time.sleep(0.5)
        
        return results
    
    def check_endpoint(self, endpoint: str, name: str):
        """Check a single endpoint"""
        url = f"{self.engine_url}{endpoint}"
        
        try:
            start = time.time()
            response = requests.get(url, timeout=10)
            elapsed = time.time() - start
            
            return {
                "endpoint": endpoint,
                "name": name,
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "status_code": response.status_code,
                "response_time": round(elapsed, 3),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "endpoint": endpoint,
                "name": name,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def generate_report(self):
        """Generate monitoring report"""
        results = self.check_all_endpoints()
        
        healthy = sum(1 for r in results if r["status"] == "healthy")
        total = len(results)
        
        report = {
            "engine_url": self.engine_url,
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_endpoints": total,
                "healthy": healthy,
                "unhealthy": total - healthy,
                "health_percentage": round((healthy / total) * 100, 1)
            },
            "details": results
        }
        
        return report

if __name__ == "__main__":
    monitor = EngineMonitor("https://ugboard-engine.onrender.com")
    report = monitor.generate_report()
    
    print(json.dumps(report, indent=2))
    
    if report["summary"]["health_percentage"] < 100:
        print(f"\n⚠️  Warning: {report['summary']['unhealthy']} endpoints unhealthy")
        for detail in report["details"]:
            if detail["status"] != "healthy":
                print(f"  - {detail['name']} ({detail['endpoint']}): {detail['status']}")
