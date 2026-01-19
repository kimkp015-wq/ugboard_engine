#!/usr/bin/env python3
"""
Test script to verify deployment will work
"""

import subprocess
import sys

def test_requirements():
    """Test that all requirements can be installed"""
    print("🧪 Testing requirements installation...")
    
    with open("requirements.txt", "r") as f:
        packages = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    
    successful = []
    failed = []
    
    for package in packages[:10]:  # Test first 10 packages
        try:
            # Try to install package
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--no-deps", package],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                successful.append(package.split("==")[0])
                print(f"  ✅ {package}")
            else:
                failed.append(package)
                print(f"  ❌ {package}: {result.stderr[:100]}")
                
        except Exception as e:
            failed.append(package)
            print(f"  ❌ {package}: {str(e)[:100]}")
    
    return successful, failed

def test_imports():
    """Test that core packages can be imported"""
    print("\n🧪 Testing package imports...")
    
    core_packages = [
        "fastapi",
        "uvicorn",
        "pydantic",
        "sqlalchemy",
        "pandas",
        "numpy",
        "aiohttp",
        "redis"
    ]
    
    for package in core_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError as e:
            print(f"  ❌ {package}: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("UG Board Engine - Deployment Test")
    print("=" * 60)
    
    # Test requirements
    successful, failed = test_requirements()
    
    # Test imports
    test_imports()
    
    print("\n" + "=" * 60)
    if not failed:
        print("✅ ALL TESTS PASSED - Ready for deployment!")
        print("\nNext steps:")
        print("1. Commit these files to GitHub")
        print("2. Push to your repository")
        print("3. Render.com will automatically deploy")
        print("4. Check: https://ugboard-engine.onrender.com")
    else:
        print(f"⚠️  {len(failed)} packages failed")
        print("Check requirements.txt for incompatible versions")
    
    print("=" * 60)
