# scripts/build.py
import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description):
    """Run shell command with error handling."""
    print(f"\n🔧 {description}...")
    print(f"   Command: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Failed: {result.stderr}")
        return False
    else:
        print("✅ Success")
        if result.stdout.strip():
            print(f"   Output: {result.stdout[:200]}...")
        return True

def main():
    """Main build process for UG Board Engine."""
    print("=" * 60)
    print("UG Board Engine - Build System")
    print("=" * 60)
    
    # Step 1: Update pip
    if not run_command("python -m pip install --upgrade pip==23.3.1", "Updating pip"):
        return False
    
    # Step 2: Install system dependencies (if on Linux/macOS)
    if os.path.exists("install_audio_deps.sh"):
        if not run_command("chmod +x install_audio_deps.sh && ./install_audio_deps.sh", 
                          "Installing system dependencies"):
            print("⚠️  System dependencies may need manual installation")
    
    # Step 3: Install Python dependencies
    if Path("requirements.txt").exists():
        if not run_command("pip install --no-cache-dir -r requirements.txt", 
                          "Installing Python dependencies"):
            return False
    else:
        print("❌ requirements.txt not found")
        return False
    
    # Step 4: Optional dependencies
    if Path("requirements-optional.txt").exists():
        if not run_command("pip install --no-cache-dir -r requirements-optional.txt", 
                          "Installing optional dependencies"):
            print("⚠️  Optional dependencies failed, continuing...")
    
    # Step 5: Test the installation
    print("\n🧪 Testing installation...")
    test_cmds = [
        ("python -c \"import fastapi; print(f'FastAPI {fastapi.__version__}')\"", 
         "FastAPI"),
        ("python -c \"import librosa; print(f'Librosa {librosa.__version__}')\"", 
         "Librosa"),
        ("python -c \"import numpy; print(f'NumPy {numpy.__version__}')\"", 
         "NumPy"),
        ("python -c \"import sqlalchemy; print(f'SQLAlchemy {sqlalchemy.__version__}')\"", 
         "SQLAlchemy"),
    ]
    
    all_ok = True
    for cmd, name in test_cmds:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {name}: {result.stdout.strip()}")
        else:
            print(f"❌ {name} test failed")
            all_ok = False
    
    print("\n" + "=" * 60)
    if all_ok:
        print("🎉 BUILD SUCCESSFUL!")
        print("\nNext steps:")
        print("1. Run fingerprinting test: python scripts/test_fingerprinting.py")
        print("2. Start the API: uvicorn api.main:app --reload")
        print("3. Check TV scraper: python scripts/tv_scraper.py --help")
        return True
    else:
        print("❌ BUILD FAILED - Check errors above")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
