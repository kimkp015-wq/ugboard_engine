"""
install_playwright.py - Install Playwright dependencies
"""

import subprocess
import sys
import os

def install_playwright():
    """Install Playwright and browser dependencies"""
    print("🔧 Installing Playwright...")
    
    try:
        # Install Playwright Python package
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "playwright==1.49.0"
        ])
        
        print("✅ Playwright Python package installed")
        
        # Install Playwright browsers
        print("🌐 Installing Playwright browsers...")
        subprocess.check_call([
            sys.executable, "-m", "playwright", "install", "chromium"
        ])
        
        print("✅ Playwright Chromium installed")
        
        # Install dependencies for headless mode
        if os.name != 'nt':  # Not Windows
            print("🔧 Installing system dependencies for headless Chrome...")
            try:
                subprocess.check_call([
                    "apt-get", "update"
                ])
                subprocess.check_call([
                    "apt-get", "install", "-y",
                    "libnss3",
                    "libxss1",
                    "libasound2",
                    "libatk-bridge2.0-0",
                    "libgtk-3-0",
                    "libgbm1"
                ])
                print("✅ System dependencies installed")
            except:
                print("⚠️  Could not install system dependencies, continuing...")
        
        print("🎉 Playwright setup complete!")
        print("\nTo verify installation, run:")
        print("  python -m playwright --version")
        
    except Exception as e:
        print(f"❌ Installation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    install_playwright()
