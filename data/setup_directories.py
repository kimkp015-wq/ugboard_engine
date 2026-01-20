#!/usr/bin/env python3
"""
Setup directories for UG Board Engine
"""
import os
from pathlib import Path

def setup_directories():
    """Create required directories"""
    directories = [
        "data",           # For JSON storage
        "logs",           # For log files
        "scripts",        # For TV scraper
        "config",         # For configuration files
        "data/backups",   # For backup files
        "data/exports"    # For data exports
    ]
    
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {dir_path}")
        
    # Set permissions (optional)
    os.chmod("logs", 0o755)
    os.chmod("data", 0o755)
    
    print("\n📁 Directory structure created successfully!")
    print("Structure:")
    print("ugboard_engine/")
    print("├── data/          # JSON database files")
    print("├── logs/          # Application logs")
    print("├── scripts/       # TV scraper scripts")
    print("├── config/        # Configuration files")
    print("├── main.py        # Main application")
    print("└── requirements.txt")

if __name__ == "__main__":
    setup_directories()
