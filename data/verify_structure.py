#!/usr/bin/env python3
"""
Verify project structure
"""
import json
from pathlib import Path

def verify_project_structure():
    """Verify all required files and directories exist"""
    print("🔍 Verifying UG Board Engine structure...")
    print("=" * 50)
    
    # Required directories
    required_dirs = ["data", "logs", "scripts", "config"]
    missing_dirs = []
    
    for dir_name in required_dirs:
        if Path(dir_name).exists():
            print(f"✅ Directory exists: {dir_name}/")
        else:
            print(f"❌ Missing directory: {dir_name}/")
            missing_dirs.append(dir_name)
    
    print("\n" + "=" * 50)
    
    # Required files
    required_files = ["main.py", "requirements.txt", "data/songs.json", 
                     "data/regions.json", "data/chart_history.json"]
    missing_files = []
    
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"✅ File exists: {file_path}")
            
            # Check file content for data files
            if file_path.endswith('.json'):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    print(f"   ↳ Contains: {len(data) if isinstance(data, list) else len(data.keys())} items")
                except:
                    print(f"   ↳ Warning: Could not read JSON")
        else:
            print(f"❌ Missing file: {file_path}")
            missing_files.append(file_path)
    
    print("\n" + "=" * 50)
    
    # Check scripts directory
    scripts_files = list(Path("scripts").glob("*.py"))
    if scripts_files:
        print(f"📁 Scripts directory contains {len(scripts_files)} Python files:")
        for script in scripts_files:
            print(f"   • {script.name}")
    else:
        print("⚠️  Scripts directory is empty - TV scraper may not work")
    
    print("\n" + "=" * 50)
    
    # Summary
    if not missing_dirs and not missing_files:
        print("🎉 All checks passed! Project structure is ready for deployment.")
        return True
    else:
        print("⚠️  Issues found:")
        if missing_dirs:
            print(f"   Missing directories: {', '.join(missing_dirs)}")
        if missing_files:
            print(f"   Missing files: {', '.join(missing_files)}")
        print("\nRun the setup scripts to fix these issues.")
        return False

if __name__ == "__main__":
    verify_project_structure()
