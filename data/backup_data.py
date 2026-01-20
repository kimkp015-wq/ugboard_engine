#!/usr/bin/env python3
"""
Backup script for UG Board Engine data
"""
import json
import shutil
from datetime import datetime
from pathlib import Path

def backup_data():
    """Create backup of all data files"""
    data_dir = Path("data")
    backup_dir = Path("data/backups")
    backup_dir.mkdir(exist_ok=True)
    
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Files to backup
    files_to_backup = ["songs.json", "regions.json", "chart_history.json"]
    
    print(f"📦 Creating backup: backup_{timestamp}")
    
    for filename in files_to_backup:
        source = data_dir / filename
        if source.exists():
            # Create backup copy
            backup_name = f"{filename.replace('.json', '')}_{timestamp}.json"
            backup_path = backup_dir / backup_name
            
            shutil.copy2(source, backup_path)
            print(f"  ✅ Backed up: {filename} → {backup_name}")
        else:
            print(f"  ⚠️  Skipped: {filename} (not found)")
    
    # Also backup the entire data directory as zip
    zip_backup = backup_dir / f"full_backup_{timestamp}.zip"
    shutil.make_archive(
        str(zip_backup).replace('.zip', ''),
        'zip',
        data_dir,
        '.'
    )
    
    print(f"\n✅ Backup completed: {backup_dir}/")
    print(f"📁 Total backups: {len(list(backup_dir.glob('*.json')))} JSON files")
    print(f"💾 Full backup: {zip_backup.name}")
    
    # Clean old backups (keep last 7 days)
    cleanup_old_backups(backup_dir)

def cleanup_old_backups(backup_dir, days_to_keep=7):
    """Remove backups older than specified days"""
    cutoff_time = datetime.now().timestamp() - (days_to_keep * 86400)
    
    for backup_file in backup_dir.glob("*"):
        if backup_file.stat().st_mtime < cutoff_time:
            backup_file.unlink()
            print(f"  🗑️  Cleaned up old backup: {backup_file.name}")

if __name__ == "__main__":
    backup_data()
