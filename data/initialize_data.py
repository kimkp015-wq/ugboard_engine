#!/usr/bin/env python3
"""
Initialize data files for UG Board Engine
"""
import json
from datetime import datetime
from pathlib import Path

def initialize_data_files():
    """Create initial data files"""
    data_dir = Path("data")
    
    # 1. Create songs.json with sample Ugandan songs
    sample_songs = [
        {
            "id": "song_1",
            "title": "Nalumansi",
            "artist": "Bobi Wine",
            "plays": 10000,
            "score": 95.5,
            "genre": "kadongo kamu",
            "region": "ug",
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "initial_data"
        },
        {
            "id": "song_2",
            "title": "Sitya Loss",
            "artist": "Eddy Kenzo",
            "plays": 8500,
            "score": 92.3,
            "genre": "afrobeat",
            "region": "ug",
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "initial_data"
        },
        {
            "id": "song_3",
            "title": "Bailando",
            "artist": "Sheebah Karungi",
            "plays": 9200,
            "score": 94.1,
            "genre": "dancehall",
            "region": "ug",
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "initial_data"
        }
    ]
    
    with open(data_dir / "songs.json", 'w') as f:
        json.dump(sample_songs, f, indent=2)
    print("✅ Created songs.json with sample data")
    
    # 2. Create regions.json
    regions_data = {
        "ug": {"name": "Uganda", "songs": sample_songs},
        "ke": {"name": "Kenya", "songs": []},
        "tz": {"name": "Tanzania", "songs": []},
        "rw": {"name": "Rwanda", "songs": []}
    }
    
    with open(data_dir / "regions.json", 'w') as f:
        json.dump(regions_data, f, indent=2)
    print("✅ Created regions.json with East African regions")
    
    # 3. Create chart_history.json
    chart_history = [
        {
            "week": datetime.utcnow().strftime("%Y-W%W"),
            "published_at": datetime.utcnow().isoformat(),
            "top100": sample_songs,
            "regions": {
                "ug": sample_songs[:2],
                "ke": [],
                "tz": [],
                "rw": []
            }
        }
    ]
    
    with open(data_dir / "chart_history.json", 'w') as f:
        json.dump(chart_history, f, indent=2)
    print("✅ Created chart_history.json with current week")
    
    # 4. Create .gitignore for data directory
    gitignore_content = """# Data files - do not commit to version control
*.json
*.log
*.db
*.sqlite3
backups/
exports/
"""
    
    with open(data_dir / ".gitignore", 'w') as f:
        f.write(gitignore_content)
    print("✅ Created .gitignore for data directory")
    
    print("\n📊 Data initialization complete!")
    print(f"Total songs: {len(sample_songs)}")
    print(f"Regions configured: {len(regions_data)}")

if __name__ == "__main__":
    initialize_data_files()
