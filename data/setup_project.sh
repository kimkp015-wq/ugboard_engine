#!/bin/bash

echo "🚀 Setting up UG Board Engine Project..."
echo "========================================"

# Step 1: Create directories
echo "📁 Creating directories..."
mkdir -p data logs scripts config data/backups data/exports

# Step 2: Initialize data files
echo "📊 Initializing data files..."
python3 -c "
import json
from datetime import datetime
data = [
    {
        'id': 'song_1',
        'title': 'Nalumansi',
        'artist': 'Bobi Wine',
        'plays': 10000,
        'score': 95.5,
        'genre': 'kadongo kamu',
        'region': 'ug',
        'ingested_at': datetime.utcnow().isoformat(),
        'source': 'initial_data'
    }
]
with open('data/songs.json', 'w') as f:
    json.dump(data, f, indent=2)
print('✅ Created songs.json')
"

# Step 3: Create .gitignore for data
echo "📝 Creating .gitignore files..."
cat > data/.gitignore << 'EOF'
# Data files - do not commit to version control
*.json
*.log
*.db
*.sqlite3
backups/
exports/
EOF

cat > logs/.gitignore << 'EOF'
# Log files
*.log
*.log.*
EOF

# Step 4: Verify structure
echo "🔍 Verifying structure..."
echo "Project structure:"
find . -type d -not -path '*/\.*' | sort

echo ""
echo "Files created:"
ls -la data/*.json 2>/dev/null || echo "No data files found"

# Step 5: Set permissions
echo "🔐 Setting permissions..."
chmod 755 logs data

echo ""
echo "========================================"
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Add your TV scraper to scripts/"
echo "2. Update main.py with your API logic"
echo "3. Run: python verify_structure.py"
echo "4. Deploy to Render.com"
