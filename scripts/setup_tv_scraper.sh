#!/bin/bash
# scripts/setup_tv_scraper.sh - Setup TV Scraper Environment

echo "📡 Setting up UG Board TV Scraper..."
echo "====================================="

# Create necessary directories
mkdir -p logs config data/audio_samples

# Check if config file exists
if [ ! -f "config/tv_stations.yaml" ]; then
    echo "Creating default TV stations config..."
    cat > config/tv_stations.yaml << 'EOF'
# TV Stations Configuration
version: "1.0"
stations:
  - name: "NTV Uganda"
    url: "https://ntv.metropolitan.videopulse.co/ntv/ntv.m3u8"
    region: "ug"
    language: "en"
    enabled: true
    
  - name: "NBS Television"
    url: "https://cdn-ap-aka.metropolitan.videopulse.co/NTV/NTV.m3u8"
    region: "ug"
    language: "en"
    enabled: true
EOF
    echo "✅ Created config/tv_stations.yaml"
fi

# Install dependencies
echo "Installing Python dependencies..."
pip install aiohttp pyyaml

# Test configuration
echo "Testing configuration..."
python -c "
import yaml
import os

try:
    with open('config/tv_stations.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    stations = config.get('stations', [])
    print(f'✅ Loaded {len(stations)} TV stations')
    
    # Check environment variables
    token = os.getenv('INGEST_TOKEN')
    engine_url = os.getenv('ENGINE_URL', 'https://ugboard-engine.onrender.com')
    
    if token:
        print(f'✅ INGEST_TOKEN is set')
    else:
        print('⚠️  INGEST_TOKEN not set in environment')
    
    print(f'✅ Engine URL: {engine_url}')
    
except Exception as e:
    print(f'❌ Configuration error: {e}')
"

echo ""
echo "🎉 Setup complete!"
echo ""
echo "To run the TV scraper:"
echo "1. Set your INGEST_TOKEN:"
echo "   export INGEST_TOKEN='1994199620002019866'"
echo ""
echo "2. Run single test cycle:"
echo "   python scripts/tv_scraper.py --single-cycle --verbose"
echo ""
echo "3. Run continuous scraping:"
echo "   python scripts/tv_scraper.py --interval 30"
echo ""
echo "4. Or let GitHub Actions run it automatically every 30 minutes"
