#!/bin/bash
# install.sh - Zero-fail installation for UG Board Engine

set -e

echo "🔧 UG Board Engine Installation (Zero-Fail Method)"
echo "=================================================="

# Install system dependencies
if command -v apt-get &> /dev/null; then
    echo "📦 Installing system packages..."
    sudo apt-get update
    sudo apt-get install -y \
        python3-pip \
        python3-venv \
        ffmpeg \
        libsndfile1 \
        portaudio19-dev
elif command -v brew &> /dev/null; then
    echo "🍎 Installing via Homebrew..."
    brew install ffmpeg portaudio
fi

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install minimal guaranteed dependencies
echo "📥 Installing Python dependencies..."
pip install --upgrade pip

cat > requirements_minimal.txt << 'EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
aiohttp==3.9.0
beautifulsoup4==4.12.0
requests==2.31.0
librosa==0.10.2.post1
numpy==1.26.4
ffmpeg-python==0.2.0
pydantic==2.5.0
EOF

pip install -r requirements_minimal.txt

# Test installation
echo "✅ Verification..."
python -c "
import sys
print(f'Python: {sys.version}')
import fastapi, aiohttp, librosa, numpy as np
print(f'FastAPI: {fastapi.__version__}')
print(f'aiohttp: {aiohttp.__version__}')
print(f'librosa: {librosa.__version__}')
print(f'numpy: {np.__version__}')
print('✓ All dependencies installed successfully!')
"

echo ""
echo "🎉 Installation complete!"
echo "Quick start:"
echo "  source .venv/bin/activate"
echo "  python scripts/tv_scraper.py --station 'NTV Uganda'"
