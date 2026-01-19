#!/bin/bash
# install_deps.sh - Production-grade dependency installer for UG Board Engine

set -e  # Exit on error

echo "🚀 UG Board Engine - Dependency Installation (2026)"
echo "=================================================="

# Detect OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "📦 Detected Linux environment"
    
    # Update package lists
    sudo apt-get update
    
    # Install system dependencies
    echo "🔄 Installing system dependencies..."
    sudo apt-get install -y \
        ffmpeg \
        libavcodec-extra \
        libavformat-dev \
        libavdevice-dev \
        chromaprint-tools \
        mysql-client \
        libmysqlclient-dev \
        python3-dev \
        python3-pip \
        python3-venv
    
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "🍎 Detected macOS environment"
    brew install ffmpeg chromaprint mysql-client
fi

# Create virtual environment
echo "🔧 Setting up Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip to latest (2026 standards)
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install base dependencies
echo "📥 Installing Python dependencies..."
pip install -r requirements/base.txt

# Install TV scraper dependencies
echo "📺 Installing TV scraper dependencies..."
pip install -r requirements/tv-scraper.txt

# Install development dependencies (if in dev mode)
if [ "$1" == "--dev" ]; then
    echo "💻 Installing development dependencies..."
    pip install -r requirements/dev.txt
fi

# Verify installations
echo "✅ Verification..."
python -c "
import fastapi, uvicorn, aiohttp, librosa, asyncpg
print('✓ FastAPI:', fastapi.__version__)
print('✓ aiohttp:', aiohttp.__version__)
print('✓ librosa:', librosa.__version__)
print('✓ asyncpg:', asyncpg.__version__)
"

echo "🎉 All dependencies installed successfully!"
echo ""
echo "Quick Start:"
echo "1. Activate env: source .venv/bin/activate"
echo "2. Run scraper: python scripts/tv_scraper.py --station 'NTV Uganda'"
echo "3. Run API: uvicorn main:app --reload"
