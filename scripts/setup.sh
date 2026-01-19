#!/bin/bash
# setup.sh - UG Board Engine dependency installation

set -e  # Exit on error

echo "🚀 Setting up UG Board Engine dependencies..."

# Update pip
echo "📦 Updating pip..."
python -m pip install --upgrade pip==23.3.1

# Install system dependencies if needed
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "🐧 Installing Linux system dependencies..."
    sudo apt-get update
    sudo apt-get install -y \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-dev \
        portaudio19-dev \
        ffmpeg
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "🍎 Installing macOS system dependencies..."
    brew install \
        portaudio \
        ffmpeg
fi

# Create virtual environment
echo "🏗️ Creating virtual environment..."
python -m venv venv
source venv/bin/activate

# Install core requirements
echo "📚 Installing core dependencies..."
pip install --no-cache-dir -r requirements.txt

# Install development dependencies if file exists
if [ -f "requirements-dev.txt" ]; then
    echo "🔧 Installing development dependencies..."
    pip install --no-cache-dir -r requirements-dev.txt
fi

# Verify installation
echo "✅ Verifying installation..."
python -c "
import fastapi, uvicorn, aiohttp, sqlalchemy, redis, librosa
print(f'FastAPI {fastapi.__version__}')
print(f'SQLAlchemy {sqlalchemy.__version__}')
print('All dependencies installed successfully!')
"

echo "🎉 Setup complete! Activate with: source venv/bin/activate"
