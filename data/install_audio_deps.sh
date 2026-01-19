#!/bin/bash
# install_audio_deps.sh - Install system dependencies for librosa

echo "🔧 Installing system dependencies for audio processing..."

# Ubuntu/Debian
if command -v apt-get &> /dev/null; then
    sudo apt-get update
    sudo apt-get install -y \
        build-essential \
        python3-dev \
        libsndfile1 \
        ffmpeg \
        libavcodec-dev \
        libavformat-dev \
        libavutil-dev
fi

# macOS
if command -v brew &> /dev/null; then
    brew install \
        ffmpeg \
        libsndfile
fi

echo "✅ Audio dependencies installed for librosa!"
