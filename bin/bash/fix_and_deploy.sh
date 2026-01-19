#!/bin/bash
# fix_and_deploy.sh

echo "🚀 UG Board Engine Emergency Fix & Deploy"
echo "========================================="

# Backup current requirements
if [ -f "requirements.txt" ]; then
    cp requirements.txt "requirements.backup.$(date +%s).txt"
fi

# Create working requirements
cat > requirements.txt << 'EOF'
# UG Board Engine - Working Requirements
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
sqlalchemy==2.0.23
psycopg[binary]==3.1.18
redis==5.0.1
aiohttp==3.9.1
aiofiles==23.2.1
pandas==2.2.3
numpy==1.26.4
orjson==3.9.12
python-dotenv==1.0.0
EOF

echo "✅ Created working requirements.txt"

# Update pip
echo "🔄 Updating pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Create api directory if not exists
mkdir -p api

# Test if we can import
echo "🧪 Testing installation..."
python -c "
import fastapi, uvicorn, pydantic, sqlalchemy, pandas
print('✅ FastAPI:', fastapi.__version__)
print('✅ Pandas:', pandas.__version__)
print('✅ All core packages installed successfully')
"

echo ""
echo "🎉 FIX COMPLETE!"
echo "👉 Start the engine: uvicorn api.main:app --reload --port 8000"
