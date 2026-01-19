#!/bin/bash
# emergency_recovery.sh - Get UG Board Engine back online

set -e  # Exit on any error

echo "🚨 EMERGENCY RECOVERY - UG Board Engine"
echo "========================================"

# Backup current requirements if exists
if [ -f "requirements.txt" ]; then
    cp requirements.txt requirements.txt.backup.$(date +%Y%m%d_%H%M%S)
    echo "✅ Backed up requirements.txt"
fi

# Create minimal working requirements
cat > requirements.txt << 'EOF'
# EMERGENCY MINIMAL REQUIREMENTS - UG Board Engine
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
pydantic-settings==2.1.0
sqlalchemy==2.0.23
asyncpg==0.29.0
redis==5.0.1
aiohttp==3.9.1
aiofiles==23.2.1
pandas==2.1.4
numpy==1.26.2
orjson==3.9.10
python-dotenv==1.0.0
structlog==23.2.0
EOF

echo "✅ Created emergency requirements.txt"

# Update pip to latest
echo "🔄 Updating pip..."
python -m pip install --upgrade pip

# Install with no cache
echo "📦 Installing emergency dependencies..."
pip install --no-cache-dir -r requirements.txt

# Test core functionality
echo "🧪 Testing core imports..."
python -c "
import fastapi, uvicorn, pydantic, sqlalchemy, aiohttp, pandas, numpy
print('✅ FastAPI:', fastapi.__version__)
print('✅ SQLAlchemy:', sqlalchemy.__version__)
print('✅ Pandas:', pandas.__version__)
print('✅ Core dependencies OK')
"

# Create minimal main.py if missing
if [ ! -f "api/main.py" ]; then
    echo "⚠️  Creating minimal main.py..."
    mkdir -p api
    cat > api/main.py << 'PYTHONEOF'
from fastapi import FastAPI
import os

app = FastAPI(title="UG Board Engine - Emergency Mode")

@app.get("/")
async def root():
    return {
        "status": "online",
        "mode": "emergency_recovery",
        "message": "Engine is back online with minimal dependencies"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
PYTHONEOF
fi

echo "🎉 EMERGENCY RECOVERY COMPLETE!"
echo "👉 Run: uvicorn api.main:app --reload --port 8000"
