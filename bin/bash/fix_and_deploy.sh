#!/bin/bash
# fix_engine_structure.sh

echo "🔧 Fixing UG Board Engine structure..."

# 1. Backup current main.py
if [ -f "main.py" ]; then
    echo "Backing up root main.py..."
    mv main.py main.py.backup.$(date +%s)
fi

# 2. Check if api/main.py exists
if [ -f "api/main.py" ]; then
    echo "Found api/main.py - using API structure"
    
    # 3. Create __init__.py if missing
    if [ ! -f "api/__init__.py" ]; then
        echo "Creating api/__init__.py..."
        echo "# UG Board Engine API Package" > api/__init__.py
    fi
    
    # 4. Check api/main.py structure
    echo "Checking api/main.py structure..."
    if ! grep -q "app = FastAPI" api/main.py; then
        echo "⚠️  api/main.py doesn't define 'app'. Creating template..."
        
        cat > api/main.py << 'EOF'
from fastapi import FastAPI
from datetime import datetime

app = FastAPI(title="UG Board Engine", version="6.0.0")

@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "status": "online",
        "version": "6.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
EOF
    fi
    
    # 5. Create minimal requirements
    cat > requirements.txt << 'EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
python-dotenv==1.0.0
EOF
    
    echo "✅ Using API structure: api/main.py"
    
else
    echo "❌ No api/main.py found. Creating root main.py..."
    
    cat > main.py << 'EOF'
from fastapi import FastAPI
from datetime import datetime

app = FastAPI(title="UG Board Engine", version="6.0.0")

@app.get("/")
async def root():
    return {
        "service": "UG Board Engine",
        "status": "online", 
        "version": "6.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
EOF
    
    echo "✅ Created root main.py"
fi

# 6. Create runtime.txt
echo "python-3.11.9" > runtime.txt

echo ""
echo "📁 Current structure:"
ls -la
echo ""
echo "📁 API directory:"
ls -la api/ 2>/dev/null || echo "No api directory"

echo ""
echo "🧪 Testing import..."
python -c "
import sys
try:
    # Try api.main first
    sys.path.insert(0, '.')
    from api.main import app
    print('✅ Successfully imported from api.main')
    print(f'   App title: {app.title}')
except ImportError:
    try:
        # Try root main
        from main import app
        print('✅ Successfully imported from main')
        print(f'   App title: {app.title}')
    except Exception as e:
        print(f'❌ Import failed: {e}')
"

echo ""
echo "📤 To deploy:"
echo "git add ."
echo "git commit -m 'Fix engine structure'"
echo "git push origin main"
echo ""
echo "🌐 Render will auto-deploy in 2-3 minutes"
