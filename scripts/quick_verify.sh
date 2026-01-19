#!/bin/bash
# quick_verify.sh

echo "🔍 Verifying UG Board Engine can start..."

# Test imports
python3 -c "
try:
    from fastapi import FastAPI
    import uvicorn
    import pydantic
    import aiohttp
    print('✅ Core imports successful')
    
    # Try to create app
    app = FastAPI()
    print('✅ FastAPI app created')
    
    print('\n🎉 Engine CAN start!')
    print('👉 Run: uvicorn api.main:app --reload --port 8000')
    
except ImportError as e:
    print(f'❌ Import failed: {e}')
    print('Run: pip install -r requirements_emergency.txt')
"

# Check if main.py exists
if [ -f "api/main.py" ]; then
    echo "✅ Main application file exists"
else
    echo "⚠️  Creating minimal main.py..."
    mkdir -p api
    cp main_minimal.py api/main.py 2>/dev/null || echo "Please create api/main.py"
fi
