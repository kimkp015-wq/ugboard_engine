#!/bin/bash
# fix_render_deploy.sh

echo "🔧 Fixing Render deployment..."

# Create minimal requirements
cat > requirements.txt << 'EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
python-dotenv==1.0.0
EOF

# Create runtime.txt
echo "python-3.11.9" > runtime.txt

# Create main.py at root
cat > main.py << 'EOF'
from fastapi import FastAPI
from datetime import datetime

app = FastAPI(title="UG Board Engine", version="1.0.0")

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "UG Board Engine",
        "timestamp": datetime.utcnow().isoformat(),
        "url": "https://ugboard-engine.onrender.com"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}
EOF

echo "✅ Files created:"
echo "   - requirements.txt"
echo "   - runtime.txt"  
echo "   - main.py"
echo ""
echo "📤 Commit and push to GitHub:"
echo "git add ."
echo "git commit -m 'Fix Render deployment'"
echo "git push origin main"
echo ""
echo "🌐 Render will deploy in 2-3 minutes"
echo "Check: https://ugboard-engine.onrender.com"
