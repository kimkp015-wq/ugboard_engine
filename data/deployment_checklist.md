# UG Board Engine Deployment Checklist

## ✅ Step 1: Requirements
- [ ] `requirements.txt` updated with correct dependencies
- [ ] Python 3.11+ specified in `runtime.txt`
- [ ] All import statements verified

## ✅ Step 2: Directory Structure
- [ ] `data/` directory created
- [ ] `logs/` directory created  
- [ ] `scripts/` directory created
- [ ] `config/` directory created

## ✅ Step 3: Data Files
- [ ] `data/songs.json` initialized
- [ ] `data/regions.json` initialized
- [ ] `data/chart_history.json` initialized
- [ ] `data/.gitignore` created

## ✅ Step 4: Main Application
- [ ] `main.py` updated with new structure
- [ ] All endpoints tested locally
- [ ] Health check endpoint working

## ✅ Step 5: Environment Variables
- [ ] ADMIN_TOKEN set in Render.com
- [ ] INGEST_TOKEN set in Render.com  
- [ ] INTERNAL_TOKEN set in Render.com
- [ ] YOUTUBE_TOKEN set in Render.com
- [ ] ENV=production set

## ✅ Step 6: Git Commit
- [ ] All changes committed to Git
- [ ] `data/*.json` added to .gitignore
- [ ] `logs/*.log` added to .gitignore

## ✅ Step 7: Deployment
- [ ] Push to GitHub repository
- [ ] Trigger Render.com deployment
- [ ] Monitor deployment logs
- [ ] Verify health checks (200 OK)

## ✅ Step 8: Post-Deployment
- [ ] Test all API endpoints
- [ ] Verify Swagger documentation at /docs
- [ ] Test authentication with tokens
- [ ] Test YouTube worker integration

## Troubleshooting
- **ModuleNotFoundError**: Check requirements.txt
- **ImportError**: Verify Python path and imports
- **FileNotFoundError**: Check directory structure
- **401 Unauthorized**: Verify token configuration
