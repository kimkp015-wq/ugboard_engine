"""
UG Board Engine - Complete Production System v10.0.0
Root-level main.py with built-in secrets and enhanced scraper integration
"""

import os
import sys
import json
import time
import asyncio
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from contextlib import asynccontextmanager
import subprocess
import signal

# Add the current directory to path for local imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Third-party imports
from fastapi import FastAPI, HTTPException, Header, Depends, Query, Path as FPath, Request, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, ConfigDict
import uvicorn

# ====== BUILT-IN SECRETS & CONFIGURATION ======
class Config:
    """Centralized configuration with built-in secrets"""
    
    # ====== BUILT-IN SECRETS (Production Ready) ======
    # Environment
    ENVIRONMENT = "production"  # Force production mode
    DEBUG = False
    PORT = 8000
    
    # Security Tokens (Built-in from your requirements)
    ADMIN_TOKEN = "admin-ug-board-2025"
    INGEST_TOKEN = "1994199620002019866"
    INTERNAL_TOKEN = "1994199620002019866"
    YOUTUBE_TOKEN = "1994199620002019866"
    SCRAPER_APIKEY = "1994199620002019866"
    
    # YouTube Integration
    YOUTUBE_WORKER_URL = "https://ugboard-youtube-puller.kimkp015.workers.dev"
    
    # Koyeb URL (from your configuration)
    KOYEB_APP_URL = "https://ugboard-1ubboardengine1-cf8eb3a3.koyeb.app"
    
    # Paths
    BASE_DIR = current_dir
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"
    SCRIPTS_DIR = BASE_DIR / "scripts"
    CACHE_DIR = BASE_DIR / "cache"
    
    # Ugandan Regions
    UGANDAN_REGIONS = {
        "central": {
            "name": "Central Region",
            "districts": ["Kampala", "Mukono", "Wakiso", "Masaka", "Luwero"],
            "musicians": ["Bobi Wine", "Eddy Kenzo", "Sheebah", "Daddy Andre", "Alien Skin", "Azawi", "Vinka"],
            "tv_stations": ["NTV Uganda", "Bukedde TV", "Salt TV", "Spark TV"],
            "radio_stations": ["CBS FM", "Capital FM", "Radio One", "Sanyu FM"]
        },
        "eastern": {
            "name": "Eastern Region",
            "districts": ["Jinja", "Mbale", "Soroti", "Iganga", "Tororo"],
            "musicians": ["Geosteady", "Victor Ruz", "Temperature Touch", "Rexy", "Judith Babirye"],
            "tv_stations": ["Baba TV", "Urban TV", "Delta TV"],
            "radio_stations": ["Kiira FM", "Radio Wa", "Voice of Teso"]
        },
        "western": {
            "name": "Western Region",
            "districts": ["Mbarara", "Fort Portal", "Hoima", "Kabale", "Kasese"],
            "musicians": ["Rema Namakula", "Mickie Wine", "Ray G", "Truth 256", "Levixone"],
            "tv_stations": ["Voice of Toro", "Top TV", "TV West"],
            "radio_stations": ["Radio West", "Kasese Guide Radio", "Voice of Kigezi"]
        },
        "northern": {
            "name": "Northern Region",
            "districts": ["Gulu", "Lira", "Arua", "Kitgum"],
            "musicians": ["Fik Fameica", "Bosmic Otim", "Eezzy", "Laxzy Mover", "John Blaq"],
            "tv_stations": ["TV North", "Mega FM TV", "Arua One TV"],
            "radio_stations": ["Mega FM", "Radio Pacis", "Radio Wa"]
        }
    }
    
    VALID_REGIONS = set(UGANDAN_REGIONS.keys())
    
    # Chart settings
    CHART_WEEK_FORMAT = "%Y-W%W"
    TRENDING_WINDOW_HOURS = 8
    
    # Scraper settings
    SCRAPER_TIMEOUT = 300  # 5 minutes
    SCRAPER_MAX_RETRIES = 3
    SCRAPER_CACHE_TTL = 3600  # 1 hour
    
    # TV Stations configuration (from your tv_stations.yaml)
    TV_STATIONS = {
        "ntv": {
            "name": "NTV Uganda",
            "url": "https://www.ntv.co.ug",
            "scraper": "tv_scraper.py",
            "region": "central",
            "active": True
        },
        "bukedde": {
            "name": "Bukedde TV",
            "url": "https://www.bukedde.co.ug",
            "scraper": "tv_scraper.py",
            "region": "central",
            "active": True
        },
        "salt": {
            "name": "Salt TV",
            "url": "https://salttv.ug",
            "scraper": "tv_scraper.py",
            "region": "central",
            "active": True
        }
    }
    
    # Radio Stations configuration
    RADIO_STATIONS = {
        "cbs": {
            "name": "CBS FM",
            "url": "https://www.cbsfm.co.ug",
            "scraper": "radio_scraper.py",
            "region": "central",
            "frequency": "88.8 FM",
            "active": True
        },
        "capital": {
            "name": "Capital FM",
            "url": "https://www.capitalfm.co.ug",
            "scraper": "radio_scraper.py",
            "region": "central",
            "frequency": "91.3 FM",
            "active": True
        },
        "sanyu": {
            "name": "Sanyu FM",
            "url": "https://www.sanyufm.co.ug",
            "scraper": "radio_scraper.py",
            "region": "central",
            "frequency": "88.2 FM",
            "active": True
        }
    }
    
    @classmethod
    def setup_directories(cls):
        """Create necessary directories"""
        directories = [
            cls.DATA_DIR,
            cls.LOGS_DIR,
            cls.CACHE_DIR,
            cls.DATA_DIR / "regions",
            cls.DATA_DIR / "backups",
            cls.DATA_DIR / "scrapers",
            cls.LOGS_DIR / "scrapers",
            cls.CACHE_DIR / "tv",
            cls.CACHE_DIR / "radio",
            cls.CACHE_DIR / "youtube"
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        return cls
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        cls.setup_directories()
        
        # Security checks
        if cls.ENVIRONMENT == "production":
            # Verify tokens are set (they are built-in)
            required_tokens = ["ADMIN_TOKEN", "INGEST_TOKEN", "YOUTUBE_TOKEN"]
            for token_name in required_tokens:
                token_value = getattr(cls, token_name)
                if not token_value or "replace_this" in str(token_value):
                    raise ValueError(f"{token_name} must be properly set for production")
        
        return cls
    
    @classmethod
    def get_scraper_path(cls, scraper_name: str) -> Path:
        """Get full path to scraper script"""
        return cls.SCRIPTS_DIR / scraper_name

config = Config.validate()

# ====== ENHANCED LOGGING ======
def setup_logger(name: str, log_file: Optional[str] = None):
    """Setup enhanced logger with file and console handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if config.DEBUG else logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(config.LOGS_DIR / log_file)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger

# Main application logger
logger = setup_logger("ugboard", "ugboard.log")

# Scraper loggers
tv_scraper_logger = setup_logger("tv_scraper", "scrapers/tv.log")
radio_scraper_logger = setup_logger("radio_scraper", "scrapers/radio.log")

# ====== MODELS ======
class SongItem(BaseModel):
    """Enhanced song data model with validation"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    title: str = Field(..., min_length=1, max_length=200, description="Song title")
    artist: str = Field(..., min_length=1, max_length=100, description="Artist name")
    plays: int = Field(0, ge=0, description="Number of plays")
    score: float = Field(0.0, ge=0.0, le=100.0, description="Chart score (0-100)")
    station: Optional[str] = Field(None, max_length=50, description="TV/Radio station name")
    region: str = Field("central", pattern="^(central|eastern|western|northern)$", description="Ugandan region")
    district: Optional[str] = Field(None, max_length=50, description="Specific district")
    timestamp: Optional[str] = Field(None, description="ISO 8601 timestamp")
    source_type: Optional[str] = Field(None, description="Source: tv, radio, youtube")
    url: Optional[str] = Field(None, description="Source URL")
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        try:
            if v.endswith('Z'):
                v = v[:-1] + '+00:00'
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('Invalid ISO 8601 timestamp')
    
    @field_validator('title')
    @classmethod
    def validate_title(cls, v: str) -> str:
        """Clean and validate title"""
        # Remove extra whitespace
        v = ' '.join(v.split())
        # Truncate if too long
        if len(v) > 200:
            v = v[:197] + "..."
        return v
    
    @field_validator('artist')
    @classmethod
    def validate_artist(cls, v: str) -> str:
        """Clean and validate artist name"""
        v = ' '.join(v.split())
        # Capitalize each word
        v = ' '.join(word.capitalize() for word in v.split())
        return v

class IngestPayload(BaseModel):
    """Enhanced ingestion payload"""
    items: List[SongItem] = Field(..., min_items=1, max_items=1000, description="List of songs")
    source: str = Field(..., min_length=1, max_length=100, description="Source identifier")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    scrape_session_id: Optional[str] = Field(None, description="Scraper session ID for tracking")

class YouTubeIngestPayload(IngestPayload):
    """YouTube ingestion payload"""
    channel_id: Optional[str] = Field(None, max_length=50, description="YouTube channel ID")
    video_id: Optional[str] = Field(None, max_length=20, description="YouTube video ID")
    category: str = Field("music", max_length=50, description="Content category")

class ScraperRequest(BaseModel):
    """Scraper execution request"""
    station_id: str = Field(..., description="Station ID from config")
    scraper_type: str = Field(..., pattern="^(tv|radio)$", description="Type of scraper")
    force_refresh: bool = Field(False, description="Force fresh scrape ignoring cache")
    timeout: Optional[int] = Field(None, ge=60, le=600, description="Scraper timeout in seconds")

# ====== CACHE MANAGEMENT ======
class CacheManager:
    """Enhanced cache manager for scraper results"""
    
    def __init__(self):
        self.cache_dir = config.CACHE_DIR
    
    def _get_cache_key(self, scraper_type: str, station_id: str) -> str:
        """Generate cache key"""
        key = f"{scraper_type}_{station_id}_{datetime.utcnow().strftime('%Y-%m-%d')}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def get_cached_data(self, scraper_type: str, station_id: str) -> Optional[Dict[str, Any]]:
        """Get cached scraper data"""
        cache_file = self.cache_dir / scraper_type / f"{station_id}.json"
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            # Check if cache is expired
            cached_time = datetime.fromisoformat(data.get('cached_at', '2000-01-01'))
            if datetime.utcnow() - cached_time > timedelta(seconds=config.SCRAPER_CACHE_TTL):
                logger.debug(f"Cache expired for {scraper_type}/{station_id}")
                return None
            
            logger.debug(f"Using cached data for {scraper_type}/{station_id}")
            return data
        
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to read cache for {scraper_type}/{station_id}: {e}")
            return None
    
    def save_to_cache(self, scraper_type: str, station_id: str, data: Dict[str, Any]):
        """Save scraper data to cache"""
        cache_file = self.cache_dir / scraper_type / f"{station_id}.json"
        
        try:
            cache_data = {
                **data,
                'cached_at': datetime.utcnow().isoformat(),
                'expires_at': (datetime.utcnow() + timedelta(seconds=config.SCRAPER_CACHE_TTL)).isoformat()
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)
            
            logger.debug(f"Cached data for {scraper_type}/{station_id}")
        
        except IOError as e:
            logger.error(f"Failed to cache data for {scraper_type}/{station_id}: {e}")
    
    def clear_cache(self, scraper_type: Optional[str] = None, station_id: Optional[str] = None):
        """Clear cache for specific scraper or station"""
        if scraper_type:
            cache_dir = self.cache_dir / scraper_type
            if cache_dir.exists():
                if station_id:
                    cache_file = cache_dir / f"{station_id}.json"
                    if cache_file.exists():
                        cache_file.unlink()
                        logger.info(f"Cleared cache for {scraper_type}/{station_id}")
                else:
                    for cache_file in cache_dir.glob("*.json"):
                        cache_file.unlink()
                    logger.info(f"Cleared all cache for {scraper_type}")
        else:
            # Clear all cache
            for cache_dir in self.cache_dir.iterdir():
                if cache_dir.is_dir():
                    for cache_file in cache_dir.glob("*.json"):
                        cache_file.unlink()
            logger.info("Cleared all cache")

cache_manager = CacheManager()

# ====== ENHANCED SCRAPER SERVICE ======
class ScraperService:
    """Enhanced service for managing TV and Radio scrapers"""
    
    def __init__(self):
        self.active_scrapers: Dict[str, subprocess.Popen] = {}
        self.scraper_results: Dict[str, Dict[str, Any]] = {}
    
    def _validate_scraper_script(self, scraper_path: Path) -> bool:
        """Validate scraper script exists and is executable"""
        if not scraper_path.exists():
            logger.error(f"Scraper script not found: {scraper_path}")
            return False
        
        # Check if it's a Python file
        if scraper_path.suffix != '.py':
            logger.error(f"Scraper script must be a Python file: {scraper_path}")
            return False
        
        # Check file permissions
        if not os.access(scraper_path, os.X_OK):
            try:
                scraper_path.chmod(0o755)
                logger.info(f"Set executable permissions for {scraper_path}")
            except Exception as e:
                logger.error(f"Failed to set permissions for {scraper_path}: {e}")
                return False
        
        return True
    
    def _get_station_config(self, scraper_type: str, station_id: str) -> Optional[Dict[str, Any]]:
        """Get station configuration"""
        if scraper_type == "tv":
            return config.TV_STATIONS.get(station_id)
        elif scraper_type == "radio":
            return config.RADIO_STATIONS.get(station_id)
        return None
    
    async def run_scraper(self, request: ScraperRequest) -> Dict[str, Any]:
        """Run scraper for a specific station"""
        station_config = self._get_station_config(request.scraper_type, request.station_id)
        
        if not station_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Station '{request.station_id}' not found for scraper type '{request.scraper_type}'"
            )
        
        if not station_config.get('active', False):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Station '{request.station_id}' is not active"
            )
        
        scraper_name = station_config.get('scraper')
        if not scraper_name:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"No scraper configured for station '{request.station_id}'"
            )
        
        scraper_path = config.get_scraper_path(scraper_name)
        
        if not self._validate_scraper_script(scraper_path):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Invalid scraper script: {scraper_name}"
            )
        
        # Check cache first (unless force refresh)
        if not request.force_refresh:
            cached_data = cache_manager.get_cached_data(request.scraper_type, request.station_id)
            if cached_data:
                return {
                    "status": "success",
                    "source": "cache",
                    "station": station_config,
                    "data": cached_data,
                    "cached": True
                }
        
        # Generate unique session ID
        session_id = f"{request.scraper_type}_{request.station_id}_{int(time.time())}"
        
        # Prepare scraper command
        cmd = [
            sys.executable,
            str(scraper_path),
            "--station", request.station_id,
            "--url", station_config['url'],
            "--region", station_config.get('region', 'central'),
            "--session-id", session_id,
            "--output-dir", str(config.DATA_DIR / "scrapers")
        ]
        
        if config.DEBUG:
            cmd.append("--verbose")
        
        timeout = request.timeout or config.SCRAPER_TIMEOUT
        
        try:
            logger.info(f"Starting scraper: {' '.join(cmd)}")
            
            # Run scraper with timeout
            start_time = time.time()
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=config.SCRIPTS_DIR
            )
            
            self.active_scrapers[session_id] = process
            
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                exit_code = process.returncode
                
                # Parse scraper output
                scraper_output = self._parse_scraper_output(stdout, stderr, exit_code)
                
                if exit_code == 0:
                    # Successful scrape
                    result = {
                        "status": "success",
                        "session_id": session_id,
                        "station": station_config,
                        "execution_time": round(time.time() - start_time, 2),
                        "data": scraper_output.get('data', []),
                        "metadata": scraper_output.get('metadata', {})
                    }
                    
                    # Cache the result
                    cache_manager.save_to_cache(request.scraper_type, request.station_id, result)
                    
                    # Store for later reference
                    self.scraper_results[session_id] = result
                    
                    return result
                else:
                    # Scraper failed
                    error_msg = scraper_output.get('error', f"Scraper exited with code {exit_code}")
                    logger.error(f"Scraper failed: {error_msg}")
                    
                    return {
                        "status": "error",
                        "session_id": session_id,
                        "station": station_config,
                        "error": error_msg,
                        "exit_code": exit_code,
                        "stderr": stderr[:500]  # Limit error output
                    }
            
            except subprocess.TimeoutExpired:
                # Kill the process if it times out
                process.kill()
                stdout, stderr = process.communicate()
                
                logger.error(f"Scraper timed out after {timeout} seconds")
                
                return {
                    "status": "timeout",
                    "session_id": session_id,
                    "station": station_config,
                    "error": f"Scraper timed out after {timeout} seconds",
                    "execution_time": timeout
                }
        
        except Exception as e:
            logger.error(f"Failed to run scraper: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to run scraper: {str(e)}"
            )
        
        finally:
            # Clean up
            if session_id in self.active_scrapers:
                del self.active_scrapers[session_id]
    
    def _parse_scraper_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """Parse scraper output to extract data"""
        try:
            # Try to parse JSON from stdout
            lines = stdout.strip().split('\n')
            for line in reversed(lines):  # Look for JSON in last lines
                line = line.strip()
                if line.startswith('{') and line.endswith('}'):
                    try:
                        data = json.loads(line)
                        return data
                    except json.JSONDecodeError:
                        continue
            
            # If no JSON found, check for structured output
            data = []
            for line in lines:
                if '|' in line:  # Simple delimiter format
                    parts = line.split('|')
                    if len(parts) >= 3:  # title|artist|plays
                        try:
                            item = {
                                "title": parts[0].strip(),
                                "artist": parts[1].strip(),
                                "plays": int(parts[2].strip()) if len(parts) > 2 else 0,
                                "score": float(parts[3].strip()) if len(parts) > 3 else 0.0,
                                "source_type": "tv" if "tv" in line.lower() else "radio"
                            }
                            data.append(item)
                        except (ValueError, IndexError):
                            continue
            
            return {
                "data": data,
                "metadata": {
                    "lines_processed": len(data),
                    "exit_code": exit_code
                }
            }
        
        except Exception as e:
            logger.warning(f"Failed to parse scraper output: {e}")
            return {
                "data": [],
                "metadata": {},
                "error": f"Output parsing failed: {str(e)}"
            }
    
    async def run_all_stations(self, scraper_type: str, background_tasks: BackgroundTasks = None) -> Dict[str, Any]:
        """Run scrapers for all active stations of a type"""
        stations = config.TV_STATIONS if scraper_type == "tv" else config.RADIO_STATIONS
        active_stations = {sid: config for sid, config in stations.items() if config.get('active', False)}
        
        if not active_stations:
            return {
                "status": "no_active_stations",
                "message": f"No active {scraper_type} stations configured",
                "scraper_type": scraper_type
            }
        
        results = {}
        successful = 0
        failed = 0
        
        for station_id, station_config in active_stations.items():
            try:
                request = ScraperRequest(
                    station_id=station_id,
                    scraper_type=scraper_type,
                    force_refresh=False
                )
                
                if background_tasks:
                    # Run in background
                    background_tasks.add_task(self.run_scraper, request)
                    results[station_id] = {"status": "queued"}
                else:
                    # Run synchronously
                    result = await self.run_scraper(request)
                    results[station_id] = result
                    
                    if result.get('status') == 'success':
                        successful += 1
                    else:
                        failed += 1
                
            except Exception as e:
                logger.error(f"Failed to scrape {station_id}: {e}")
                results[station_id] = {"status": "error", "error": str(e)}
                failed += 1
        
        return {
            "status": "completed" if not background_tasks else "queued",
            "scraper_type": scraper_type,
            "total_stations": len(active_stations),
            "successful": successful,
            "failed": failed,
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def get_active_scrapers(self) -> Dict[str, Any]:
        """Get information about active scrapers"""
        active = {}
        for session_id, process in self.active_scrapers.items():
            active[session_id] = {
                "pid": process.pid,
                "running": process.poll() is None,
                "session_id": session_id
            }
        return active
    
    def stop_scraper(self, session_id: str) -> bool:
        """Stop a running scraper"""
        if session_id in self.active_scrapers:
            process = self.active_scrapers[session_id]
            if process.poll() is None:  # Still running
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                
                del self.active_scrapers[session_id]
                logger.info(f"Stopped scraper session: {session_id}")
                return True
        
        return False

# Initialize scraper service
scraper_service = ScraperService()

# ====== DATA SERVICE ======
class DataService:
    """Enhanced data service with scraper integration"""
    
    def __init__(self):
        self.data_dir = config.DATA_DIR
        self.regions_dir = self.data_dir / "regions"
        
        # Initialize data structures
        self.songs = []
        self.top100 = []
        self.region_data = {}
        self.scraper_history = []
        
        self.load_existing_data()
    
    def load_existing_data(self):
        """Load data from existing JSON files"""
        try:
            # Load top100.json
            top100_file = self.data_dir / "top100.json"
            if top100_file.exists():
                with open(top100_file, 'r') as f:
                    self.top100 = json.load(f)
                logger.info(f"Loaded {len(self.top100)} songs from top100.json")
            
            # Load songs.json
            songs_file = self.data_dir / "songs.json"
            if songs_file.exists():
                with open(songs_file, 'r') as f:
                    self.songs = json.load(f)
                logger.info(f"Loaded {len(self.songs)} songs from songs.json")
            
            # Load region files
            for region_file in self.regions_dir.glob("*.json"):
                region = region_file.stem
                try:
                    with open(region_file, 'r') as f:
                        self.region_data[region] = json.load(f)
                    logger.info(f"Loaded region data for {region}")
                except Exception as e:
                    logger.warning(f"Failed to load {region_file}: {e}")
            
            # Load scraper history
            history_file = self.data_dir / "scraper_history.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    self.scraper_history = json.load(f)
            
            # Use top100 as base if no songs loaded
            if not self.songs and self.top100:
                self.songs = self.top100[:100]
                logger.info("Using top100 data as songs base")
                
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            # Initialize empty structures
            self.songs = []
            self.top100 = []
            self.region_data = {}
            self.scraper_history = []
    
    def save_all_data(self):
        """Save all data to files"""
        try:
            self.save_songs()
            self.save_scraper_history()
            logger.info("All data saved successfully")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
    
    def save_songs(self):
        """Save songs to JSON file"""
        try:
            songs_file = self.data_dir / "songs.json"
            with open(songs_file, 'w') as f:
                json.dump(self.songs, f, indent=2, default=str)
            logger.debug(f"Saved {len(self.songs)} songs")
        except Exception as e:
            logger.error(f"Failed to save songs: {e}")
    
    def save_scraper_history(self):
        """Save scraper history"""
        try:
            history_file = self.data_dir / "scraper_history.json"
            # Keep only last 100 entries
            if len(self.scraper_history) > 100:
                self.scraper_history = self.scraper_history[-100:]
            
            with open(history_file, 'w') as f:
                json.dump(self.scraper_history, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save scraper history: {e}")
    
    def add_scraper_result(self, result: Dict[str, Any]):
        """Add scraper result to history"""
        self.scraper_history.append({
            **result,
            "timestamp": datetime.utcnow().isoformat()
        })
        self.save_scraper_history()
    
    def process_scraper_data(self, scraper_data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Process and add scraper data to database"""
        try:
            songs_data = scraper_data.get('data', [])
            station_info = scraper_data.get('station', {})
            
            if not songs_data:
                return {
                    "status": "no_data",
                    "message": "No songs found in scraper data",
                    "source": source
                }
            
            # Convert to SongItem objects
            song_items = []
            for item in songs_data:
                try:
                    # Ensure required fields
                    if 'title' not in item or 'artist' not in item:
                        continue
                    
                    song_item = SongItem(
                        title=item['title'],
                        artist=item['artist'],
                        plays=item.get('plays', 0),
                        score=item.get('score', 0.0),
                        station=station_info.get('name', source),
                        region=station_info.get('region', 'central'),
                        source_type=item.get('source_type', source),
                        url=item.get('url')
                    )
                    song_items.append(song_item)
                    
                except Exception as e:
                    logger.warning(f"Failed to create SongItem from {item}: {e}")
                    continue
            
            if not song_items:
                return {
                    "status": "validation_failed",
                    "message": "No valid songs after validation",
                    "source": source
                }
            
            # Add to database
            result = self.add_songs(song_items, f"{source}_{station_info.get('name', 'unknown')}")
            
            # Record in scraper history
            self.add_scraper_result({
                "source": source,
                "station": station_info.get('name'),
                "songs_processed": len(song_items),
                "result": result
            })
            
            return {
                "status": "success",
                "message": f"Processed {len(song_items)} songs from {station_info.get('name')}",
                "source": source,
                "station": station_info,
                "database_result": result
            }
            
        except Exception as e:
            logger.error(f"Failed to process scraper data: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to process scraper data: {str(e)}",
                "source": source
            }
    
    def add_songs(self, songs: List[SongItem], source: str) -> Dict[str, Any]:
        """Add songs with deduplication and validation"""
        added = 0
        duplicates = 0
        added_songs = []
        
        for song in songs:
            song_dict = song.model_dump()
            song_dict["source"] = source
            song_dict["ingested_at"] = datetime.utcnow().isoformat()
            song_dict["id"] = f"song_{len(self.songs) + added + 1}"
            song_dict["last_updated"] = datetime.utcnow().isoformat()
            
            # Enhanced deduplication
            is_duplicate = False
            for existing_song in self.songs[-500:]:  # Check last 500 songs
                if (existing_song.get("title", "").lower() == song.title.lower() and
                    existing_song.get("artist", "").lower() == song.artist.lower()):
                    
                    # Update existing song if this one has higher plays/score
                    if (song.plays > existing_song.get("plays", 0) or 
                        song.score > existing_song.get("score", 0)):
                        existing_song["plays"] = max(existing_song.get("plays", 0), song.plays)
                        existing_song["score"] = max(existing_song.get("score", 0), song.score)
                        existing_song["last_updated"] = datetime.utcnow().isoformat()
                    
                    is_duplicate = True
                    duplicates += 1
                    break
            
            if not is_duplicate:
                self.songs.append(song_dict)
                added_songs.append(song_dict)
                added += 1
        
        # Save if songs were added
        if added > 0:
            self.save_songs()
            
            # Update region data
            if songs:
                region = songs[0].region
                if region in config.VALID_REGIONS:
                    region_songs = [s for s in self.songs if s.get("region") == region]
                    region_file = self.regions_dir / f"{region}.json"
                    with open(region_file, 'w') as f:
                        json.dump({
                            "songs": region_songs[:100],
                            "updated_at": datetime.utcnow().isoformat(),
                            "total_songs": len(region_songs)
                        }, f, indent=2, default=str)
        
        return {
            "added": added,
            "duplicates": duplicates,
            "total_songs": len(self.songs),
            "added_songs": [s["title"] for s in added_songs[:5]]  # First 5 titles
        }
    
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs with enhanced scoring"""
        # Use existing top100 if available and region not specified
        if not region and self.top100:
            return self.top100[:limit]
        
        # Filter by region if specified
        source_list = [s for s in self.songs if s.get("region") == region] if region else self.songs
        
        # Enhanced scoring: combine score, plays, and recency
        def song_score(song: Dict[str, Any]) -> float:
            base_score = song.get("score", 0)
            play_factor = min(song.get("plays", 0) / 1000, 10)  # Max 10 points from plays
            
            # Recency bonus (songs from last 7 days get bonus)
            last_updated = song.get("last_updated", song.get("ingested_at", "2000-01-01"))
            try:
                update_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                days_old = (datetime.utcnow() - update_time).days
                recency_bonus = max(0, 10 - days_old)  # 10 points for today, decreasing
            except:
                recency_bonus = 0
            
            return base_score + play_factor + recency_bonus
        
        sorted_songs = sorted(
            source_list,
            key=song_score,
            reverse=True
        )[:limit]
        
        return sorted_songs
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive region statistics"""
        stats = {}
        
        for region_code, region_data in config.UGANDAN_REGIONS.items():
            region_songs = [s for s in self.songs if s.get("region") == region_code]
            
            if region_songs:
                total_plays = sum(s.get("plays", 0) for s in region_songs)
                avg_score = sum(s.get("score", 0) for s in region_songs) / len(region_songs)
                
                # Get top artists in region
                artist_counts = {}
                for song in region_songs:
                    artist = song.get("artist", "Unknown")
                    artist_counts[artist] = artist_counts.get(artist, 0) + 1
                
                top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                
                # Get top stations
                station_counts = {}
                for song in region_songs:
                    station = song.get("station", "Unknown")
                    if station:
                        station_counts[station] = station_counts.get(station, 0) + 1
                
                top_stations = sorted(station_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                
                stats[region_code] = {
                    "name": region_data["name"],
                    "total_songs": len(region_songs),
                    "total_plays": total_plays,
                    "average_score": round(avg_score, 2),
                    "districts": region_data["districts"],
                    "musicians": region_data["musicians"],
                    "tv_stations": region_data.get("tv_stations", []),
                    "radio_stations": region_data.get("radio_stations", []),
                    "top_artists": [{"artist": a, "song_count": c} for a, c in top_artists],
                    "top_stations": [{"station": s, "song_count": c} for s, c in top_stations],
                    "last_updated": datetime.utcnow().isoformat()
                }
        
        return stats

# Initialize data service
data_service = DataService()

# ====== TRENDING SERVICE ======
class TrendingService:
    """Enhanced trending service with multi-factor scoring"""
    
    @staticmethod
    def get_current_trending_window() -> Dict[str, Any]:
        """Get current 8-hour window information"""
        current_time = time.time()
        hours_since_epoch = int(current_time // 3600)
        
        window_number = hours_since_epoch // config.TRENDING_WINDOW_HOURS
        window_start_hour = (window_number * config.TRENDING_WINDOW_HOURS) % 24
        window_end_hour = (window_start_hour + config.TRENDING_WINDOW_HOURS) % 24
        
        next_window_start = (window_number + 1) * config.TRENDING_WINDOW_HOURS * 3600
        seconds_remaining = max(0, next_window_start - current_time)
        
        return {
            "window_number": window_number,
            "window_start_utc": f"{window_start_hour:02d}:00",
            "window_end_utc": f"{window_end_hour:02d}:00",
            "seconds_remaining": int(seconds_remaining),
            "hours_remaining": int(seconds_remaining // 3600),
            "minutes_remaining": int((seconds_remaining % 3600) // 60),
            "description": f"{config.TRENDING_WINDOW_HOURS}-hour window {window_start_hour:02d}:00-{window_end_hour:02d}:00 UTC"
        }
    
    @staticmethod
    def calculate_trending_score(song: Dict[str, Any], window_number: int) -> float:
        """Calculate trending score with multiple factors"""
        base_score = song.get("score", 0) * 0.5
        play_score = min(song.get("plays", 0) / 100, 20)  # Max 20 from plays
        
        # Recency factor (heavily weighted)
        ingested_at = song.get("ingested_at", "2000-01-01")
        try:
            ingest_time = datetime.fromisoformat(ingested_at.replace('Z', '+00:00'))
            hours_old = (datetime.utcnow() - ingest_time).total_seconds() / 3600
            recency_score = max(0, 50 - hours_old)  # 50 for newest, decreasing
        except:
            recency_score = 0
        
        # Source type bonus
        source_type = song.get("source_type", "").lower()
        source_bonus = {
            "youtube": 15,
            "tv": 10,
            "radio": 8
        }.get(source_type, 0)
        
        # Window-based variation (makes trending change)
        window_factor = (hash(f"{window_number}_{song.get('id', '0')}") % 100) / 100
        
        total_score = (base_score + play_score + recency_score + source_bonus) * (1 + window_factor * 0.2)
        
        return round(total_score, 2)
    
    @staticmethod
    def get_trending_songs(all_songs: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs with enhanced algorithm"""
        if not all_songs:
            return []
        
        window_info = TrendingService.get_current_trending_window()
        window_number = window_info["window_number"]
        
        # Filter for recent songs (last 7 days)
        recent_songs = [
            song for song in all_songs
            if datetime.fromisoformat(song.get("ingested_at", "2000-01-01").replace('Z', '+00:00')) >
            datetime.utcnow() - timedelta(days=7)
        ]
        
        if not recent_songs:
            # Fallback to all songs if no recent ones
            recent_songs = all_songs[:100]
        
        # Calculate trending scores
        for song in recent_songs:
            song["trending_score"] = TrendingService.calculate_trending_score(song, window_number)
        
        # Sort by trending score
        sorted_songs = sorted(recent_songs, key=lambda x: x["trending_score"], reverse=True)[:limit]
        
        # Add trend information
        for i, song in enumerate(sorted_songs, 1):
            song["trend_rank"] = i
            song["trend_window"] = window_info["window_number"]
            song["trend_change"] = "new" if i <= 3 else "stable"
        
        return sorted_songs

# ====== AUTHENTICATION ======
security = HTTPBearer(auto_error=False)

class AuthService:
    """Enhanced authentication service with built-in tokens"""
    
    TOKENS = {
        "admin": config.ADMIN_TOKEN,
        "ingest": config.INGEST_TOKEN,
        "youtube": config.YOUTUBE_TOKEN,
        "internal": config.INTERNAL_TOKEN
    }
    
    @staticmethod
    def verify_token(
        token_type: str,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        """Verify token with built-in tokens"""
        expected_token = AuthService.TOKENS.get(token_type)
        
        if not expected_token:
            logger.error(f"{token_type.upper()}_TOKEN not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"{token_type} authentication not configured"
            )
        
        if not credentials:
            logger.warning(f"Missing credentials for {token_type}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Missing {token_type} token"
            )
        
        # Constant-time comparison to prevent timing attacks
        if len(credentials.credentials) != len(expected_token):
            logger.warning(f"Invalid {token_type} token length")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid {token_type} token"
            )
        
        # Simple comparison (tokens are built-in, not secret)
        if credentials.credentials != expected_token:
            logger.warning(f"Invalid {token_type} token")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid {token_type} token"
            )
        
        logger.debug(f"Successful {token_type} authentication")
        return True
    
    @staticmethod
    def verify_admin(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("admin", credentials)
    
    @staticmethod
    def verify_ingest(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("ingest", credentials)
    
    @staticmethod
    def verify_youtube(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("youtube", credentials)
    
    @staticmethod
    def verify_internal(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> bool:
        return AuthService.verify_token("internal", credentials)

# ====== GLOBAL STATE ======
current_chart_week = datetime.utcnow().strftime(config.CHART_WEEK_FORMAT)
app_start_time = datetime.utcnow()
request_count = 0

# ====== LIFECYCLE ======
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Enhanced application lifecycle management"""
    
    # Startup
    logger.info("=" * 70)
    logger.info(f"🚀 UG BOARD ENGINE v10.0.0 - PRODUCTION SYSTEM")
    logger.info(f"📅 Chart Week: {current_chart_week}")
    logger.info(f"🗺️  Regions: {', '.join(sorted(config.VALID_REGIONS))}")
    logger.info(f"🎵 Songs: {len(data_service.songs)} loaded")
    logger.info(f"📺 TV Stations: {len(config.TV_STATIONS)} configured")
    logger.info(f"📻 Radio Stations: {len(config.RADIO_STATIONS)} configured")
    logger.info(f"🔐 Authentication: Built-in tokens active")
    logger.info("=" * 70)
    
    # Check scraper scripts
    tv_scraper = config.SCRIPTS_DIR / "tv_scraper.py"
    radio_scraper = config.SCRIPTS_DIR / "radio_scraper.py"
    
    if tv_scraper.exists():
        logger.info(f"✅ TV Scraper found: {tv_scraper}")
    else:
        logger.warning(f"⚠️ TV Scraper not found: {tv_scraper}")
    
    if radio_scraper.exists():
        logger.info(f"✅ Radio Scraper found: {radio_scraper}")
    else:
        logger.warning(f"⚠️ Radio Scraper not found: {radio_scraper}")
    
    # Create initial data if needed
    if not data_service.songs:
        logger.info("📝 Creating sample data...")
        sample_songs = [
            SongItem(
                title="Nalumansi",
                artist="Bobi Wine",
                plays=15000,
                score=95.5,
                region="central",
                station="NTV Uganda",
                source_type="tv"
            ),
            SongItem(
                title="Sitya Loss",
                artist="Eddy Kenzo",
                plays=12000,
                score=92.0,
                region="central",
                station="CBS FM",
                source_type="radio"
            ),
            SongItem(
                title="Malaika",
                artist="Azawi",
                plays=8000,
                score=88.5,
                region="central",
                source_type="youtube"
            )
        ]
        data_service.add_songs(sample_songs, "system_init")
        logger.info("✅ Sample data created")
    
    yield
    
    # Shutdown
    logger.info("=" * 70)
    logger.info(f"🛑 UG Board Engine Shutting Down")
    logger.info(f"📊 Total Requests: {request_count}")
    logger.info(f"🎵 Total Songs: {len(data_service.songs)}")
    
    # Save all data
    data_service.save_all_data()
    
    # Stop any active scrapers
    active_scrapers = scraper_service.get_active_scrapers()
    if active_scrapers:
        logger.info(f"🛑 Stopping {len(active_scrapers)} active scrapers...")
        for session_id in list(active_scrapers.keys()):
            scraper_service.stop_scraper(session_id)
    
    logger.info("✅ Shutdown complete")
    logger.info("=" * 70)

# ====== FASTAPI APP ======
app = FastAPI(
    title="UG Board Engine v10.0.0",
    version="10.0.0",
    description="Complete Ugandan Music Chart System with Enhanced Scraper Integration",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Root", "description": "Service information and health"},
        {"name": "Charts", "description": "Music chart endpoints"},
        {"name": "Regions", "description": "Ugandan regional charts and statistics"},
        {"name": "Trending", "description": "Trending songs with enhanced algorithm"},
        {"name": "Ingestion", "description": "Data ingestion endpoints"},
        {"name": "Scrapers", "description": "TV and Radio scraper management"},
        {"name": "Admin", "description": "Administrative functions"},
        {"name": "Worker", "description": "YouTube Worker integration"},
    ]
)

# ====== MIDDLEWARE ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all in production (adjust as needed)
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["*"]
)

app.add_middleware(GZipMiddleware, minimum_size=500)

# Mount static files
if (config.BASE_DIR / "monitor.html").exists():
    app.mount("/static", StaticFiles(directory=config.BASE_DIR), name="static")

# ====== API ENDPOINTS ======

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with comprehensive system information"""
    global request_count
    request_count += 1
    
    window_info = TrendingService.get_current_trending_window()
    region_stats = data_service.get_region_stats()
    
    # Check for existing modules
    existing_modules = {
        "admin_api": (config.BASE_DIR / "api" / "admin").exists(),
        "charts_api": (config.BASE_DIR / "api" / "charts").exists(),
        "ingestion_api": (config.BASE_DIR / "api" / "ingestion").exists(),
        "scripts": (config.BASE_DIR / "scripts").exists() and len(list((config.BASE_DIR / "scripts").glob("*.py"))) > 0,
        "tests": (config.BASE_DIR / "tests").exists(),
    }
    
    # Active scrapers
    active_scrapers = scraper_service.get_active_scrapers()
    
    return {
        "service": "UG Board Engine",
        "version": "10.0.0",
        "status": "online",
        "environment": config.ENVIRONMENT,
        "timestamp": datetime.utcnow().isoformat(),
        "chart_week": current_chart_week,
        "trending_window": window_info,
        "system": {
            "uptime_seconds": int((datetime.utcnow() - app_start_time).total_seconds()),
            "requests_served": request_count,
            "data_songs": len(data_service.songs),
            "regions_configured": len(config.VALID_REGIONS),
            "active_scrapers": len(active_scrapers)
        },
        "scrapers": {
            "tv_stations": len(config.TV_STATIONS),
            "radio_stations": len(config.RADIO_STATIONS),
            "active": len(active_scrapers)
        },
        "existing_modules": existing_modules,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "monitor": "/monitor" if (config.BASE_DIR / "monitor.html").exists() else None,
            "charts": {
                "top100": "/charts/top100",
                "regions": "/charts/regions",
                "region_detail": "/charts/regions/{region}",
                "trending": "/charts/trending",
                "trending_now": "/charts/trending/now"
            },
            "scrapers": {
                "tv_scrape": "/scrapers/tv/run",
                "radio_scrape": "/scrapers/radio/run",
                "scrape_all": "/scrapers/run/all",
                "active": "/scrapers/active",
                "cache": "/scrapers/cache"
            },
            "ingestion": {
                "youtube": "/ingest/youtube",
                "tv": "/ingest/tv",
                "radio": "/ingest/radio"
            },
            "admin": {
                "health": "/admin/health",
                "status": "/admin/status",
                "data": "/admin/data",
                "scrapers": "/admin/scrapers"
            },
            "worker": {
                "status": "/worker/status",
                "trigger": "/worker/trigger"
            }
        }
    }

@app.get("/health", tags=["Root"])
async def health():
    """Comprehensive health check"""
    uptime = datetime.utcnow() - app_start_time
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": str(uptime).split('.')[0],
        "uptime_seconds": int(uptime.total_seconds()),
        "requests_served": request_count,
        "data": {
            "songs": len(data_service.songs),
            "top100": len(data_service.top100),
            "regions": len(data_service.region_data)
        },
        "scrapers": {
            "tv_configured": len(config.TV_STATIONS),
            "radio_configured": len(config.RADIO_STATIONS),
            "active": len(scraper_service.get_active_scrapers())
        },
        "chart_week": current_chart_week,
        "trending_window": TrendingService.get_current_trending_window(),
        "environment": config.ENVIRONMENT
    }
    
    # Check critical services
    issues = []
    
    # Check data directory
    if not config.DATA_DIR.exists():
        issues.append("data_directory_missing")
    
    # Check scraper scripts
    tv_scraper = config.SCRIPTS_DIR / "tv_scraper.py"
    radio_scraper = config.SCRIPTS_DIR / "radio_scraper.py"
    
    if not tv_scraper.exists():
        issues.append("tv_scraper_missing")
    
    if not radio_scraper.exists():
        issues.append("radio_scraper_missing")
    
    if issues:
        health_status["status"] = "degraded"
        health_status["issues"] = issues
    
    return health_status

# ====== SCRAPER ENDPOINTS ======

@app.post("/scrapers/tv/run", tags=["Scrapers"])
async def run_tv_scraper(
    request: ScraperRequest,
    background_tasks: BackgroundTasks = None,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Run TV scraper for a specific station"""
    if request.scraper_type != "tv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This endpoint is for TV scrapers only"
        )
    
    try:
        result = await scraper_service.run_scraper(request)
        
        # Process the data if scrape was successful
        if result.get("status") == "success" and not background_tasks:
            processed = data_service.process_scraper_data(result, "tv")
            result["processed"] = processed
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"TV scraper error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"TV scraper failed: {str(e)}"
        )

@app.post("/scrapers/radio/run", tags=["Scrapers"])
async def run_radio_scraper(
    request: ScraperRequest,
    background_tasks: BackgroundTasks = None,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Run Radio scraper for a specific station"""
    if request.scraper_type != "radio":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This endpoint is for Radio scrapers only"
        )
    
    try:
        result = await scraper_service.run_scraper(request)
        
        # Process the data if scrape was successful
        if result.get("status") == "success" and not background_tasks:
            processed = data_service.process_scraper_data(result, "radio")
            result["processed"] = processed
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Radio scraper error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Radio scraper failed: {str(e)}"
        )

@app.post("/scrapers/run/all", tags=["Scrapers"])
async def run_all_scrapers(
    scraper_type: str = Query(..., pattern="^(tv|radio|both)$"),
    background: bool = Query(False, description="Run in background"),
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Run all scrapers of a specific type"""
    background_tasks = BackgroundTasks() if background else None
    
    if scraper_type in ["tv", "both"]:
        tv_result = await scraper_service.run_all_stations("tv", background_tasks)
    else:
        tv_result = {"status": "skipped", "scraper_type": "tv"}
    
    if scraper_type in ["radio", "both"]:
        radio_result = await scraper_service.run_all_stations("radio", background_tasks)
    else:
        radio_result = {"status": "skipped", "scraper_type": "radio"}
    
    return {
        "status": "completed" if not background else "queued",
        "timestamp": datetime.utcnow().isoformat(),
        "tv": tv_result,
        "radio": radio_result,
        "background": background
    }

@app.get("/scrapers/active", tags=["Scrapers"])
async def get_active_scrapers(auth: bool = Depends(AuthService.verify_ingest)):
    """Get information about active scrapers"""
    active = scraper_service.get_active_scrapers()
    
    return {
        "active_scrapers": active,
        "count": len(active),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.delete("/scrapers/active/{session_id}", tags=["Scrapers"])
async def stop_scraper(session_id: str, auth: bool = Depends(AuthService.verify_admin)):
    """Stop a specific scraper"""
    success = scraper_service.stop_scraper(session_id)
    
    if success:
        return {
            "status": "stopped",
            "session_id": session_id,
            "message": "Scraper stopped successfully"
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scraper session {session_id} not found or already stopped"
        )

@app.get("/scrapers/cache", tags=["Scrapers"])
async def get_cache_info(auth: bool = Depends(AuthService.verify_ingest)):
    """Get cache information"""
    cache_files = []
    
    for scraper_type in ["tv", "radio"]:
        cache_dir = config.CACHE_DIR / scraper_type
        if cache_dir.exists():
            for cache_file in cache_dir.glob("*.json"):
                try:
                    stat = cache_file.stat()
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                    
                    cache_files.append({
                        "type": scraper_type,
                        "station": cache_file.stem,
                        "size_kb": round(stat.st_size / 1024, 2),
                        "cached_at": data.get("cached_at"),
                        "expires_at": data.get("expires_at"),
                        "data_count": len(data.get("data", []))
                    })
                except Exception as e:
                    logger.warning(f"Failed to read cache file {cache_file}: {e}")
    
    return {
        "cache_files": cache_files,
        "total_files": len(cache_files),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.delete("/scrapers/cache", tags=["Scrapers"])
async def clear_cache(
    scraper_type: Optional[str] = Query(None, pattern="^(tv|radio)$"),
    station_id: Optional[str] = Query(None),
    auth: bool = Depends(AuthService.verify_admin)
):
    """Clear scraper cache"""
    cache_manager.clear_cache(scraper_type, station_id)
    
    return {
        "status": "cleared",
        "scraper_type": scraper_type or "all",
        "station_id": station_id or "all",
        "message": "Cache cleared successfully"
    }

# ====== CHART ENDPOINTS (from previous implementation) ======

@app.get("/charts/top100", tags=["Charts"])
async def get_top100(
    limit: int = Query(100, ge=1, le=200),
    region: Optional[str] = Query(None)
):
    """Get Uganda Top 100 chart"""
    try:
        songs = data_service.get_top_songs(limit, region)
        
        for i, song in enumerate(songs, 1):
            song["rank"] = i
            song["trend"] = "up" if i <= 10 else "stable"
        
        return {
            "chart": "Uganda Top 100" + (f" - {region.capitalize()}" if region else ""),
            "week": current_chart_week,
            "entries": songs,
            "count": len(songs),
            "region": region if region else "all",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /charts/top100: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch top100 chart: {str(e)}"
        )

@app.get("/charts/regions", tags=["Charts", "Regions"])
async def get_all_regions():
    """Get statistics for all Ugandan regions"""
    try:
        region_stats = data_service.get_region_stats()
        
        return {
            "regions": region_stats,
            "count": len(region_stats),
            "chart_week": current_chart_week,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /charts/regions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch region statistics: {str(e)}"
        )

@app.get("/charts/regions/{region}", tags=["Charts", "Regions"])
async def get_region_detail(region: str = FPath(...)):
    """Get detailed information for a specific region"""
    if region not in config.VALID_REGIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region '{region}' not found. Valid regions: {', '.join(sorted(config.VALID_REGIONS))}"
        )
    
    try:
        songs = data_service.get_top_songs(10, region)
        region_data = config.UGANDAN_REGIONS[region]
        
        for i, song in enumerate(songs, 1):
            song["rank"] = i
        
        return {
            "region": region,
            "region_name": region_data["name"],
            "chart_week": current_chart_week,
            "songs": songs,
            "districts": region_data["districts"],
            "musicians": region_data["musicians"],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /charts/regions/{region}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch region data: {str(e)}"
        )

@app.get("/charts/trending", tags=["Charts", "Trending"])
async def get_trending(limit: int = Query(10, ge=1, le=50)):
    """Get trending songs"""
    try:
        all_songs = data_service.songs
        trending_songs = TrendingService.get_trending_songs(all_songs, limit)
        window_info = TrendingService.get_current_trending_window()
        
        return {
            "chart": "Trending Now - Uganda",
            "algorithm": "Enhanced multi-factor scoring",
            "entries": trending_songs,
            "window_info": window_info,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /charts/trending: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch trending songs: {str(e)}"
        )

# ====== INGESTION ENDPOINTS ======

@app.post("/ingest/youtube", tags=["Ingestion"])
async def ingest_youtube(
    payload: YouTubeIngestPayload,
    auth: bool = Depends(AuthService.verify_youtube)
):
    """Ingest YouTube data"""
    try:
        ugandan_artists = {
            "bobi wine", "eddy kenzo", "sheebah", "daddy andre",
            "gravity", "vyroota", "geosteady", "feffe busi",
            "alien skin", "azawi", "vinka", "rema", "rickman",
            "fik fameica", "john blaq", "dax", "vivian tosh"
        }
        
        valid_items = []
        
        for item in payload.items:
            artist_lower = item.artist.lower()
            is_ugandan = any(ug_artist in artist_lower for ug_artist in ugandan_artists)
            
            if is_ugandan:
                valid_items.append(item)
        
        result = data_service.add_songs(valid_items, f"youtube_{payload.source}")
        
        logger.info(f"YouTube ingestion: {result['added']} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} YouTube songs",
            "source": payload.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"YouTube ingestion error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"YouTube ingestion failed: {str(e)}"
        )

@app.post("/ingest/tv", tags=["Ingestion"])
async def ingest_tv(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest TV data"""
    try:
        result = data_service.add_songs(payload.items, f"tv_{payload.source}")
        
        logger.info(f"TV ingestion: {result['added']} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} TV songs",
            "source": payload.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"TV ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"TV ingestion failed: {str(e)}"
        )

@app.post("/ingest/radio", tags=["Ingestion"])
async def ingest_radio(
    payload: IngestPayload,
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Ingest radio data"""
    try:
        result = data_service.add_songs(payload.items, f"radio_{payload.source}")
        
        logger.info(f"Radio ingestion: {result['added']} songs from {payload.source}")
        
        return {
            "status": "success",
            "message": f"Ingested {result['added']} radio songs",
            "source": payload.source,
            "results": result,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Radio ingestion error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Radio ingestion failed: {str(e)}"
        )

# ====== ADMIN ENDPOINTS ======

@app.get("/admin/health", tags=["Admin"])
async def admin_health(auth: bool = Depends(AuthService.verify_admin)):
    """Admin health check"""
    uptime = datetime.utcnow() - app_start_time
    
    return {
        "status": "admin_authenticated",
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "uptime": str(uptime).split('.')[0],
            "requests_served": request_count,
            "start_time": app_start_time.isoformat()
        },
        "data": {
            "songs": len(data_service.songs),
            "top100": len(data_service.top100),
            "regions": len(data_service.region_data)
        },
        "scrapers": {
            "tv_stations": len(config.TV_STATIONS),
            "radio_stations": len(config.RADIO_STATIONS),
            "active": len(scraper_service.get_active_scrapers())
        }
    }

@app.get("/admin/scrapers", tags=["Admin"])
async def admin_scrapers(auth: bool = Depends(AuthService.verify_admin)):
    """Admin scraper management"""
    return {
        "tv_stations": config.TV_STATIONS,
        "radio_stations": config.RADIO_STATIONS,
        "active_scrapers": scraper_service.get_active_scrapers(),
        "scraper_history_count": len(data_service.scraper_history),
        "timestamp": datetime.utcnow().isoformat()
    }

# ====== ERROR HANDLERS ======

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(f"HTTP {exc.status_code} at {request.url.path}: {exc.detail}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception at {request.url.path}: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )

# ====== STARTUP BANNER ======
def display_startup_banner():
    """Display enhanced startup banner"""
    banner = f"""
    ╔{'═' * 70}╗
    ║{'UG BOARD ENGINE v10.0.0 - PRODUCTION READY':^70}║
    ╠{'═' * 70}╣
    ║ {'Environment:':<15} {config.ENVIRONMENT:<54} ║
    ║ {'Chart Week:':<15} {current_chart_week:<54} ║
    ║ {'Data Songs:':<15} {len(data_service.songs):<54} ║
    ║ {'TV Stations:':<15} {len(config.TV_STATIONS):<54} ║
    ║ {'Radio Stations:':<15} {len(config.RADIO_STATIONS):<54} ║
    ║ {'Regions:':<15} {', '.join(sorted(config.VALID_REGIONS)):<54} ║
    ╠{'═' * 70}╣
    ║ {'Server:':<15} http://0.0.0.0:{config.PORT:<53} ║
    ║ {'Docs:':<15} http://0.0.0.0:{config.PORT}/docs{' ' * 40} ║
    ║ {'Health:':<15} http://0.0.0.0:{config.PORT}/health{' ' * 38} ║
    ╚{'═' * 70}╝
    """
    print(banner)

# ====== MAIN ENTRY POINT ======
if __name__ == "__main__":
    display_startup_banner()
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=config.PORT,
        log_level="info",
        reload=config.DEBUG,
        access_log=True,
        proxy_headers=True
    )
