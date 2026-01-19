#!/usr/bin/env python3
"""
tv_scraper.py - Production TV Music Scraper for UG Board Engine
Integrated with GitHub Actions workflow and Ugandan TV stations
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict, field
import threading
import queue
import subprocess
import hashlib
import atexit

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] %(message)s',
    handlers=[
        logging.FileHandler('data/tv_logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class StationConfig:
    """TV station configuration for Ugandan stations"""
    name: str
    stream_url: str
    weight: int = 5  # 1-10 priority
    region: str = "ug"
    language: str = "english"
    enabled: bool = True
    check_interval: int = 300  # seconds
    capture_duration: int = 7  # seconds
    last_checked: Optional[datetime] = None
    reliability_score: float = 0.5
    success_count: int = 0
    failure_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_reliability(self, success: bool):
        """Update station reliability score"""
        if success:
            self.success_count += 1
            self.reliability_score = min(1.0, self.reliability_score + 0.02)
        else:
            self.failure_count += 1
            self.reliability_score = max(0.1, self.reliability_score - 0.05)
        
        # Adaptive interval based on reliability
        if self.reliability_score < 0.3:
            self.check_interval = 600  # Check unreliable stations less often
        elif self.reliability_score > 0.8:
            self.check_interval = 180  # Check reliable stations more often
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['last_checked'] = self.last_checked.isoformat() if self.last_checked else None
        return data

@dataclass
class SongDetection:
    """Detected song with Ugandan music context"""
    station: str
    song_title: str
    artist: str
    detected_at: datetime
    confidence: float
    duration: int
    metadata: Dict[str, Any]
    raw_audio_path: Optional[str] = None
    fingerprint_match: bool = False
    
    def to_ugboard_format(self) -> Dict:
        """Format for UG Board Engine ingestion API"""
        from api.ingestion.tv import MusicRules  # Import your existing validation
        
        # Extract artists list
        artists = MusicRules.extract_artist_list(self.artist)
        
        # Build the payload
        return {
            "title": self.song_title[:200],
            "artist": self.artist[:200],
            "channel": self.station,
            "plays": 1,
            "score": float(self.confidence * 100),  # Convert to 0-100 scale
            "genre": self.metadata.get("genre", "afrobeat"),
            "region": "ug",
            "release_date": datetime.now().date().isoformat(),
            "metadata": {
                **self.metadata,
                "source": "tv_scraper",
                "detection_method": "audio_fingerprinting",
                "confidence": float(self.confidence),
                "fingerprint_match": self.fingerprint_match,
                "detected_at": self.detected_at.isoformat(),
                "duration_seconds": self.duration,
                "artists_list": artists,
                "artist_types": [MusicRules.get_artist_type(a) for a in artists],
                "is_collaboration": len(artists) > 1,
                "has_ugandan_artist": any(MusicRules.is_ugandan_artist(a) for a in artists),
                "has_foreign_artist": any(not MusicRules.is_ugandan_artist(a) for a in artists)
            }
        }

class TVScraperEngine:
    """Main TV scraping engine for Ugandan stations"""
    
    def __init__(self, config_path: str = "config/tv_stations.yaml"):
        self.config_path = config_path
        self.stations: Dict[str, StationConfig] = {}
        self.running = False
        self.threads: List[threading.Thread] = []
        self.detection_queue = queue.Queue(maxsize=100)
        self.dejavu_engine = None
        self.acrcloud_available = False
        self.ugboard_url = os.getenv("UGBOARD_API_URL", "http://localhost:8000")
        self.internal_token = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
        
        # Initialize components
        self._setup_directories()
        self._load_configuration()
        self._init_fingerprint_engine()
        self._check_acrcloud()
        
        # Register cleanup
        atexit.register(self.cleanup)
        
        logger.info(f"TV Scraper Engine initialized with {len([s for s in self.stations.values() if s.enabled])} enabled stations")
    
    def _setup_directories(self):
        """Ensure required directories exist"""
        directories = [
            "temp/audio_captures",
            "temp/confirmed",
            "data/tv_logs",
            "data/tv_fingerprints",
            "config"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def _load_configuration(self):
        """Load station configuration from YAML or JSON"""
        try:
            # Try YAML first
            try:
                import yaml
                with open(self.config_path, 'r') as f:
                    stations_data = yaml.safe_load(f)
            except (ImportError, FileNotFoundError):
                # Fallback to JSON
                json_path = self.config_path.replace('.yaml', '.json')
                with open(json_path, 'r') as f:
                    stations_data = json.load(f)
            
            # Load stations from configuration
            for name, data in stations_data.get("stations", {}).items():
                self.stations[name] = StationConfig(
                    name=name,
                    stream_url=data.get("stream_url", ""),
                    weight=data.get("weight", 5),
                    region=data.get("region", "ug"),
                    language=data.get("language", "english"),
                    enabled=data.get("enabled", True),
                    check_interval=data.get("check_interval", 300),
                    capture_duration=data.get("capture_duration", 7),
                    reliability_score=data.get("reliability_score", 0.5),
                    metadata=data.get("metadata", {})
                )
            
            logger.info(f"Loaded {len(self.stations)} stations from {self.config_path}")
            
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_path} not found, loading default Ugandan stations")
            self._load_default_stations()
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self._load_default_stations()
    
    def _load_default_stations(self):
        """Load default Ugandan TV stations"""
        default_stations = {
            "NTV Uganda": {
                "stream_url": "https://cdn.example.com/ntv/live/playlist.m3u8",
                "weight": 10,
                "language": "english",
                "region": "central"
            },
            "NBS TV": {
                "stream_url": "https://stream.nextmedia.co.ug/nbs/index.m3u8",
                "weight": 10,
                "language": "english",
                "region": "central"
            },
            "Bukedde TV 1": {
                "stream_url": "https://visiongroup.ug/bukedde1/master.m3u8",
                "weight": 8,
                "language": "luganda",
                "region": "central"
            },
            "TV West": {
                "stream_url": "https://visiongroup.ug/tvwest/live.m3u8",
                "weight": 7,
                "language": "runyankole",
                "region": "western"
            },
            "Baba TV": {
                "stream_url": "https://babatv.ug/live/index.m3u8",
                "weight": 6,
                "language": "lusoga",
                "region": "eastern"
            }
        }
        
        for name, data in default_stations.items():
            self.stations[name] = StationConfig(
                name=name,
                stream_url=data["stream_url"],
                weight=data["weight"],
                language=data["language"],
                region=data["region"],
                enabled=True,
                check_interval=300,
                capture_duration=7
            )
    
    def _init_fingerprint_engine(self):
        """Initialize Dejavu fingerprint engine for local matching"""
        try:
            from dejavu import Dejavu
            from dejavu.logic.recognizer.file_recognizer import FileRecognizer
            
            # Check if MySQL is available (for GitHub Actions)
            db_host = os.getenv("DEJAVU_DB_HOST", "127.0.0.1")
            db_user = os.getenv("DEJAVU_DB_USER", "root")
            db_pass = os.getenv("DEJAVU_DB_PASSWORD", "rootpassword")
            db_name = os.getenv("DEJAVU_DB_NAME", "dejavu_db")
            
            config = {
                "database": {
                    "host": db_host,
                    "user": db_user,
                    "password": db_pass,
                    "database": db_name
                },
                "database_type": "mysql",
                "fingerprint_limit": 10
            }
            
            self.dejavu_engine = Dejavu(config)
            
            # Test connection
            if hasattr(self.dejavu_engine, 'db'):
                logger.info("✅ Dejavu fingerprint engine initialized successfully")
            else:
                logger.warning("⚠️ Dejavu engine created but database connection not verified")
            
        except ImportError as e:
            logger.warning(f"Dejavu not installed: {e}. Using ACRCloud only.")
            self.dejavu_engine = None
        except Exception as e:
            logger.error(f"Failed to initialize Dejavu: {e}")
            self.dejavu_engine = None
    
    def _check_acrcloud(self):
        """Check if ACRCloud credentials are available"""
        required_vars = [
            "ACRCLOUD_HOST",
            "ACRCLOUD_ACCESS_KEY", 
            "ACRCLOUD_ACCESS_SECRET"
        ]
        
        self.acrcloud_available = all(os.getenv(var) for var in required_vars)
        
        if self.acrcloud_available:
            logger.info("✅ ACRCloud API credentials available")
        else:
            logger.warning("⚠️ ACRCloud credentials not fully configured")
    
    def capture_audio(self, station: StationConfig) -> Optional[str]:
        """
        Capture audio sample from TV stream using ffmpeg
        Returns path to captured audio file or None
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        station_slug = station.name.replace(" ", "_").replace("/", "_")
        filename = f"temp/audio_captures/{station_slug}_{timestamp}.mp3"
        
        try:
            # Build optimized ffmpeg command for Ugandan streams
            cmd = [
                'ffmpeg',
                '-i', station.stream_url,
                '-t', str(station.capture_duration),
                '-vn',                        # No video
                '-acodec', 'libmp3lame',
                '-ac', '1',                   # Mono (saves bandwidth)
                '-ar', '22050',               # Optimized for fingerprinting
                '-b:a', '64k',                # Low bitrate for speech/music
                '-y',                         # Overwrite output
                '-loglevel', 'error',         # Suppress verbose output
                filename
            ]
            
            # Run with timeout
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=station.capture_duration + 15
            )
            
            if result.returncode == 0 and os.path.exists(filename):
                file_size = os.path.getsize(filename)
                if file_size > 1024:  # At least 1KB
                    logger.debug(f"Captured {file_size/1024:.1f}KB audio from {station.name}")
                    return filename
                else:
                    os.remove(filename)
                    logger.warning(f"Empty audio file from {station.name}")
            else:
                error_msg = result.stderr.decode('utf-8', errors='ignore')[:100]
                logger.warning(f"FFmpeg failed for {station.name}: {error_msg}")
                
        except subprocess.TimeoutExpired:
            logger.warning(f"Audio capture timeout for {station.name}")
        except Exception as e:
            logger.error(f"Audio capture error for {station.name}: {e}")
        
        return None
    
    def identify_song(self, audio_file: str) -> Optional[Dict[str, Any]]:
        """
        Identify song using multiple methods (Dejavu → ACRCloud)
        Returns song data or None
        """
        if not os.path.exists(audio_file):
            return None
        
        # Method 1: Local Dejavu fingerprinting (fast, free)
        song_data = self._identify_dejavu(audio_file)
        
        # Method 2: ACRCloud API (fallback, more comprehensive)
        if not song_data and self.acrcloud_available:
            song_data = self._identify_acrcloud(audio_file)
        
        return song_data
    
    def _identify_dejavu(self, audio_file: str) -> Optional[Dict[str, Any]]:
        """Identify song using local Dejavu database"""
        if not self.dejavu_engine:
            return None
        
        try:
            from dejavu.logic.recognizer.file_recognizer import FileRecognizer
            
            result = self.dejavu_engine.recognize(FileRecognizer, audio_file)
            
            if result and result.get("results"):
                best_match = result["results"][0]
                confidence = best_match.get("confidence", 0) / 100.0
                
                if confidence > 0.3:  # 30% confidence threshold
                    return {
                        "song_title": best_match.get("song_name", "Unknown"),
                        "artist": best_match.get("artist", "Unknown"),
                        "confidence": confidence,
                        "match_time": best_match.get("offset_seconds", 0),
                        "fingerprint_hash": best_match.get("hash", ""),
                        "source": "dejavu_local",
                        "fingerprint_match": True
                    }
        
        except Exception as e:
            logger.debug(f"Dejavu identification failed: {e}")
        
        return None
    
    def _identify_acrcloud(self, audio_file: str) -> Optional[Dict[str, Any]]:
        """Identify song using ACRCloud API"""
        try:
            import base64
            import hashlib
            import hmac
            import requests
            
            # Get credentials
            host = os.getenv("ACRCLOUD_HOST")
            access_key = os.getenv("ACRCLOUD_ACCESS_KEY")
            access_secret = os.getenv("ACRCLOUD_ACCESS_SECRET")
            
            # Prepare signature
            http_method = "POST"
            http_uri = "/v1/identify"
            data_type = "audio"
            signature_version = "1"
            timestamp = str(int(time.time()))
            
            string_to_sign = f"{http_method}\n{http_uri}\n{access_key}\n{data_type}\n{signature_version}\n{timestamp}"
            signature = base64.b64encode(hmac.new(
                access_secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                digestmod=hashlib.sha1
            ).digest()).decode('utf-8')
            
            # Read audio file
            with open(audio_file, 'rb') as f:
                audio_data = f.read()
            
            # Prepare request
            files = {'sample': audio_data}
            data = {
                'access_key': access_key,
                'sample_bytes': len(audio_data),
                'timestamp': timestamp,
                'signature': signature,
                'data_type': data_type,
                'signature_version': signature_version
            }
            
            # Send request
            response = requests.post(
                f"https://{host}/v1/identify",
                files=files,
                data=data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status", {}).get("code") == 0:
                    metadata = result.get("metadata", {})
                    if metadata.get("music"):
                        music = metadata["music"][0]
                        return {
                            "song_title": music.get("title", "Unknown"),
                            "artist": music.get("artists", [{}])[0].get("name", "Unknown"),
                            "confidence": music.get("score", 0) / 100.0,
                            "album": music.get("album", {}).get("name", ""),
                            "genre": music.get("genres", [{}])[0].get("name", ""),
                            "label": music.get("label", ""),
                            "release_date": music.get("release_date", ""),
                            "source": "acrcloud_api",
                            "fingerprint_match": False
                        }
        
        except ImportError:
            logger.warning("Requests library not installed for ACRCloud")
        except Exception as e:
            logger.error(f"ACRCloud identification failed: {e}")
        
        return None
    
    def monitor_station(self, station: StationConfig):
        """Monitor a single TV station for music"""
        thread_name = f"Monitor-{station.name[:15]}"
        threading.current_thread().name = thread_name
        
        logger.info(f"Starting monitor for {station.name} (interval: {station.check_interval}s)")
        
        while self.running and station.enabled:
            try:
                station.last_checked = datetime.now()
                
                # Capture audio
                audio_file = self.capture_audio(station)
                if not audio_file:
                    station.update_reliability(False)
                    time.sleep(station.check_interval * 2)  # Wait longer on failure
                    continue
                
                # Identify song
                song_data = self.identify_song(audio_file)
                
                if song_data and song_data.get("confidence", 0) > 0.3:
                    # Create detection object
                    detection = SongDetection(
                        station=station.name,
                        song_title=song_data["song_title"],
                        artist=song_data["artist"],
                        detected_at=datetime.now(),
                        confidence=song_data["confidence"],
                        duration=station.capture_duration,
                        metadata={
                            "region": station.region,
                            "language": station.language,
                            "identification_source": song_data["source"],
                            "genre": song_data.get("genre", ""),
                            "album": song_data.get("album", ""),
                            "station_weight": station.weight,
                            "reliability_score": station.reliability_score
                        },
                        raw_audio_path=audio_file,
                        fingerprint_match=song_data.get("fingerprint_match", False)
                    )
                    
                    # Queue for processing
                    self.detection_queue.put(detection)
                    
                    # Update station reliability
                    station.update_reliability(True)
                    
                    # Log success
                    logger.info(
                        f"🎵 {station.name}: {detection.artist} - "
                        f"{detection.song_title} ({detection.confidence:.1%})"
                    )
                    
                    # Archive high-confidence detections
                    if detection.confidence > 0.8:
                        self._archive_detection(detection, audio_file)
                    else:
                        os.remove(audio_file)
                        
                else:
                    # No song detected
                    station.update_reliability(False)
                    os.remove(audio_file)
                
                # Sleep before next check
                time.sleep(station.check_interval)
                
            except Exception as e:
                logger.error(f"Monitor error for {station.name}: {e}")
                station.update_reliability(False)
                time.sleep(60)  # Wait before retry
    
    def _archive_detection(self, detection: SongDetection, audio_file: str):
        """Archive high-confidence detections for training"""
        try:
            # Create archive filename
            timestamp = detection.detected_at.strftime("%Y%m%d_%H%M%S")
            station_slug = detection.station.replace(" ", "_")
            artist_slug = detection.artist.replace(" ", "_")[:20]
            title_slug = detection.song_title.replace(" ", "_")[:20]
            
            archive_name = f"temp/confirmed/{station_slug}_{artist_slug}_{title_slug}_{timestamp}.mp3"
            
            # Move and rename file
            os.rename(audio_file, archive_name)
            
            # Log archive
            logger.debug(f"Archived detection: {archive_name}")
            
        except Exception as e:
            logger.error(f"Failed to archive detection: {e}")
            try:
                os.remove(audio_file)
            except:
                pass
    
    def process_detections(self):
        """Process detected songs and send to UG Board Engine"""
        threading.current_thread().name = "Result-Processor"
        
        logger.info("Result processor started")
        
        while self.running:
            try:
                # Get detection with timeout
                try:
                    detection = self.detection_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Convert to UG Board format
                ugboard_item = detection.to_ugboard_format()
                
                # Send to UG Board Engine
                success = self._send_to_ugboard(ugboard_item)
                
                if success:
                    logger.debug(f"Successfully sent: {detection.artist} - {detection.song_title}")
                else:
                    logger.warning(f"Failed to send: {detection.artist} - {detection.song_title}")
                
                # Cleanup
                if detection.raw_audio_path and os.path.exists(detection.raw_audio_path):
                    try:
                        os.remove(detection.raw_audio_path)
                    except:
                        pass
                
                # Mark as done
                self.detection_queue.task_done()
                
            except Exception as e:
                logger.error(f"Result processor error: {e}")
    
    def _send_to_ugboard(self, item: Dict) -> bool:
        """Send data to UG Board Engine API"""
        try:
            import requests
            
            # Build payload
            payload = {
                "items": [item],
                "source": "tv_scraper",
                "timestamp": datetime.utcnow().isoformat(),
                "metadata": {
                    "scraper_version": "2.0.0",
                    "detection_method": item["metadata"]["detection_method"],
                    "confidence": item["metadata"]["confidence"]
                }
            }
            
            # Send request
            response = requests.post(
                f"{self.ugboard_url}/ingest/tv",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": self.internal_token
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return True
            else:
                logger.error(f"UG Board API error {response.status_code}: {response.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send to UG Board: {e}")
            return False
    
    def save_configuration(self):
        """Save current station configuration"""
        try:
            config_data = {
                "stations": {},
                "last_updated": datetime.now().isoformat(),
                "total_stations": len(self.stations),
                "enabled_stations": len([s for s in self.stations.values() if s.enabled])
            }
            
            for name, station in self.stations.items():
                config_data["stations"][name] = station.to_dict()
            
            # Save as JSON
            with open("data/tv_streams.json", "w") as f:
                json.dump(config_data, f, indent=2, default=str)
            
            logger.debug("Configuration saved")
            
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
    
    def start(self, specific_station: Optional[str] = None):
        """Start the TV scraper engine"""
        if self.running:
            logger.warning("Scraper already running")
            return
        
        self.running = True
        logger.info("🚀 Starting TV Scraper Engine...")
        
        # Start result processor thread
        processor_thread = threading.Thread(
            target=self.process_detections,
            name="Result-Processor",
            daemon=True
        )
        processor_thread.start()
        
        # Start monitor threads
        started_count = 0
        
        for name, station in self.stations.items():
            if not station.enabled:
                continue
            
            # Filter by specific station if provided
            if specific_station and name != specific_station:
                continue
            
            # Create and start thread
            thread = threading.Thread(
                target=self.monitor_station,
                args=(station,),
                name=f"Monitor-{name[:15]}",
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
            started_count += 1
            
            # Stagger starts to avoid hitting all stations at once
            time.sleep(0.5)
        
        logger.info(f"Started {started_count} station monitors")
        
        # Main monitoring loop
        try:
            while self.running:
                # Periodic tasks
                current_time = time.time()
                
                # Save config every 5 minutes
                if int(current_time) % 300 == 0:
                    self.save_configuration()
                
                # Log status every minute
                if int(current_time) % 60 == 0:
                    active = len([t for t in self.threads if t.is_alive()])
                    queue_size = self.detection_queue.qsize()
                    logger.info(f"Status: {active} active monitors, {queue_size} pending detections")
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutdown requested via KeyboardInterrupt")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the TV scraper engine gracefully"""
        if not self.running:
            return
        
        logger.info("🛑 Stopping TV Scraper Engine...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5)
        
        # Save final configuration
        self.save_configuration()
        
        # Cleanup temp files
        self.cleanup()
        
        logger.info("✅ TV Scraper Engine stopped")
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            # Remove audio captures older than 1 hour
            import shutil
            if os.path.exists("temp/audio_captures"):
                shutil.rmtree("temp/audio_captures")
                os.makedirs("temp/audio_captures")
            
            logger.debug("Cleaned up temporary files")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def main():
    """Main entry point with CLI arguments"""
    parser = argparse.ArgumentParser(
        description="UG Board TV Music Scraper - Monitor Ugandan TV stations for music",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tv_scraper.py                    # Monitor all enabled stations
  python tv_scraper.py --station "NTV Uganda"  # Monitor specific station
  python tv_scraper.py --duration 10 --interval 180  # Custom settings
  python tv_scraper.py --config custom_config.yaml  # Custom config file
        """
    )
    
    parser.add_argument(
        "--station",
        help="Monitor specific station only (e.g., 'NTV Uganda')"
    )
    
    parser.add_argument(
        "--duration",
        type=int,
        default=7,
        help="Audio capture duration in seconds (default: 7)"
    )
    
    parser.add_argument(
        "--interval", 
        type=int,
        default=300,
        help="Check interval in seconds (default: 300)"
    )
    
    parser.add_argument(
        "--config",
        default="config/tv_stations.yaml",
        help="Configuration file path (default: config/tv_stations.yaml)"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Run stream URL discovery before scraping"
    )
    
    args = parser.parse_args()
    
    # Set debug logging
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    print("\n" + "="*60)
    print("📺 UGANDAN TV MUSIC SCRAPER v2.0")
    print("="*60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.station:
        print(f"Monitoring: {args.station}")
    else:
        print("Monitoring: All enabled stations")
    
    print(f"Capture Duration: {args.duration}s")
    print(f"Check Interval: {args.interval}s")
    print("="*60 + "\n")
    
    # Run stream discovery if requested
    if args.discover:
        try:
            from scripts.tv_stream_finder import discover_streams
            print("🔍 Discovering stream URLs...")
            discover_streams()
        except ImportError:
            logger.warning("Stream discovery module not available")
    
    # Create and run engine
    engine = TVScraperEngine(config_path=args.config)
    
    # Override settings from CLI args
    for station in engine.stations.values():
        station.capture_duration = args.duration
        station.check_interval = args.interval
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n📡 Received signal {signum}, shutting down gracefully...")
        engine.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start engine
    try:
        engine.start(specific_station=args.station)
    except Exception as e:
        logger.error(f"Engine failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
