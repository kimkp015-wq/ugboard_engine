"""
tv_scraper.py - Production TV Music Scraper for UG Board Engine
Integrated with Dejavu for local fingerprint matching and ACRCloud fallback
"""

import os
import sys
import json
import time
import signal
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import threading
import queue
import subprocess
import hashlib

# Add parent directory for UG Board imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class StationConfig:
    """TV station configuration"""
    name: str
    stream_url: str
    weight: int = 1
    region: str = "ug"
    language: str = "english"
    enabled: bool = True
    check_interval: int = 300  # seconds
    last_checked: Optional[datetime] = None
    reliability_score: float = 0.0
    
    def to_dict(self):
        return {
            **asdict(self),
            'last_checked': self.last_checked.isoformat() if self.last_checked else None
        }

@dataclass
class SongDetection:
    """Detected song data structure"""
    station: str
    song_title: str
    artist: str
    detected_at: datetime
    confidence: float
    duration: int
    metadata: Dict
    raw_audio_path: Optional[str] = None
    
    def to_ugboard_item(self) -> Dict:
        """Convert to UG Board Engine ingestion format"""
        return {
            "title": self.song_title,
            "artist": self.artist,
            "channel": self.station,
            "plays": 1,
            "score": self.confidence * 100,  # Convert to 0-100 scale
            "region": "ug",  # Default Uganda region
            "genre": self.metadata.get("genre", "afrobeat"),
            "metadata": {
                **self.metadata,
                "source": "tv_scraper",
                "detection_method": "audio_fingerprinting",
                "confidence": self.confidence,
                "detected_at": self.detected_at.isoformat(),
                "duration_seconds": self.duration
            }
        }

class TVScraperEngine:
    """Main TV scraping engine with local fingerprint matching"""
    
    def __init__(self, config_path: str = "data/tv_streams.json"):
        self.config_path = config_path
        self.stations: Dict[str, StationConfig] = {}
        self.running = False
        self.threads = []
        self.result_queue = queue.Queue()
        self.dejavu_engine = None
        
        # Load configuration
        self._load_configuration()
        self._init_fingerprint_engine()
        
        # Ensure directories exist
        os.makedirs("temp/audio_captures", exist_ok=True)
        os.makedirs("data/tv_fingerprints", exist_ok=True)
        
    def _load_configuration(self):
        """Load station configuration from file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    stations_data = json.load(f)
                
                for name, data in stations_data.items():
                    self.stations[name] = StationConfig(
                        name=name,
                        stream_url=data.get("stream_url", ""),
                        weight=data.get("weight", 1),
                        region=data.get("region", "ug"),
                        language=data.get("language", "english"),
                        enabled=data.get("enabled", True),
                        check_interval=data.get("check_interval", 300),
                        reliability_score=data.get("reliability_score", 0.0)
                    )
                logger.info(f"Loaded {len(self.stations)} stations from config")
            else:
                logger.warning(f"Config file {self.config_path} not found, using defaults")
                self._load_default_stations()
                
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self._load_default_stations()
    
    def _load_default_stations(self):
        """Load default Ugandan TV stations"""
        default_stations = {
            "NTV Uganda": "https://cdn.example.com/ntv/live/playlist.m3u8",
            "NBS TV": "https://stream.nextmedia.co.ug/nbs/index.m3u8",
            "Bukedde TV 1": "https://visiongroup.ug/bukedde1/master.m3u8",
            "TV West": "https://visiongroup.ug/tvwest/live.m3u8",
            "Galaxy TV": "https://live.galaxy.co.ug/hls/stream.m3u8",
            "Baba TV": "https://babatv.ug/live/index.m3u8",
            "BBS Terefayina": "https://bbs.ug/live/master.m3u8",
            "Urban TV": "https://visiongroup.ug/urban/index.m3u8",
            "Spark TV": "https://cdn.example.com/spark/live.m3u8",
            "NBS Sport": "https://stream.nextmedia.co.ug/sport/index.m3u8"
        }
        
        for name, url in default_stations.items():
            self.stations[name] = StationConfig(
                name=name,
                stream_url=url,
                weight=10 if "NTV" in name or "NBS" in name else 5,
                region="ug",
                language="english" if "TV" not in name else "luganda",
                enabled=True,
                check_interval=300
            )
    
    def _init_fingerprint_engine(self):
        """Initialize Dejavu fingerprint engine"""
        try:
            from dejavu import Dejavu
            from dejavu.logic.recognizer.file_recognizer import FileRecognizer
            
            # Dejavu configuration
            config = {
                "database": {
                    "host": "127.0.0.1",
                    "user": "root",
                    "password": "",
                    "database": "dejavu_db"
                },
                "database_type": "mysql",
                "fingerprint_limit": 10
            }
            
            self.dejavu_engine = Dejavu(config)
            logger.info("Dejavu fingerprint engine initialized")
            
        except ImportError:
            logger.warning("Dejavu not installed. Using basic detection only.")
            self.dejavu_engine = None
        except Exception as e:
            logger.error(f"Failed to initialize Dejavu: {e}")
            self.dejavu_engine = None
    
    def capture_audio_sample(self, station: StationConfig, duration: int = 7) -> Optional[str]:
        """
        Capture audio sample from TV stream using ffmpeg
        Returns path to captured audio file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        station_slug = station.name.replace(" ", "_").replace("/", "_")
        filename = f"temp/audio_captures/{station_slug}_{timestamp}.mp3"
        
        try:
            # Build ffmpeg command for HLS stream capture
            cmd = [
                'ffmpeg',
                '-i', station.stream_url,
                '-t', str(duration),          # Duration in seconds
                '-vn',                        # No video
                '-acodec', 'libmp3lame',      # MP3 codec
                '-ac', '1',                   # Mono audio (smaller files)
                '-ar', '22050',               # Sample rate optimized for fingerprinting
                '-y',                         # Overwrite output file
                filename
            ]
            
            # Run ffmpeg with timeout
            result = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=duration + 10
            )
            
            if result.returncode == 0 and os.path.exists(filename):
                # Verify file is valid and has content
                if os.path.getsize(filename) > 1000:  # At least 1KB
                    logger.debug(f"Captured audio from {station.name}: {filename}")
                    return filename
                else:
                    os.remove(filename)
                    logger.warning(f"Empty audio file captured from {station.name}")
                    return None
            else:
                error_msg = result.stderr.decode('utf-8', errors='ignore')[:200]
                logger.warning(f"FFmpeg failed for {station.name}: {error_msg}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.warning(f"Audio capture timeout for {station.name}")
            return None
        except Exception as e:
            logger.error(f"Audio capture failed for {station.name}: {e}")
            return None
    
    def identify_song_local(self, audio_file: str) -> Optional[Dict]:
        """
        Identify song using local Dejavu fingerprint database
        """
        if not self.dejavu_engine or not os.path.exists(audio_file):
            return None
        
        try:
            from dejavu.logic.recognizer.file_recognizer import FileRecognizer
            
            # Recognize song from audio file
            result = self.dejavu_engine.recognize(FileRecognizer, audio_file)
            
            if result and "results" in result and result["results"]:
                best_match = result["results"][0]
                return {
                    "song_title": best_match.get("song_name", "Unknown"),
                    "artist": best_match.get("artist", "Unknown"),
                    "confidence": best_match.get("confidence", 0.0) / 100.0,
                    "match_time": best_match.get("offset_seconds", 0),
                    "fingerprint_hash": best_match.get("hash", ""),
                    "source": "dejavu_local"
                }
        
        except Exception as e:
            logger.error(f"Local song identification failed: {e}")
        
        return None
    
    def identify_song_acrcloud(self, audio_file: str) -> Optional[Dict]:
        """
        Identify song using ACRCloud API (fallback)
        """
        # This requires ACRCloud API credentials
        acr_host = os.getenv("ACRCLOUD_HOST")
        acr_key = os.getenv("ACRCLOUD_ACCESS_KEY")
        acr_secret = os.getenv("ACRCLOUD_ACCESS_SECRET")
        
        if not all([acr_host, acr_key, acr_secret]):
            logger.debug("ACRCloud credentials not configured")
            return None
        
        try:
            import base64
            import hashlib
            import hmac
            import requests
            import time as ttime
            
            http_method = "POST"
            http_uri = "/v1/identify"
            data_type = "audio"
            signature_version = "1"
            timestamp = str(int(ttime.time()))
            
            # Prepare signature
            string_to_sign = f"{http_method}\n{http_uri}\n{acr_key}\n{data_type}\n{signature_version}\n{timestamp}"
            sign = base64.b64encode(hmac.new(
                acr_secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                digestmod=hashlib.sha1
            ).digest()).decode('utf-8')
            
            # Read audio file
            with open(audio_file, 'rb') as f:
                audio_data = f.read()
            
            # Prepare request
            files = {'sample': audio_data}
            data = {
                'access_key': acr_key,
                'sample_bytes': len(audio_data),
                'timestamp': timestamp,
                'signature': sign,
                'data_type': data_type,
                'signature_version': signature_version
            }
            
            # Send request
            response = requests.post(f"http://{acr_host}/v1/identify", files=files, data=data, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status", {}).get("code") == 0:
                    metadata = result.get("metadata", {})
                    if metadata.get("music"):
                        music_data = metadata["music"][0]
                        return {
                            "song_title": music_data.get("title", "Unknown"),
                            "artist": music_data.get("artists", [{}])[0].get("name", "Unknown"),
                            "confidence": music_data.get("score", 0.0) / 100.0,
                            "album": music_data.get("album", {}).get("name", ""),
                            "label": music_data.get("label", ""),
                            "genre": music_data.get("genres", [{}])[0].get("name", ""),
                            "release_date": music_data.get("release_date", ""),
                            "source": "acrcloud_api"
                        }
            
        except ImportError:
            logger.warning("Requests library not installed for ACRCloud")
        except Exception as e:
            logger.error(f"ACRCloud identification failed: {e}")
        
        return None
    
    def monitor_station(self, station: StationConfig):
        """Monitor a single TV station for music"""
        logger.info(f"Starting monitor for {station.name}")
        
        while self.running:
            try:
                # Update last checked time
                station.last_checked = datetime.now()
                
                # Capture audio sample
                audio_file = self.capture_audio_sample(station)
                if not audio_file:
                    # Station might be offline, wait longer
                    time.sleep(station.check_interval * 2)
                    continue
                
                # Try local identification first
                song_data = self.identify_song_local(audio_file)
                
                # Fallback to ACRCloud if local fails
                if not song_data:
                    song_data = self.identify_song_acrcloud(audio_file)
                
                # If song identified, process it
                if song_data and song_data.get("confidence", 0) > 0.3:  # 30% confidence threshold
                    detection = SongDetection(
                        station=station.name,
                        song_title=song_data.get("song_title", "Unknown"),
                        artist=song_data.get("artist", "Unknown"),
                        detected_at=datetime.now(),
                        confidence=song_data.get("confidence", 0.0),
                        duration=7,  # Our capture duration
                        metadata={
                            "region": station.region,
                            "language": station.language,
                            "identification_source": song_data.get("source", "unknown"),
                            "album": song_data.get("album", ""),
                            "genre": song_data.get("genre", ""),
                            "label": song_data.get("label", "")
                        },
                        raw_audio_path=audio_file
                    )
                    
                    # Queue result for processing
                    self.result_queue.put(detection)
                    
                    # Update station reliability
                    station.reliability_score = min(1.0, station.reliability_score + 0.05)
                    
                    logger.info(f"🎵 {station.name}: {detection.artist} - {detection.song_title} ({detection.confidence:.1%})")
                    
                    # Keep successful captures for debugging
                    if detection.confidence > 0.8:
                        # Rename with artist-song for high confidence matches
                        new_name = f"temp/confirmed/{station.name.replace(' ', '_')}_{detection.artist}_{detection.song_title}.mp3"
                        os.makedirs("temp/confirmed", exist_ok=True)
                        try:
                            os.rename(audio_file, new_name)
                        except:
                            pass
                    else:
                        # Clean up low-confidence captures
                        try:
                            os.remove(audio_file)
                        except:
                            pass
                else:
                    # Clean up if no identification
                    try:
                        os.remove(audio_file)
                    except:
                        pass
                
                # Adaptive sleep based on station weight and reliability
                base_interval = station.check_interval
                if station.reliability_score < 0.3:
                    base_interval *= 2  # Check unreliable stations less frequently
                
                time.sleep(base_interval)
                
            except Exception as e:
                logger.error(f"Monitor error for {station.name}: {e}")
                station.reliability_score = max(0.0, station.reliability_score - 0.1)
                time.sleep(60)  # Wait before retry
    
    def result_processor(self):
        """Process detected songs and send to UG Board Engine"""
        from api.ingestion.tv_processor import TVIngestionProcessor
        
        processor = TVIngestionProcessor()
        logger.info("Result processor started")
        
        while self.running:
            try:
                # Wait for results with timeout
                try:
                    detection = self.result_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Convert to UG Board format
                ugboard_item = detection.to_ugboard_item()
                
                # Send to UG Board Engine
                success = processor.send_to_ugboard(ugboard_item)
                
                if success:
                    logger.debug(f"Successfully sent: {detection.artist} - {detection.song_title}")
                else:
                    logger.warning(f"Failed to send: {detection.artist} - {detection.song_title}")
                
                # Clean up raw audio file if it exists
                if detection.raw_audio_path and os.path.exists(detection.raw_audio_path):
                    try:
                        os.remove(detection.raw_audio_path)
                    except:
                        pass
                
                # Mark task as done
                self.result_queue.task_done()
                
            except Exception as e:
                logger.error(f"Result processor error: {e}")
    
    def save_configuration(self):
        """Save current station configuration to file"""
        try:
            config_data = {}
            for name, station in self.stations.items():
                config_data[name] = station.to_dict()
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            with open(self.config_path, 'w') as f:
                json.dump(config_data, f, indent=2, default=str)
            
            logger.info(f"Configuration saved to {self.config_path}")
            
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
    
    def start(self):
        """Start the TV scraper engine"""
        if self.running:
            logger.warning("Scraper already running")
            return
        
        self.running = True
        logger.info("Starting TV Scraper Engine...")
        
        # Start result processor thread
        processor_thread = threading.Thread(target=self.result_processor, daemon=True)
        processor_thread.start()
        
        # Start monitor threads for enabled stations
        for name, station in self.stations.items():
            if station.enabled:
                thread = threading.Thread(
                    target=self.monitor_station,
                    args=(station,),
                    daemon=True
                )
                thread.start()
                self.threads.append(thread)
                
                # Stagger starts to avoid hitting all stations at once
                time.sleep(1)
        
        logger.info(f"Started {len(self.threads)} station monitors")
        
        # Main loop
        try:
            while self.running:
                # Save configuration periodically
                if int(time.time()) % 300 == 0:  # Every 5 minutes
                    self.save_configuration()
                
                # Log status
                if int(time.time()) % 60 == 0:
                    active_stations = len([s for s in self.stations.values() if s.enabled])
                    logger.info(f"Engine running - Monitoring {active_stations} stations")
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the TV scraper engine"""
        logger.info("Stopping TV Scraper Engine...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5)
        
        # Save final configuration
        self.save_configuration()
        logger.info("TV Scraper Engine stopped")

def main():
    """Main entry point for TV scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description="UG Board TV Music Scraper")
    parser.add_argument("--config", default="data/tv_streams.json", help="Configuration file path")
    parser.add_argument("--station", help="Monitor specific station only")
    parser.add_argument("--duration", type=int, default=7, help="Audio capture duration in seconds")
    parser.add_argument("--interval", type=int, default=300, help="Check interval in seconds")
    args = parser.parse_args()
    
    # Create engine
    engine = TVScraperEngine(config_path=args.config)
    
    # Override settings if specified
    if args.station and args.station in engine.stations:
        # Disable all stations except the specified one
        for name in engine.stations:
            engine.stations[name].enabled = (name == args.station)
    
    for station in engine.stations.values():
        if args.duration != 7:
            # Not directly configurable per station in this simple override
            pass
        if args.interval != 300:
            station.check_interval = args.interval
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        engine.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start engine
    engine.start()

if __name__ == "__main__":
    main()
