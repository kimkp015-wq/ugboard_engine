"""
tv_scraper.py - TV Station Monitoring Engine for UG Board
Canonical implementation following UG Board Engine principles
"""

import asyncio
import aiohttp
import yaml
import logging
import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import signal
import sys
import subprocess

# Import your custom fingerprinting
from ingestion.fingerprinting.audio_fingerprinter import UgandanMusicFingerprinter, get_fingerprinter

logger = logging.getLogger(__name__)

@dataclass
class TVStation:
    """TV station configuration"""
    name: str
    stream_url: str
    region: str
    language: str
    priority: int = 1
    enabled: bool = True
    last_scraped: Optional[datetime] = None
    reliability_score: float = 1.0

@dataclass 
class SongIdentification:
    """Result of song identification"""
    song_title: str
    artist: str
    confidence: float
    identified_at: datetime
    station: str
    source: str  # "fingerprint", "acrcloud", "metadata"
    fingerprint_match: bool = False
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['identified_at'] = self.identified_at.isoformat()
        return data

@dataclass
class ScraperStats:
    """Scraper statistics for monitoring"""
    total_scrapes: int = 0
    successful_scrapes: int = 0
    songs_identified: int = 0
    fingerprint_matches: int = 0
    last_scrape_time: Optional[datetime] = None
    uptime_seconds: float = 0.0

class TVScraperEngine:
    """
    Main TV scraping engine for Ugandan TV stations.
    Follows UG Board Engine canonical principles:
    - Immutable: All identifications are timestamped and immutable
    - Idempotent: Safe to retry operations
    - Crash-safe: Graceful degradation on errors
    """
    
    def __init__(
        self,
        config_path: str = "config/tv_stations.yaml",
        data_dir: str = "data/tv_scraper",
        max_concurrent: int = 3,
        scrape_interval: int = 1800  # 30 minutes
    ):
        """
        Initialize TV scraper engine.
        
        Args:
            config_path: Path to TV stations configuration
            data_dir: Directory for storing scraped data
            max_concurrent: Maximum concurrent station scrapes
            scrape_interval: Seconds between scrapes per station
        """
        self.config_path = Path(config_path)
        self.data_dir = Path(data_dir)
        self.max_concurrent = max_concurrent
        self.scrape_interval = scrape_interval
        
        # Initialize directories
        self._init_directories()
        
        # Load configuration
        self.stations = self._load_stations()
        
        # Initialize components
        self.fingerprinter = get_fingerprinter()
        self.acrcloud_available = self._check_acrcloud()
        
        # State management
        self.running = False
        self.stats = ScraperStats()
        self.start_time = datetime.utcnow()
        
        # Session management
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"TVScraperEngine initialized with {len(self.stations)} stations")
    
    def _init_directories(self):
        """Initialize required directories following canonical structure."""
        directories = [
            self.data_dir,
            self.data_dir / "audio_samples",
            self.data_dir / "identifications",
            self.data_dir / "logs",
            self.data_dir / "state",
            Path("data/reference_tracks")  # For reference tracks
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def _load_stations(self) -> List[TVStation]:
        """Load TV stations from configuration file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            stations = []
            for station_config in config.get('stations', []):
                station = TVStation(
                    name=station_config['name'],
                    stream_url=station_config['stream_url'],
                    region=station_config.get('region', 'ug'),
                    language=station_config.get('language', 'en'),
                    priority=station_config.get('priority', 1),
                    enabled=station_config.get('enabled', True)
                )
                stations.append(station)
            
            # Sort by priority (higher priority first)
            stations.sort(key=lambda x: x.priority, reverse=True)
            
            logger.info(f"Loaded {len(stations)} TV stations from {self.config_path}")
            return stations
            
        except Exception as e:
            logger.error(f"Failed to load stations config: {e}")
            # Return minimal default stations as fallback
            return self._get_default_stations()
    
    def _get_default_stations(self) -> List[TVStation]:
        """Get default Ugandan TV stations for crash safety."""
        return [
            TVStation(
                name="NTV Uganda",
                stream_url="https://ntv.metropolitan.videopulse.co/ntv/ntv.m3u8",
                region="ug",
                language="en",
                priority=10
            ),
            TVStation(
                name="NBS Television",
                stream_url="https://cdn-ap-aka.metropolitan.videopulse.co/NTV/NTV.m3u8",
                region="ug",
                language="en",
                priority=9
            ),
            TVStation(
                name="Bukedde TV",
                stream_url="https://cdn-ap-aka.metropolitan.videopulse.co/bukedde/bukedde.m3u8",
                region="ug",
                language="lug",
                priority=8
            )
        ]
    
    def _check_acrcloud(self) -> bool:
        """Check if ACRCloud is available (optional commercial service)."""
        try:
            # Check for ACRCloud configuration
            acr_config_path = Path("config/acrcloud.yaml")
            if acr_config_path.exists():
                logger.info("ACRCloud configuration found (optional)")
                return True
            return False
        except Exception:
            return False
    
    async def identify_song(self, audio_file: str) -> Optional[SongIdentification]:
        """
        Identify song using multiple methods with fallbacks.
        Follows UG Board canonical principle: Truth over completeness.
        
        Args:
            audio_file: Path to audio file for identification
            
        Returns:
            SongIdentification if successful, None otherwise
        """
        if not os.path.exists(audio_file):
            logger.warning(f"Audio file does not exist: {audio_file}")
            return None
        
        file_size = os.path.getsize(audio_file)
        if file_size < 1024:  # 1KB minimum
            logger.warning(f"Audio file too small: {file_size} bytes")
            return None
        
        # Method 1: Custom fingerprinting (primary)
        logger.debug(f"Attempting fingerprint identification: {audio_file}")
        fingerprint_result = await self._identify_fingerprint(audio_file)
        
        if fingerprint_result and fingerprint_result.confidence >= 0.6:
            logger.info(f"Fingerprint match found: {fingerprint_result.artist} - {fingerprint_result.song_title}")
            self.stats.fingerprint_matches += 1
            return fingerprint_result
        
        # Method 2: ACRCloud (optional fallback)
        if self.acrcloud_available:
            logger.debug(f"Attempting ACRCloud identification: {audio_file}")
            acr_result = await self._identify_acrcloud(audio_file)
            if acr_result:
                logger.info(f"ACRCloud match found: {acr_result.artist} - {acr_result.song_title}")
                return acr_result
        
        # Method 3: Extract metadata from stream (fallback)
        logger.debug(f"Attempting metadata extraction: {audio_file}")
        metadata_result = await self._extract_metadata(audio_file)
        if metadata_result:
            logger.info(f"Metadata extracted: {metadata_result.artist} - {metadata_result.song_title}")
            return metadata_result
        
        logger.debug(f"No identification methods succeeded for: {audio_file}")
        return None
    
    async def _identify_fingerprint(self, audio_file: str) -> Optional[SongIdentification]:
        """Identify song using custom fingerprinting."""
        try:
            # Run fingerprinting in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            fingerprint = await loop.run_in_executor(
                None, self.fingerprinter.extract_fingerprint, audio_file
            )
            
            if not fingerprint:
                return None
            
            # Find match in database
            match = await loop.run_in_executor(
                None, self.fingerprinter.find_match, fingerprint, 0.6
            )
            
            if match:
                song_title, artist, confidence = match
                return SongIdentification(
                    song_title=song_title,
                    artist=artist,
                    confidence=confidence,
                    identified_at=datetime.utcnow(),
                    station="fingerprint_scanner",
                    source="fingerprint",
                    fingerprint_match=True,
                    metadata={
                        "fingerprint_hash": fingerprint.hash[:16],
                        "duration": fingerprint.duration,
                        "sample_rate": fingerprint.sample_rate
                    }
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Fingerprint identification failed: {e}")
            return None
    
    async def _identify_acrcloud(self, audio_file: str) -> Optional[SongIdentification]:
        """Identify song using ACRCloud API (optional commercial service)."""
        try:
            # This would integrate with ACRCloud API
            # For now, return None as placeholder
            return None
        except Exception as e:
            logger.error(f"ACRCloud identification failed: {e}")
            return None
    
    async def _extract_metadata(self, audio_file: str) -> Optional[SongIdentification]:
        """Extract metadata from audio file or stream."""
        try:
            # This could extract ID3 tags or stream metadata
            # For now, return None as placeholder
            return None
        except Exception as e:
            logger.error(f"Metadata extraction failed: {e}")
            return None
    
    async def capture_audio_sample(self, station: TVStation, duration: int = 7) -> Optional[str]:
        """
        Capture audio sample from TV stream.
        
        Args:
            station: TV station to capture from
            duration: Sample duration in seconds
            
        Returns:
            Path to captured audio file or None if failed
        """
        output_file = self.data_dir / "audio_samples" / f"{station.name}_{int(time.time())}.wav"
        
        try:
            # Use ffmpeg to capture audio from stream
            # This is a simplified example - in production you'd need proper stream handling
            cmd = [
                'ffmpeg',
                '-y',  # Overwrite output
                '-t', str(duration),  # Duration
                '-i', station.stream_url,
                '-ac', '1',  # Mono audio
                '-ar', '22050',  # Sample rate
                '-acodec', 'pcm_s16le',  # Audio codec
                str(output_file)
            ]
            
            logger.debug(f"Capturing audio from {station.name}: {' '.join(cmd)}")
            
            # Run ffmpeg with timeout
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=duration + 10)
            
            if process.returncode == 0 and output_file.exists():
                file_size = output_file.stat().st_size
                if file_size > 1024:  # At least 1KB
                    logger.info(f"Captured {file_size:,} bytes from {station.name}")
                    return str(output_file)
                else:
                    logger.warning(f"Capture too small: {file_size} bytes")
                    output_file.unlink(missing_ok=True)
                    return None
            else:
                logger.error(f"FFmpeg failed for {station.name}: {stderr.decode()}")
                return None
                
        except asyncio.TimeoutError:
            logger.error(f"Capture timeout for {station.name}")
            return None
        except Exception as e:
            logger.error(f"Capture failed for {station.name}: {e}")
            return None
    
    async def scrape_station(self, station: TVStation) -> bool:
        """
        Scrape a single TV station.
        
        Args:
            station: TV station to scrape
            
        Returns:
            True if successful, False otherwise
        """
        async with self.semaphore:
            logger.info(f"Scraping station: {station.name}")
            
            try:
                # Capture audio sample
                audio_file = await self.capture_audio_sample(station)
                if not audio_file:
                    logger.warning(f"Failed to capture audio from {station.name}")
                    return False
                
                # Identify song
                identification = await self.identify_song(audio_file)
                
                # Clean up audio file (optional - could keep for debugging)
                try:
                    os.remove(audio_file)
                except:
                    pass
                
                if identification:
                    # Save identification
                    identification.station = station.name
                    await self._save_identification(identification)
                    
                    self.stats.songs_identified += 1
                    logger.info(f"Identified: {identification.artist} - {identification.song_title} "
                              f"on {station.name} (confidence: {identification.confidence:.1%})")
                    
                    # Send to UG Board API
                    await self._submit_to_api(identification)
                    
                    return True
                else:
                    logger.info(f"No song identified on {station.name}")
                    return False
                    
            except Exception as e:
                logger.error(f"Scrape failed for {station.name}: {e}")
                return False
            finally:
                station.last_scraped = datetime.utcnow()
                self.stats.total_scrapes += 1
    
    async def _save_identification(self, identification: SongIdentification):
        """Save identification to file (immutable storage)."""
        try:
            timestamp = identification.identified_at.strftime("%Y%m%d_%H%M%S")
            filename = f"{identification.station}_{timestamp}.json"
            filepath = self.data_dir / "identifications" / filename
            
            with open(filepath, 'w') as f:
                json.dump(identification.to_dict(), f, indent=2)
            
            logger.debug(f"Saved identification to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save identification: {e}")
    
    async def _submit_to_api(self, identification: SongIdentification):
        """Submit identification to UG Board API."""
        try:
            # This would make HTTP request to your UG Board API
            # For now, log it as placeholder
            logger.info(f"Would submit to API: {identification.artist} - {identification.song_title}")
            
            # Example implementation:
            # async with self.session.post(
            #     "https://your-api.com/ingestion/tv",
            #     json=identification.to_dict(),
            #     headers={"Authorization": f"Bearer {self.ingestion_token}"}
            # ) as response:
            #     if response.status == 200:
            #         logger.info("Successfully submitted to API")
            #     else:
            #         logger.error(f"API submission failed: {response.status}")
            
        except Exception as e:
            logger.error(f"API submission failed: {e}")
    
    async def run_scrape_cycle(self):
        """Run one complete scrape cycle across all enabled stations."""
        logger.info(f"Starting scrape cycle for {len(self.stations)} stations")
        
        enabled_stations = [s for s in self.stations if s.enabled]
        
        # Create tasks for all stations
        tasks = []
        for station in enabled_stations:
            task = asyncio.create_task(self.scrape_station(station))
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successes
        successful = sum(1 for r in results if r is True)
        self.stats.successful_scrapes += successful
        
        logger.info(f"Scrape cycle complete: {successful}/{len(enabled_stations)} successful")
        return successful
    
    async def start(self, continuous: bool = True):
        """Start the TV scraper engine."""
        logger.info("Starting TV Scraper Engine")
        self.running = True
        
        # Initialize HTTP session
        self.session = aiohttp.ClientSession()
        
        try:
            if continuous:
                # Continuous operation
                while self.running:
                    cycle_start = time.time()
                    
                    await self.run_scrape_cycle()
                    
                    # Update uptime
                    self.stats.uptime_seconds = (datetime.utcnow() - self.start_time).total_seconds()
                    self.stats.last_scrape_time = datetime.utcnow()
                    
                    # Save stats
                    await self._save_stats()
                    
                    # Wait for next cycle
                    cycle_duration = time.time() - cycle_start
                    sleep_time = max(0, self.scrape_interval - cycle_duration)
                    
                    logger.info(f"Cycle took {cycle_duration:.1f}s, sleeping {sleep_time:.1f}s")
                    
                    # Sleep in chunks to allow graceful shutdown
                    for _ in range(int(sleep_time)):
                        if not self.running:
                            break
                        await asyncio.sleep(1)
                        
            else:
                # Single cycle
                await self.run_scrape_cycle()
                
        except Exception as e:
            logger.error(f"Scraper engine failed: {e}", exc_info=True)
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the TV scraper engine gracefully."""
        logger.info("Stopping TV Scraper Engine")
        self.running = False
        
        if self.session:
            await self.session.close()
        
        # Save final stats
        await self._save_stats()
        
        logger.info("TV Scraper Engine stopped")
    
    async def _save_stats(self):
        """Save scraper statistics."""
        try:
            stats_file = self.data_dir / "state" / "scraper_stats.json"
            stats_dict = {
                "total_scrapes": self.stats.total_scrapes,
                "successful_scrapes": self.stats.successful_scrapes,
                "songs_identified": self.stats.songs_identified,
                "fingerprint_matches": self.stats.fingerprint_matches,
                "last_scrape_time": self.stats.last_scrape_time.isoformat() if self.stats.last_scrape_time else None,
                "uptime_seconds": self.stats.uptime_seconds,
                "start_time": self.start_time.isoformat(),
                "current_time": datetime.utcnow().isoformat()
            }
            
            with open(stats_file, 'w') as f:
                json.dump(stats_dict, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.running = False
    
    def store_reference_tracks(self, reference_dir: str = "data/reference_tracks"):
        """
        Store known Ugandan songs for fingerprint matching.
        Run this once to populate the fingerprint database.
        
        Args:
            reference_dir: Directory containing reference audio files
        """
        logger.info(f"Storing reference tracks from {reference_dir}")
        
        reference_path = Path(reference_dir)
        if not reference_path.exists():
            logger.error(f"Reference directory does not exist: {reference_dir}")
            return
        
        audio_files = list(reference_path.glob("**/*.mp3")) + \
                     list(reference_path.glob("**/*.wav"))
        
        if not audio_files:
            logger.warning(f"No audio files found in {reference_dir}")
            return
        
        logger.info(f"Found {len(audio_files)} audio files to process")
        
        processed = 0
        failed = 0
        
        for audio_file in audio_files:
            try:
                # Extract metadata from filename
                filename = audio_file.stem
                if " - " in filename:
                    artist, song_title = filename.split(" - ", 1)
                else:
                    # Try other separators
                    for sep in ["_-_", "_", "-"]:
                        if sep in filename:
                            artist, song_title = filename.split(sep, 1)
                            break
                    else:
                        artist, song_title = "Unknown", filename
                
                # Clean up names
                artist = artist.strip()
                song_title = song_title.strip()
                
                logger.debug(f"Processing: {artist} - {song_title}")
                
                # Extract fingerprint
                fingerprint = self.fingerprinter.extract_fingerprint(str(audio_file))
                if fingerprint:
                    # Store in database
                    success = self.fingerprinter.store_fingerprint(
                        fingerprint, song_title, artist
                    )
                    
                    if success:
                        processed += 1
                        logger.info(f"Stored reference: {artist} - {song_title}")
                    else:
                        failed += 1
                        logger.warning(f"Failed to store: {artist} - {song_title}")
                else:
                    failed += 1
                    logger.warning(f"Fingerprint extraction failed: {audio_file}")
                    
            except Exception as e:
                failed += 1
                logger.error(f"Failed to process {audio_file}: {e}")
        
        logger.info(f"Reference tracks processing complete: {processed} succeeded, {failed} failed")


# Command-line interface
async def main():
    """Main entry point for TV scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description="UG Board TV Scraper Engine")
    parser.add_argument("--config", default="config/tv_stations.yaml", help="TV stations config file")
    parser.add_argument("--data-dir", default="data/tv_scraper", help="Data directory")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument("--store-references", help="Directory containing reference tracks to store")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Max concurrent scrapes")
    parser.add_argument("--interval", type=int, default=1800, help="Scrape interval in seconds")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("data/tv_scraper/logs/scraper.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize engine
    engine = TVScraperEngine(
        config_path=args.config,
        data_dir=args.data_dir,
        max_concurrent=args.max_concurrent,
        scrape_interval=args.interval
    )
    
    try:
        if args.store_references:
            # Store reference tracks mode
            engine.store_reference_tracks(args.store_references)
        else:
            # Run scraper mode
            await engine.start(continuous=args.continuous)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        await engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
