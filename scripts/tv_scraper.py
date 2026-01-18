"""
tv_scraper.py - Hybrid Scraper for UG Board Engine
Scrapes schedules and uses audio fingerprinting for Ugandan TV music data.
"""

import asyncio
import aiohttp
import subprocess
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import hashlib
import sys
import os

# Add parent directory to path to import UG Board Engine modules if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TvStation:
    """Data class for TV station configuration"""
    name: str
    # Primary: URL for schedule/scraping
    schedule_url: Optional[str] = None
    # Secondary: Direct stream URL for audio fingerprinting (.m3u8 preferred)
    stream_url: Optional[str] = None
    type: str = "general"  # general, music, entertainment
    weight: int = 1  # Priority

class HybridTvScraper:
    """Main scraper implementing the hybrid strategy"""
    
    def __init__(self):
        # Initialize with verified and fallback stations
        self.stations = self._initialize_stations()
        self.session: Optional[aiohttp.ClientSession] = None
        # In-memory cache for schedules
        self.schedule_cache = {}
        
    def _initialize_stations(self) -> List[TvStation]:
        """Initialize station list with verified and backup sources"""
        return [
            # Tier 1: Stations with confirmed working pages/schedules
            TvStation(
                name="BBS Terefayina",
                schedule_url="https://bbstv.ug/live",  # Has detailed schedule
                stream_url=None,  # To be discovered
                type="general",
                weight=10
            ),
            TvStation(
                name="NTV Uganda",
                schedule_url="https://ntv.co.ug/tv-schedule",  # Schedule page
                stream_url="https://ntv.co.ug/live",  # Live page
                type="news",
                weight=9
            ),
            TvStation(
                name="Bukedde TV 1",
                schedule_url="https://newvision.co.ug/tv/4",  # ViDE portal
                stream_url=None,
                type="regional",
                weight=8
            ),
            # Tier 2: Stations for audio fingerprinting if stream is found
            TvStation(
                name="Baba TV",
                schedule_url="https://babatv.co.ug/live",
                stream_url=None,
                type="regional",
                weight=7
            ),
            # Tier 3: Fallback - Target aggregator pages if direct fails
            TvStation(
                name="Spark TV",
                schedule_url="https://ntv.co.ug/sparktv/live",
                stream_url=None,
                type="entertainment",
                weight=6
            ),
        ]
    
    async def scrape_schedule(self, station: TvStation) -> List[Dict]:
        """Scrape program schedule from station website"""
        if not station.schedule_url:
            return []
            
        cache_key = f"{station.name}_{datetime.now().strftime('%Y-%m-%d')}"
        if cache_key in self.schedule_cache:
            return self.schedule_cache[cache_key]
        
        try:
            async with self.session.get(station.schedule_url) as response:
                if response.status != 200:
                    logger.warning(f"Schedule HTTP {response.status} for {station.name}")
                    return []
                
                html = await response.text()
                
                # Parse schedule based on known station structures
                if "bbstv.ug" in station.schedule_url:
                    return self._parse_bbs_schedule(html, station)
                elif "ntv.co.ug" in station.schedule_url:
                    return self._parse_ntv_schedule(html, station)
                else:
                    # Generic fallback - look for time and program patterns
                    return self._parse_generic_schedule(html, station)
                    
        except Exception as e:
            logger.error(f"Failed to scrape schedule for {station.name}: {e}")
            return []
    
    def _parse_bbs_schedule(self, html: str, station: TvStation) -> List[Dict]:
        """Parse BBS Terefayina's detailed schedule table"""
        import re
        programs = []
        
        # Look for schedule table patterns (adapt based on actual HTML)
        schedule_pattern = r'(\d{2}:\d{2})\s*–\s*(\d{2}:\d{2})\s*\|\s*(.+?)(?=\n|$)'
        matches = re.findall(schedule_pattern, html, re.MULTILINE)
        
        for start_time, end_time, program_name in matches:
            if any(keyword in program_name.lower() for keyword in ['music', 'video', 'hit', 'song']):
                programs.append({
                    'station': station.name,
                    'program': program_name.strip(),
                    'start_time': start_time.strip(),
                    'end_time': end_time.strip(),
                    'type': 'music_block',
                    'date': datetime.now().strftime('%Y-%m-%d')
                })
        
        logger.info(f"Parsed {len(programs)} music programs from {station.name}")
        return programs
    
    def _parse_generic_schedule(self, html: str, station: TvStation) -> List[Dict]:
        """Generic parser for schedule pages"""
        import re
        programs = []
        
        # Common Ugandan music show patterns
        music_keywords = ['miziki', 'music', 'video', 'hit', 'mix', 'chart', 'top']
        time_pattern = r'(\d{1,2}[:.]\d{2})\s*(?:[AP]M)?'
        
        # Simple line-by-line analysis
        lines = html.split('\n')
        for i, line in enumerate(lines[:100]):  # Check first 100 lines
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in music_keywords):
                # Look for time in this or nearby lines
                for check_line in lines[max(0, i-2):min(len(lines), i+3)]:
                    time_match = re.search(time_pattern, check_line)
                    if time_match:
                        programs.append({
                            'station': station.name,
                            'program': line.strip()[:100],
                            'time': time_match.group(1),
                            'type': 'music_suspected',
                            'date': datetime.now().strftime('%Y-%m-%d')
                        })
                        break
        
        return programs
    
    async def capture_and_identify(self, station: TvStation, duration_seconds: int = 15) -> Optional[Dict]:
        """Capture audio sample and identify song using ACRCloud"""
        if not station.stream_url:
            logger.debug(f"No stream URL for {station.name}")
            return None
        
        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"audio_captures/{station.name.replace(' ', '_')}_{timestamp}.mp3"
        os.makedirs("audio_captures", exist_ok=True)
        
        try:
            # Use ffmpeg to capture audio sample
            # Note: Stream URL should be .m3u8 playlist. You'll need to discover this.
            cmd = [
                'ffmpeg',
                '-i', station.stream_url,
                '-t', str(duration_seconds),
                '-vn', '-acodec', 'libmp3lame',
                '-ac', '1', '-ar', '44100',
                '-y', filename
            ]
            
            # Run ffmpeg (timeout after duration + 5 seconds)
            result = subprocess.run(cmd, capture_output=True, timeout=duration_seconds + 5)
            
            if result.returncode == 0 and os.path.exists(filename):
                # Here you would send to ACRCloud API
                # For now, we'll simulate response
                song_data = await self._identify_with_acrcloud(filename)
                
                # Clean up file
                os.remove(filename)
                
                return song_data
                
        except subprocess.TimeoutExpired:
            logger.warning(f"Audio capture timeout for {station.name}")
        except Exception as e:
            logger.error(f"Audio capture failed for {station.name}: {e}")
        
        return None
    
    async def _identify_with_acrcloud(self, audio_file: str) -> Dict:
        """Identify song using ACRCloud API"""
        # This is a placeholder for ACRCloud integration
        # You'll need to sign up at https://www.acrcloud.com/
        # and use their Python SDK
        
        # Simulated response structure
        return {
            'status': 'success',
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'song_title': 'Simulated Song',
                'artist': 'Simulated Artist',
                'album': 'Simulated Album',
                'duration': 180,
                'confidence': 85.5
            }
        }
    
    def prepare_ugboard_payload(self, song_data: Dict, station: TvStation) -> Dict:
        """Prepare data for UG Board Engine ingestion API"""
        # Align with your existing /ingest/tv endpoint structure
        return {
            "items": [{
                "title": song_data['metadata']['song_title'],
                "artist": song_data['metadata']['artist'],
                "channel": station.name,
                "plays": 1,  # Default
                "score": song_data['metadata'].get('confidence', 50),
                "metadata": {
                    "source": "tv_scraper",
                    "identification_method": "audio_fingerprinting",
                    "station_type": station.type,
                    "confidence": song_data['metadata']['confidence'],
                    "timestamp": song_data['metadata']['timestamp']
                }
            }],
            "source": "tv_scraper",
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "scraper_version": "2.0",
                "station": station.name,
                "identification_engine": "acrcloud"
            }
        }
    
    async def send_to_ugboard(self, payload: Dict) -> bool:
        """Send data to UG Board Engine API"""
        api_url = os.getenv("UGBOARD_API_URL", "http://localhost:8000")
        internal_token = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
        
        try:
            async with self.session.post(
                f"{api_url}/ingest/tv",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": internal_token
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"Successfully sent data: {result.get('message')}")
                    return True
                else:
                    logger.error(f"API error {response.status}: {await response.text()}")
                    return False
        except Exception as e:
            logger.error(f"Failed to send to UG Board: {e}")
            return False
    
    async def run_scraping_cycle(self):
        """Run one complete scraping cycle"""
        logger.info("Starting TV scraping cycle")
        
        all_results = []
        
        # Step 1: Get schedules for all stations
        for station in self.stations:
            schedule = await self.scrape_schedule(station)
            
            # Step 2: If music programs found, capture audio during those times
            current_hour = datetime.now().hour
            for program in schedule:
                if program['type'] == 'music_block':
                    # Check if current time is within program time
                    # (Simplified - in production, parse times properly)
                    logger.info(f"Music program found on {station.name}: {program['program']}")
                    
                    # Capture audio sample
                    song_data = await self.capture_and_identify(station)
                    if song_data and song_data['status'] == 'success':
                        # Prepare and send to UG Board
                        payload = self.prepare_ugboard_payload(song_data, station)
                        await self.send_to_ugboard(payload)
                        all_results.append(song_data)
        
        # Step 3: If no schedule-based hits, do random sampling on high-weight stations
        if not all_results:
            logger.info("No schedule hits, performing random sampling")
            for station in [s for s in self.stations if s.weight >= 8]:
                song_data = await self.capture_and_identify(station)
                if song_data and song_data['status'] == 'success':
                    payload = self.prepare_ugboard_payload(song_data, station)
                    await self.send_to_ugboard(payload)
                    all_results.append(song_data)
                    break  # One success is enough for this cycle
        
        logger.info(f"Cycle complete. Found {len(all_results)} songs.")
        return all_results
    
    async def start_continuous_monitoring(self, interval_minutes: int = 30):
        """Run scraper continuously at specified interval"""
        logger.info(f"Starting continuous monitoring (interval: {interval_minutes} min)")
        
        while True:
            try:
                await self.run_scraping_cycle()
                logger.info(f"Sleeping for {interval_minutes} minutes...")
                await asyncio.sleep(interval_minutes * 60)
            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Monitoring cycle failed: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retry

async def main():
    """Main entry point"""
    print("=" * 60)
    print("📺 UGANDAN TV MUSIC SCRAPER v2.0")
    print("Hybrid Schedule + Audio Fingerprinting")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    scraper = HybridTvScraper()
    
    async with aiohttp.ClientSession() as session:
        scraper.session = session
        
        # Run in continuous mode (for production)
        # await scraper.start_continuous_monitoring(interval_minutes=30)
        
        # Or run once (for testing)
        results = await scraper.run_scraping_cycle()
        
        if results:
            print(f"✅ Found {len(results)} songs this cycle")
            for result in results[:3]:  # Show first 3
                meta = result['metadata']
                print(f"   🎵 {meta.get('artist', 'Unknown')} - {meta.get('song_title', 'Unknown')}")
        else:
            print("ℹ️ No songs found this cycle")
    
    print("=" * 60)
    print("🎉 TV scraping completed!")

if __name__ == "__main__":
    # For async execution
    asyncio.run(main())
