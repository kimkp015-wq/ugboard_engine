# src/infrastructure/external/radio_scraper.py
import asyncio
import aiohttp
import backoff
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import json
import logging
from enum import Enum
from urllib.parse import urlparse

from src.config.settings import settings
from src.domain.services.music_rules import MusicRulesService


logger = logging.getLogger(__name__)


class StationType(str, Enum):
    ICECAST = "icecast"
    RADIO_CO = "radio_co"
    WEB = "web"
    SHOUTCAST = "shoutcast"


@dataclass
class Station:
    name: str
    url: str
    type: StationType
    region: str
    city: str
    frequency: str
    weight: int = 1  # Priority weight
    timeout: int = 8
    enabled: bool = True


class RadioScraperError(Exception):
    """Base exception for radio scraping errors"""
    pass


class StationTimeoutError(RadioScraperError):
    """Station timeout error"""
    pass


class MetadataParseError(RadioScraperError):
    """Failed to parse metadata"""
    pass


class EnhancedRadioScraper:
    """Production-ready radio scraper with async operations"""
    
    def __init__(self, music_rules: MusicRulesService):
        self.music_rules = music_rules
        self.session: Optional[aiohttp.ClientSession] = None
        self.stations: List[Station] = []
        self._load_stations()
    
    def _load_stations(self):
        """Load stations from configuration"""
        try:
            with open(settings.scraper.station_config_path, 'r') as f:
                stations_data = json.load(f)
            
            self.stations = [
                Station(
                    name=s["name"],
                    url=s["url"],
                    type=StationType(s["type"]),
                    region=s["region"],
                    city=s["city"],
                    frequency=s.get("frequency", ""),
                    weight=s.get("weight", 1),
                    timeout=s.get("timeout", 8),
                    enabled=s.get("enabled", True)
                )
                for s in stations_data["stations"]
                if s.get("enabled", True)
            ]
            
            logger.info(f"Loaded {len(self.stations)} stations from config")
            
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load stations config: {e}, using defaults")
            self._load_default_stations()
    
    def _load_default_stations(self):
        """Load default Ugandan stations"""
        self.stations = [
            Station(
                name="Capital FM",
                url="https://ice.capitalradio.co.ug/capital_live",
                type=StationType.ICECAST,
                region="Central",
                city="Kampala",
                frequency="91.3",
                weight=10
            ),
            Station(
                name="Radio Simba",
                url="https://stream.radiosimba.ug/live",
                type=StationType.ICECAST,
                region="Central",
                city="Kampala",
                frequency="97.3",
                weight=9
            ),
            # Add more default stations...
        ]
    
    async def __aenter__(self):
        """Async context manager entry"""
        timeout = aiohttp.ClientTimeout(total=settings.scraper.request_timeout)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": settings.scraper.user_agent}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=settings.scraper.retry_attempts,
        max_time=30
    )
    async def scrape_station(self, station: Station) -> Optional[Dict]:
        """Scrape a single station with retry logic"""
        if not station.enabled:
            return None
        
        try:
            if station.type == StationType.ICECAST:
                return await self._scrape_icecast(station)
            elif station.type == StationType.RADIO_CO:
                return await self._scrape_radio_co(station)
            elif station.type == StationType.SHOUTCAST:
                return await self._scrape_shoutcast(station)
            else:
                return await self._scrape_web(station)
                
        except asyncio.TimeoutError:
            logger.warning(f"Station timeout: {station.name}")
            raise StationTimeoutError(f"Timeout connecting to {station.name}")
        except Exception as e:
            logger.error(f"Failed to scrape {station.name}: {str(e)[:100]}")
            return None
    
    async def _scrape_icecast(self, station: Station) -> Optional[Dict]:
        """Scrape Icecast stream metadata"""
        headers = {"Icy-MetaData": "1"}
        
        async with self.session.get(station.url, headers=headers) as response:
            if response.status != 200:
                logger.warning(f"{station.name}: HTTP {response.status}")
                return None
            
            # Check for metadata support
            metaint_header = response.headers.get("icy-metaint")
            if not metaint_header:
                logger.debug(f"{station.name}: No metadata support")
                return None
            
            metaint = int(metaint_header)
            
            # Read audio data up to metadata block
            reader = response.content
            await reader.readexactly(metaint)
            
            # Read metadata length byte
            meta_length_byte = await reader.readexactly(1)
            meta_length = ord(meta_length_byte) * 16
            
            if meta_length == 0:
                return None
            
            # Read metadata
            metadata_bytes = await reader.readexactly(meta_length)
            metadata = metadata_bytes.decode("utf-8", errors="ignore")
            
            # Parse metadata
            song_data = self._parse_icecast_metadata(metadata, station)
            if song_data:
                # Validate with Ugandan music rules
                is_valid = self._validate_song(song_data)
                if is_valid:
                    return song_data
            
            return None
    
    def _parse_icecast_metadata(self, metadata: str, station: Station) -> Optional[Dict]:
        """Parse Icecast metadata into structured song data"""
        import re
        
        # Extract StreamTitle
        match = re.search(r"StreamTitle='(.*?)';", metadata)
        if not match:
            return None
        
        full_title = match.group(1).strip()
        if not full_title or full_title.lower() in ["", "none", "null"]:
            return None
        
        # Parse artist and title
        artist, title = self._parse_artist_title(full_title)
        
        # Extract additional metadata
        bitrate_match = re.search(r"icy-br:'?(\d+)'?", metadata)
        genre_match = re.search(r"icy-genre:'?([^';]+)'?", metadata)
        
        return {
            "station": station.name,
            "frequency": station.frequency,
            "artist": artist,
            "title": title,
            "raw_title": full_title,
            "bitrate": bitrate_match.group(1) if bitrate_match else None,
            "genre": genre_match.group(1) if genre_match else None,
            "timestamp": datetime.utcnow().isoformat(),
            "region": station.region,
            "city": station.city,
            "source_url": station.url,
            "source_type": station.type.value,
            "metadata_format": "icecast"
        }
    
    def _parse_artist_title(self, full_title: str) -> Tuple[str, str]:
        """Parse artist and title from various formats"""
        # Remove common prefixes
        prefixes = [
            "Now Playing:",
            "Current:",
            "Playing:",
            "On Air:",
            "▶",
            "●"
        ]
        
        for prefix in prefixes:
            if full_title.startswith(prefix):
                full_title = full_title[len(prefix):].strip()
        
        # Try different separator patterns
        patterns = [
            (r"^(.*?)\s*[-~–—]\s*(.*)$", (0, 1)),  # Artist - Title
            (r"^(.*?)\s*[|‖]\s*(.*)$", (0, 1)),     # Artist | Title
            (r"^(.*?)\s*[:：]\s*(.*)$", (0, 1)),     # Artist : Title
            (r"^(.*)\s+by\s+(.*)$", (1, 0)),        # Title by Artist
            (r"^(.*?)\s*\(ft\.\s*(.*)\)\s*[-~]\s*(.*)$", (0, 2)),  # Artist (ft. X) - Title
        ]
        
        for pattern, indices in patterns:
            match = re.match(pattern, full_title, re.IGNORECASE)
            if match:
                artist_idx, title_idx = indices
                artist = match.group(artist_idx + 1).strip()
                title = match.group(title_idx + 1).strip()
                
                # Clean up
                artist = self._clean_text(artist)
                title = self._clean_text(title)
                
                if artist and title:
                    return artist, title
        
        # If no pattern matches, assume it's just a title
        return "Unknown Artist", self._clean_text(full_title)
    
    def _clean_text(self, text: str) -> str:
        """Clean text of common artifacts"""
        artifacts = [
            "(Official Audio)",
            "(Official Video)",
            "(Official Music Video)",
            "[Official Audio]",
            "[Official Video]",
            "(Audio)",
            "(Video)",
            "(Lyrics)",
            "(Visualizer)",
            "()",
            "[]"
        ]
        
        for artifact in artifacts:
            text = text.replace(artifact, "")
        
        return text.strip()
    
    def _validate_song(self, song_data: Dict) -> bool:
        """Validate song against Ugandan music rules"""
        artist = song_data.get("artist", "")
        
        # Extract artists list
        artists = self.music_rules.extract_artist_list(artist)
        
        # Validate
        is_valid, error = self.music_rules.validate_artists(artists)
        
        if not is_valid:
            logger.debug(f"Song validation failed: {error}")
            return False
        
        return True
    
    async def scrape_all(self) -> List[Dict]:
        """Scrape all enabled stations with concurrency control"""
        if not self.session:
            async with self as scraper:
                return await scraper._scrape_all_internal()
        return await self._scrape_all_internal()
    
    async def _scrape_all_internal(self) -> List[Dict]:
        """Internal method for scraping all stations"""
        logger.info(f"Starting scrape of {len(self.stations)} stations")
        
        # Sort stations by weight (priority)
        sorted_stations = sorted(
            self.stations,
            key=lambda s: s.weight,
            reverse=True
        )
        
        # Scrape in batches to control concurrency
        batch_size = settings.scraper.max_concurrent_stations
        results = []
        
        for i in range(0, len(sorted_stations), batch_size):
            batch = sorted_stations[i:i + batch_size]
            
            # Create scraping tasks
            tasks = [self.scrape_station(station) for station in batch]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for station, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.warning(f"Station {station.name} failed: {result}")
                elif result:
                    results.append(result)
            
            # Rate limiting between batches
            if i + batch_size < len(sorted_stations):
                await asyncio.sleep(1)
        
        logger.info(f"Scrape complete: {len(results)} songs found")
        return results


# Factory function for dependency injection
async def get_radio_scraper() -> EnhancedRadioScraper:
    """Dependency injection for radio scraper"""
    from src.domain.services.music_rules import get_music_rules_service
    music_rules = await get_music_rules_service()
    
    async with EnhancedRadioScraper(music_rules) as scraper:
        yield scraper
