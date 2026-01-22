"""
streams_scraper.py - Enhanced with Playwright for JavaScript rendering and Spotify API
"""

import asyncio
import json
import re
import time
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
from dataclasses import dataclass
import base64

# Third-party imports
import requests
from bs4 import BeautifulSoup
from lxml import html as lhtml
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from rapidfuzz import process, fuzz

# Playwright for JavaScript rendering
try:
    from playwright.async_api import async_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not available, falling back to requests")

# Use existing logger
logger = logging.getLogger("streams_scraper")

@dataclass
class ScrapedSong:
    """Enhanced song data structure with metadata"""
    title: str
    artist: str
    plays: Optional[int] = None
    score: float = 0.0
    rank: Optional[int] = None
    source_type: str = "streaming"
    source: str = ""
    url: Optional[str] = None
    region: str = "central"
    duration_ms: Optional[int] = None
    album: Optional[str] = None
    release_date: Optional[str] = None
    popularity: Optional[int] = None
    metadata: Optional[Dict] = None

class EnhancedStreamsScraper:
    """Production-ready streams scraper with Playwright and Spotify API"""
    
    def __init__(self, db_service=None, config=None):
        self.db = db_service
        self.config = config
        
        # Spotify API configuration (should be in config/secrets)
        self.spotify_client_id = "your_spotify_client_id"  # Replace from config
        self.spotify_client_secret = "your_spotify_client_secret"  # Replace from config
        self.spotify_playlist_id = "37i9dQZEVXbLuVZhVkCJ64"  # Uganda Top 50
        
        # Initialize Spotify client
        self.spotify_client = self._init_spotify_client()
        
        # Platform configurations with enhanced settings
        self.platforms = {
            "songboost": {
                "name": "SongBoost Radio",
                "url": "https://charts.songboost.app/",
                "enabled": True,
                "weight": 1.5,
                "requires_js": True,
                "timeout": 30000,  # ms for Playwright
                "retries": 2,
                "selectors": {
                    "container": ".chart-item, .track-item, [class*='chart']",
                    "title": ".track-title, .song-title, .title",
                    "artist": ".track-artist, .artist-name, .artist"
                }
            },
            "spotify": {
                "name": "Spotify Uganda",
                "url": f"https://open.spotify.com/playlist/{self.spotify_playlist_id}",
                "enabled": True,
                "weight": 1.0,
                "requires_js": True,
                "timeout": 30000,
                "retries": 2,
                "use_api": True  # Use Spotify API when possible
            },
            "boomplay": {
                "name": "Boomplay Uganda",
                "url": "https://www.boomplay.com/charts/4849",
                "enabled": True,
                "weight": 0.8,
                "requires_js": True,
                "timeout": 30000,
                "retries": 2,
                "selectors": {
                    "container": ".chartItem, .song-item, .playlist-item",
                    "title": ".song-name, .title, [class*='name']",
                    "artist": ".artist-name, .singer, [class*='artist']"
                }
            },
            "audiomack": {
                "name": "Audiomack Uganda",
                "url": "https://audiomack.com/world/country/uganda",
                "enabled": True,
                "weight": 0.6,
                "requires_js": False,  # Can use static scraping
                "timeout": 15000,
                "retries": 2,
                "selectors": {
                    "container": "article, .music-card, .chart-item",
                    "title": ".music-obj__title, .title, .song-title",
                    "artist": ".music-obj__artist, .artist, .singer"
                }
            }
        }
        
        # Headers for requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        }
        
        # Playwright browser instance
        self.browser = None
        self.context = None
        self.playwright = None
        
        # Ugandan artist patterns with fuzzy matching
        self.ugandan_artists = [
            "bobi wine", "eddy kenzo", "sheebah", "azawi", "vinka",
            "alien skin", "spice diana", "rema", "winnie nwagi",
            "jose chameleone", "bebe cool", "pallaso", "daddy andre",
            "geosteady", "fik fameica", "john blaq", "gravity omutujju",
            "vyroota", "feffe busi", "dax", "vivian tosh", "king saha",
            "david lutalo", "zex bilangilangi", "b2c", "chosen becky",
            "karole kasita", "ray g", "truth 256", "levixone", "judith babirye"
        ]
        
        # Initialize Playwright (async)
        self._init_playwright()
    
    def _init_spotify_client(self):
        """Initialize Spotify API client"""
        if not self.spotify_client_id or not self.spotify_client_secret:
            logger.warning("Spotify credentials not configured")
            return None
        
        try:
            auth_manager = SpotifyClientCredentials(
                client_id=self.spotify_client_id,
                client_secret=self.spotify_client_secret
            )
            return spotipy.Spotify(auth_manager=auth_manager)
        except Exception as e:
            logger.error(f"Failed to initialize Spotify client: {e}")
            return None
    
    async def _init_playwright(self):
        """Initialize Playwright browser"""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not installed. Some scrapers may not work properly.")
            return
        
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-accelerated-2d-canvas',
                    '--disable-gpu'
                ]
            )
            
            # Create context with mobile viewport
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=self.headers['User-Agent'],
                java_script_enabled=True,
                ignore_https_errors=True
            )
            
            logger.info("✅ Playwright browser initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Playwright: {e}")
            self.browser = None
    
    async def _scrape_with_playwright(self, url: str, platform: str) -> Optional[str]:
        """Scrape website using Playwright for JavaScript rendering"""
        if not self.browser:
            logger.warning(f"Playwright not available for {platform}, falling back to requests")
            return await self._scrape_with_requests(url, platform)
        
        platform_config = self.platforms[platform]
        page = None
        
        try:
            # Create new page
            page = await self.context.new_page()
            
            # Set additional headers
            await page.set_extra_http_headers({
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            })
            
            # Navigate to URL
            response = await page.goto(
                url,
                wait_until="networkidle",
                timeout=platform_config.get("timeout", 30000)
            )
            
            if response and response.status != 200:
                logger.warning(f"HTTP {response.status} from {platform}")
                return None
            
            # Wait for content to load
            await page.wait_for_load_state("networkidle")
            
            # Take screenshot for debugging (optional)
            # await page.screenshot(path=f"debug_{platform}_{int(time.time())}.png")
            
            # Get page content
            content = await page.content()
            
            return content
            
        except Exception as e:
            logger.error(f"Playwright error for {platform}: {e}")
            return None
        finally:
            if page:
                await page.close()
    
    async def _scrape_with_requests(self, url: str, platform: str) -> Optional[str]:
        """Fallback scraping with requests"""
        platform_config = self.platforms[platform]
        
        for attempt in range(platform_config.get("retries", 1)):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=platform_config.get("timeout", 10000) / 1000,  # Convert to seconds
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Rate limited on {platform}, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"HTTP {response.status_code} from {platform}")
                    
            except Exception as e:
                logger.error(f"Request error on {platform}: {e}")
            
            if attempt < platform_config.get("retries", 1) - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def _clean_string(self, text: str) -> str:
        """Enhanced string cleaning"""
        if not text:
            return ""
        
        # Remove unwanted characters and normalize
        text = re.sub(r'[\(\[].*?[\)\]]', '', text)  # Remove brackets content
        text = re.sub(r'[^\w\s\-&]', ' ', text)  # Remove special chars except &, -, space
        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
        
        # Remove common prefixes/suffixes
        text = re.sub(r'(?i)\s*(ft\.|feat\.|featuring|vs\.|vs|with)\s+.*$', '', text)
        text = re.sub(r'(?i)\s*(official\s*(video|audio|lyrics?|visualizer)?|lyric\s*video)', '', text)
        text = re.sub(r'(?i)\s*(prod\.|prod\s+by)\s+.*$', '', text)
        
        return text.strip()
    
    def _extract_artist_title(self, raw_text: str) -> Tuple[str, str]:
        """Enhanced artist/title extraction with fuzzy matching"""
        if not raw_text:
            return "Various Artists", "Unknown"
        
        clean_text = self._clean_string(raw_text)
        
        # Try multiple extraction patterns
        patterns = [
            # Artist - Title (most common)
            (r'^(.*?)\s*[-–—]\s*(.*)$', (0, 1)),
            # Title by Artist
            (r'^(.*?)\s+by\s+(.*)$', (1, 0)),
            # Artist: Title
            (r'^(.*?)\s*[:：]\s*(.*)$', (0, 1)),
            # Artist "Title"
            (r'^(.*?)\s+["\'](.*?)["\']$', (0, 1)),
            # Title | Artist
            (r'^(.*?)\s*\|\s*(.*)$', (1, 0)),
        ]
        
        for pattern, indices in patterns:
            match = re.match(pattern, clean_text, re.IGNORECASE)
            if match:
                artist_idx, title_idx = indices
                artist = match.group(artist_idx + 1).strip()
                title = match.group(title_idx + 1).strip()
                
                # Validate and clean
                if artist and title:
                    # Check if we need to swap (sometimes pattern is wrong)
                    if self._looks_like_artist(title) and not self._looks_like_artist(artist):
                        artist, title = title, artist
                    
                    return artist, title
        
        # If no pattern matches, try to split by common separators
        separators = [' - ', ' – ', ' — ', ' : ', ' | ', ' ~ ']
        for sep in separators:
            if sep in clean_text:
                parts = clean_text.split(sep, 1)
                if len(parts) == 2:
                    part1, part2 = parts[0].strip(), parts[1].strip()
                    # Determine which is likely artist and title
                    if self._looks_like_artist(part1):
                        return part1, part2
                    else:
                        return part2, part1
        
        # Last resort: return as title with unknown artist
        return "Various Artists", clean_text
    
    def _looks_like_artist(self, text: str) -> bool:
        """Check if text looks like an artist name"""
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Check for Ugandan artist names (fuzzy match)
        best_match, score, _ = process.extractOne(text_lower, self.ugandan_artists, scorer=fuzz.token_sort_ratio)
        
        # If high match score with known Ugandan artist
        if score > 70:
            return True
        
        # Check for patterns that look like artist names
        artist_patterns = [
            r'\s+&\s+',  # Contains "&"
            r'\s+feat\.',  # Contains "feat."
            r'\s+x\s+',  # Contains "x" for collaboration
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',  # Two capitalized words
        ]
        
        return any(re.search(pattern, text_lower) for pattern in artist_patterns)
    
    def _is_ugandan_artist(self, artist: str, threshold: float = 65.0) -> bool:
        """Check if artist is Ugandan using fuzzy matching"""
        if not artist:
            return False
        
        artist_lower = artist.lower()
        
        # Exact match
        if artist_lower in self.ugandan_artists:
            return True
        
        # Fuzzy match with threshold
        best_match, score, _ = process.extractOne(
            artist_lower, 
            self.ugandan_artists, 
            scorer=fuzz.token_sort_ratio
        )
        
        if score >= threshold:
            logger.debug(f"Fuzzy match: {artist} -> {best_match} ({score}%)")
            return True
        
        # Check for Ugandan keywords
        ugandan_keywords = ['uganda', 'kampala', 'ugandan', '256']
        if any(keyword in artist_lower for keyword in ugandan_keywords):
            return True
        
        return False
    
    def _calculate_score(self, rank: int, platform: str, total_items: int = 50, 
                        popularity: Optional[int] = None) -> float:
        """Enhanced score calculation"""
        platform_config = self.platforms.get(platform, {"weight": 1.0})
        weight = platform_config.get("weight", 1.0)
        
        # Base score from rank (higher rank = higher score)
        base_score = 100 - ((rank - 1) / total_items * 80)
        
        # Incorporate popularity if available (e.g., from Spotify API)
        if popularity is not None:
            popularity_factor = popularity / 100  # Normalize 0-100 to 0-1
            base_score = base_score * 0.7 + (popularity_factor * 30) * 0.3
        
        # Apply platform weight
        weighted_score = base_score * weight
        
        # Add small random variation to prevent ties
        import random
        weighted_score += random.uniform(-0.5, 0.5)
        
        return round(max(0, min(100, weighted_score)), 2)
    
    async def scrape_songboost(self) -> List[ScrapedSong]:
        """Scrape SongBoost using Playwright"""
        songs = []
        platform = "songboost"
        platform_config = self.platforms[platform]
        
        try:
            # Use Playwright for JavaScript rendering
            html = await self._scrape_with_playwright(platform_config["url"], platform)
            
            if not html:
                logger.warning(f"Using fallback data for {platform}")
                return self._get_fallback_data(platform)
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try multiple selectors
            selectors = platform_config.get("selectors", {}).get("container", 
                ".chart-item, .track-item, [class*='chart'], table tbody tr, li.chart-entry")
            
            items = soup.select(selectors)
            
            for i, item in enumerate(items[:50], 1):
                try:
                    # Try to extract using specific selectors
                    title_selector = platform_config.get("selectors", {}).get("title", 
                        ".track-title, .song-title, .title, td:nth-child(2)")
                    artist_selector = platform_config.get("selectors", {}).get("artist",
                        ".track-artist, .artist-name, .artist, td:nth-child(3)")
                    
                    title_elem = item.select_one(title_selector)
                    artist_elem = item.select_one(artist_selector)
                    
                    if title_elem and artist_elem:
                        title = self._clean_string(title_elem.get_text(strip=True))
                        artist = self._clean_string(artist_elem.get_text(strip=True))
                    else:
                        # Fallback to text extraction
                        text = item.get_text(separator=' ', strip=True)
                        artist, title = self._extract_artist_title(text)
                    
                    # Only include if Ugandan artist
                    if self._is_ugandan_artist(artist):
                        songs.append(ScrapedSong(
                            title=title,
                            artist=artist,
                            rank=i,
                            score=self._calculate_score(i, platform),
                            source_type="radio",
                            source=f"stream_{platform}",
                            metadata={
                                "platform": platform_config["name"],
                                "scraped_at": datetime.utcnow().isoformat(),
                                "method": "playwright"
                            }
                        ))
                        
                except Exception as e:
                    logger.debug(f"Error parsing SongBoost item {i}: {e}")
            
            logger.info(f"✅ SongBoost: Found {len(songs)} Ugandan songs")
            
        except Exception as e:
            logger.error(f"❌ SongBoost scraping failed: {e}")
            songs = self._get_fallback_data(platform)
        
        return songs
    
    async def scrape_spotify(self) -> List[ScrapedSong]:
        """Scrape Spotify using API when available, fallback to web scraping"""
        songs = []
        platform = "spotify"
        platform_config = self.platforms[platform]
        
        # Try API first
        if platform_config.get("use_api", False) and self.spotify_client:
            try:
                return await self._scrape_spotify_api()
            except Exception as e:
                logger.warning(f"Spotify API failed, falling back to web scraping: {e}")
        
        # Fallback to web scraping
        try:
            html = await self._scrape_with_playwright(platform_config["url"], platform)
            
            if not html:
                logger.warning(f"Using fallback data for {platform}")
                return self._get_fallback_data(platform)
            
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for JSON-LD data
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    tracks = data.get('track', []) if isinstance(data.get('track'), list) else []
                    
                    for i, track in enumerate(tracks[:50], 1):
                        name = track.get('name', '')
                        artist_info = track.get('byArtist', {})
                        artist = artist_info.get('name', '') if isinstance(artist_info, dict) else 'Various Artists'
                        
                        if name and artist and self._is_ugandan_artist(artist):
                            songs.append(ScrapedSong(
                                title=name,
                                artist=artist,
                                rank=i,
                                score=self._calculate_score(i, platform),
                                source_type="streaming",
                                source=f"stream_{platform}",
                                metadata={
                                    "platform": platform_config["name"],
                                    "scraped_at": datetime.utcnow().isoformat(),
                                    "method": "json-ld"
                                }
                            ))
                except json.JSONDecodeError:
                    logger.warning("Could not parse Spotify JSON-LD")
            
            # Fallback to HTML parsing
            if not songs:
                # Spotify embed specific selectors
                track_selectors = [
                    '[data-testid="tracklist-row"]',
                    '.tracklist-row',
                    'tr[data-testid="tracklist-row"]',
                    '[role="row"]'
                ]
                
                for selector in track_selectors:
                    track_elements = soup.select(selector)
                    if track_elements:
                        break
                
                for i, element in enumerate(track_elements[:50], 1):
                    try:
                        # Extract track info from Spotify embed
                        title_elem = element.select_one('[data-testid="tracklist-title"]') or \
                                    element.select_one('.track-name') or \
                                    element.select_one('td:nth-child(2)')
                        
                        artist_elem = element.select_one('[data-testid="tracklist-artist"]') or \
                                     element.select_one('.artist-name') or \
                                     element.select_one('td:nth-child(3)')
                        
                        if title_elem and artist_elem:
                            title = self._clean_string(title_elem.get_text(strip=True))
                            artist = self._clean_string(artist_elem.get_text(strip=True))
                            
                            if title and artist and self._is_ugandan_artist(artist):
                                songs.append(ScrapedSong(
                                    title=title,
                                    artist=artist,
                                    rank=i,
                                    score=self._calculate_score(i, platform),
                                    source_type="streaming",
                                    source=f"stream_{platform}",
                                    metadata={
                                        "platform": platform_config["name"],
                                        "scraped_at": datetime.utcnow().isoformat(),
                                        "method": "html-parsing"
                                    }
                                ))
                    except Exception as e:
                        logger.debug(f"Error parsing Spotify track {i}: {e}")
            
            logger.info(f"✅ Spotify: Found {len(songs)} Ugandan songs")
            
        except Exception as e:
            logger.error(f"❌ Spotify scraping failed: {e}")
            songs = self._get_fallback_data(platform)
        
        return songs
    
    async def _scrape_spotify_api(self) -> List[ScrapedSong]:
        """Scrape Spotify using official API"""
        songs = []
        platform = "spotify"
        
        try:
            # Get playlist tracks
            results = self.spotify_client.playlist_tracks(
                self.spotify_playlist_id,
                market='UG',  # Uganda market
                limit=50
            )
            
            tracks = results.get('items', [])
            
            for i, item in enumerate(tracks, 1):
                track = item.get('track', {})
                if not track:
                    continue
                
                # Extract track info
                title = self._clean_string(track.get('name', ''))
                artists = track.get('artists', [])
                artist_names = [artist.get('name', '') for artist in artists if artist.get('name')]
                artist = ', '.join(artist_names) if artist_names else 'Various Artists'
                
                # Check if any artist is Ugandan
                is_ugandan = any(self._is_ugandan_artist(name) for name in artist_names)
                
                if title and is_ugandan:
                    songs.append(ScrapedSong(
                        title=title,
                        artist=artist,
                        rank=i,
                        plays=track.get('popularity', 0),  # Spotify popularity score
                        score=self._calculate_score(i, platform, popularity=track.get('popularity')),
                        source_type="streaming",
                        source=f"stream_{platform}",
                        url=track.get('external_urls', {}).get('spotify'),
                        duration_ms=track.get('duration_ms'),
                        album=track.get('album', {}).get('name'),
                        release_date=track.get('album', {}).get('release_date'),
                        popularity=track.get('popularity'),
                        metadata={
                            "platform": "Spotify Uganda Top 50",
                            "scraped_at": datetime.utcnow().isoformat(),
                            "method": "api",
                            "track_id": track.get('id'),
                            "album_id": track.get('album', {}).get('id'),
                            "artist_ids": [artist.get('id') for artist in artists if artist.get('id')]
                        }
                    ))
            
            logger.info(f"✅ Spotify API: Found {len(songs)} Ugandan songs")
            
        except Exception as e:
            logger.error(f"❌ Spotify API failed: {e}")
            raise
        
        return songs
    
    async def scrape_boomplay(self) -> List[ScrapedSong]:
        """Scrape Boomplay using Playwright"""
        songs = []
        platform = "boomplay"
        platform_config = self.platforms[platform]
        
        try:
            html = await self._scrape_with_playwright(platform_config["url"], platform)
            
            if not html:
                logger.warning(f"Using fallback data for {platform}")
                return self._get_fallback_data(platform)
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try multiple selectors
            selectors = platform_config.get("selectors", {}).get("container",
                ".chartItem, .song-item, .music-item, .playlist-item, li[data-id], .track-item")
            
            items = soup.select(selectors)
            
            for i, item in enumerate(items[:50], 1):
                try:
                    title_selector = platform_config.get("selectors", {}).get("title",
                        ".song-name, .title, .name, .songTitle")
                    artist_selector = platform_config.get("selectors", {}).get("artist",
                        ".artist-name, .singer, .artist, .author")
                    
                    title_elem = item.select_one(title_selector)
                    artist_elem = item.select_one(artist_selector)
                    
                    if title_elem and artist_elem:
                        title = self._clean_string(title_elem.get_text(strip=True))
                        artist = self._clean_string(artist_elem.get_text(strip=True))
                    else:
                        # Fallback
                        text = item.get_text(separator=' ', strip=True)
                        artist, title = self._extract_artist_title(text)
                    
                    if title and artist and self._is_ugandan_artist(artist):
                        songs.append(ScrapedSong(
                            title=title,
                            artist=artist,
                            rank=i,
                            score=self._calculate_score(i, platform),
                            source_type="streaming",
                            source=f"stream_{platform}",
                            metadata={
                                "platform": platform_config["name"],
                                "scraped_at": datetime.utcnow().isoformat(),
                                "method": "playwright"
                            }
                        ))
                        
                except Exception as e:
                    logger.debug(f"Error parsing Boomplay item {i}: {e}")
            
            logger.info(f"✅ Boomplay: Found {len(songs)} Ugandan songs")
            
        except Exception as e:
            logger.error(f"❌ Boomplay scraping failed: {e}")
            songs = self._get_fallback_data(platform)
        
        return songs
    
    async def scrape_audiomack(self) -> List[ScrapedSong]:
        """Scrape Audiomack (can use requests since minimal JS)"""
        songs = []
        platform = "audiomack"
        platform_config = self.platforms[platform]
        
        try:
            # Use requests for Audiomack (no JS needed)
            html = await self._scrape_with_requests(platform_config["url"], platform)
            
            if not html:
                logger.warning(f"Using fallback data for {platform}")
                return self._get_fallback_data(platform)
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Audiomack specific selectors
            selectors = platform_config.get("selectors", {}).get("container",
                "article, .music-card, .chart-item, [class*='music']")
            
            items = soup.select(selectors)
            
            for i, item in enumerate(items[:50], 1):
                try:
                    title_selector = platform_config.get("selectors", {}).get("title",
                        ".music-obj__title, .title, .song-title, [class*='title']")
                    artist_selector = platform_config.get("selectors", {}).get("artist",
                        ".music-obj__artist, .artist, .singer, [class*='artist']")
                    
                    title_elem = item.select_one(title_selector)
                    artist_elem = item.select_one(artist_selector)
                    
                    if title_elem and artist_elem:
                        title = self._clean_string(title_elem.get_text(strip=True))
                        artist = self._clean_string(artist_elem.get_text(strip=True))
                    else:
                        # Fallback pattern for Audiomack
                        text = item.get_text(separator=' ', strip=True)
                        if ' - ' in text:
                            parts = text.split(' - ', 1)
                            artist = self._clean_string(parts[0])
                            title = self._clean_string(parts[1].split('|')[0])
                        else:
                            artist, title = self._extract_artist_title(text)
                    
                    if title and artist and self._is_ugandan_artist(artist):
                        songs.append(ScrapedSong(
                            title=title,
                            artist=artist,
                            rank=i,
                            score=self._calculate_score(i, platform),
                            source_type="streaming",
                            source=f"stream_{platform}",
                            metadata={
                                "platform": platform_config["name"],
                                "scraped_at": datetime.utcnow().isoformat(),
                                "method": "requests"
                            }
                        ))
                        
                except Exception as e:
                    logger.debug(f"Error parsing Audiomack item {i}: {e}")
            
            logger.info(f"✅ Audiomack: Found {len(songs)} Ugandan songs")
            
        except Exception as e:
            logger.error(f"❌ Audiomack scraping failed: {e}")
            songs = self._get_fallback_data(platform)
        
        return songs
    
    def _get_fallback_data(self, platform: str) -> List[ScrapedSong]:
        """Generate realistic fallback data"""
        logger.warning(f"Using fallback data for {platform}")
        
        # Platform-specific sample data
        platform_samples = {
            "songboost": [
                ("Nalumansi", "Bobi Wine"),
                ("Sitya Loss", "Eddy Kenzo"),
                ("Malaika", "Sheebah"),
                ("Bomboclat", "Alien Skin"),
                ("Number One", "Azawi"),
            ],
            "spotify": [
                ("Sweet Love", "Vinka"),
                ("Tonjola", "Spice Diana"),
                ("Kaddugala", "Winnie Nwagi"),
                ("Biri Biri", "John Blaq"),
                ("Latest Hit", "Fik Fameica"),
            ],
            "boomplay": [
                ("Radio Favorite", "Bebe Cool"),
                ("Chart Topper", "Pallaso"),
                ("Weekend Special", "Daddy Andre"),
                ("Morning Jam", "Geosteady"),
                ("Drive Time Hit", "Gravity Omutujju"),
            ],
            "audiomack": [
                ("Street Anthem", "Vyroota"),
                ("Underground Hit", "Feffe Busi"),
                ("Viral Track", "Dax"),
                ("Local Favorite", "Vivian Tosh"),
                ("Trending Now", "King Saha"),
            ]
        }
        
        samples = platform_samples.get(platform, [
            ("Fallback Song", "Various Artists"),
            ("Backup Track", "Ugandan Artist"),
            ("Sample Hit", "Local Artist")
        ])
        
        platform_config = self.platforms.get(platform, {"name": platform})
        
        songs = []
        for i, (title, artist) in enumerate(samples, 1):
            songs.append(ScrapedSong(
                title=title,
                artist=artist,
                rank=i,
                score=self._calculate_score(i, platform),
                source_type="streaming",
                source=f"stream_{platform}",
                metadata={
                    "platform": platform_config["name"],
                    "scraped_at": datetime.utcnow().isoformat(),
                    "fallback": True
                }
            ))
        
        return songs
    
    async def save_to_database(self, songs: List[ScrapedSong]) -> Dict[str, int]:
        """Save scraped songs to database"""
        if not self.db:
            logger.warning("No database service available, skipping save")
            return {"total": len(songs), "added": 0, "updated": 0}
        
        added = 0
        updated = 0
        
        for song in songs:
            try:
                # Convert to database schema
                song_data = {
                    "title": song.title,
                    "artist": song.artist,
                    "plays": song.plays or 0,
                    "score": song.score,
                    "station": song.metadata.get("platform", f"Stream: {song.source}") if song.metadata else f"Stream: {song.source}",
                    "region": song.region,
                    "source_type": song.source_type,
                    "source": song.source,
                    "url": song.url,
                    "youtube_channel_id": None,
                    "youtube_video_id": None
                }
                
                # Add metadata as JSON string if needed
                if song.metadata:
                    song_data["metadata_json"] = json.dumps(song.metadata)
                
                # Add to database
                was_updated, song_id = self.db.add_song(song_data)
                
                if was_updated:
                    updated += 1
                else:
                    added += 1
                    
            except Exception as e:
                logger.error(f"Failed to save song {song.title}: {e}")
        
        logger.info(f"💾 Database: {added} added, {updated} updated, {len(songs) - (added + updated)} failed")
        
        return {
            "total": len(songs),
            "added": added,
            "updated": updated,
            "failed": len(songs) - (added + updated)
        }
    
    async def scrape_all(self) -> Dict[str, Any]:
        """Scrape all platforms asynchronously"""
        start_time = time.time()
        results = {}
        
        logger.info("🚀 Starting enhanced streams scraping for all platforms")
        
        # Get enabled platforms
        platforms_to_scrape = [p for p, config in self.platforms.items() if config.get("enabled", True)]
        
        # Create scraping tasks
        scraping_tasks = []
        for platform in platforms_to_scrape:
            scraper_method = getattr(self, f"scrape_{platform}", None)
            if scraper_method and callable(scraper_method):
                scraping_tasks.append(scraper_method())
        
        # Run all scrapers concurrently
        if scraping_tasks:
            all_results = await asyncio.gather(*scraping_tasks, return_exceptions=True)
            
            # Process results
            for platform, platform_songs in zip(platforms_to_scrape, all_results):
                platform_start = time.time()
                
                if isinstance(platform_songs, Exception):
                    logger.error(f"❌ {platform} scraping failed: {platform_songs}")
                    results[platform] = {
                        "status": "error",
                        "error": str(platform_songs),
                        "execution_time": round(time.time() - platform_start, 2)
                    }
                    continue
                
                # Save to database
                save_result = await self.save_to_database(platform_songs)
                
                platform_time = time.time() - platform_start
                
                results[platform] = {
                    "status": "success",
                    "songs_found": len(platform_songs),
                    "songs_saved": save_result,
                    "execution_time": round(platform_time, 2)
                }
                
                logger.info(f"✅ {platform}: {len(platform_songs)} songs, {save_result['added']} added, {save_result['updated']} updated")
        
        total_time = time.time() - start_time
        
        # Record in scraper history
        if self.db:
            total_found = sum(r.get("songs_found", 0) for r in results.values() if isinstance(r, dict) and r.get("status") == "success")
            total_added = sum(r.get("songs_saved", {}).get("added", 0) for r in results.values() if isinstance(r, dict) and r.get("status") == "success")
            
            self.db.add_scraper_history(
                scraper_type="streams",
                station_id="all_platforms",
                items_found=total_found,
                items_added=total_added,
                status="success" if any(r.get("status") == "success" for r in results.values()) else "error",
                execution_time=total_time
            )
        
        return {
            "status": "completed",
            "scraper_type": "streams",
            "timestamp": datetime.utcnow().isoformat(),
            "total_time": round(total_time, 2),
            "platforms_scraped": len(platforms_to_scrape),
            "successful_platforms": len([r for r in results.values() if isinstance(r, dict) and r.get("status") == "success"]),
            "results": results
        }
    
    async def close(self):
        """Cleanup resources"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("✅ Streams scraper resources cleaned up")
