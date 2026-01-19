"""
tv_stream_finder.py - Discover actual .m3u8 stream URLs from TV station websites
"""

import re
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class TVStreamFinder:
    """Discover and validate TV stream URLs"""
    
    # Common patterns for Ugandan TV stream URLs
    M3U8_PATTERNS = [
        r'https?://[^\s"\']+\.m3u8',
        r'src=["\'][^"\']+\.m3u8[^"\']*["\']',
        r'file["\']?\s*:\s*["\'][^"\']+\.m3u8',
        r'hlsUrl["\']?\s*:\s*["\'][^"\']+\.m3u8'
    ]
    
    # Known broadcaster domains and their stream paths
    KNOWN_STREAMS = {
        "ntv.co.ug": ["/live/", "/stream/", "/hls/"],
        "nbs.ug": ["/live/", "/stream/"],
        "newvision.co.ug": ["/tv/", "/live/"],
        "ubc.go.ug": ["/live/", "/stream/"],
        "babatv.co.ug": ["/live/", "/stream/"],
        "bbstv.ug": ["/live/", "/stream/"],
        "sanyukatv.ug": ["/live/", "/stream/"],
        "galaxy.co.ug": ["/tv-live/", "/stream/"]
    }
    
    async def discover_stream_url(self, station_url: str) -> Optional[str]:
        """Discover .m3u8 URL from station website"""
        try:
            async with aiohttp.ClientSession() as session:
                # Fetch the page
                async with session.get(station_url, timeout=10) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for {station_url}")
                        return None
                    
                    html = await response.text()
                    
                    # Method 1: Look for .m3u8 in page source
                    for pattern in self.M3U8_PATTERNS:
                        matches = re.findall(pattern, html, re.IGNORECASE)
                        if matches:
                            # Clean up the URL
                            url = matches[0].replace('src="', '').replace("src='", "")
                            url = url.replace('"', '').replace("'", "")
                            if url.startswith("http"):
                                logger.info(f"Found stream URL in source: {url}")
                                return url
                    
                    # Method 2: Parse HTML for video players
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for video tags
                    video_tags = soup.find_all('video')
                    for video in video_tags:
                        if video.get('src') and '.m3u8' in video['src']:
                            return video['src']
                    
                    # Look for iframes with stream
                    iframes = soup.find_all('iframe')
                    for iframe in iframes:
                        src = iframe.get('src', '')
                        if '.m3u8' in src or 'live' in src.lower():
                            return src
                    
                    # Method 3: Check common paths
                    domain = station_url.split('/')[2]
                    if domain in self.KNOWN_STREAMS:
                        for path in self.KNOWN_STREAMS[domain]:
                            test_url = f"https://{domain}{path}stream.m3u8"
                            if await self.test_stream_url(test_url):
                                return test_url
                    
                    return None
                    
        except Exception as e:
            logger.error(f"Discovery failed for {station_url}: {e}")
            return None
    
    async def test_stream_url(self, stream_url: str) -> bool:
        """Test if a stream URL is valid and accessible"""
        try:
            async with aiohttp.ClientSession() as session:
                # Simple HEAD request to check accessibility
                async with session.head(stream_url, timeout=5) as response:
                    if response.status == 200:
                        # Verify it's actually an m3u8 file
                        if 'm3u8' in response.headers.get('content-type', '').lower():
                            return True
                        
                        # Get first few bytes to check content
                        async with session.get(stream_url, timeout=5) as get_response:
                            content = await get_response.text()
                            if '#EXTM3U' in content[:100]:
                                return True
            return False
        except:
            return False
    
    async def scan_multiple_stations(self, station_urls: Dict[str, str]) -> Dict[str, str]:
        """Scan multiple stations for stream URLs"""
        results = {}
        
        tasks = []
        for name, url in station_urls.items():
            task = self.discover_stream_url(url)
            tasks.append((name, task))
        
        for name, task in tasks:
            try:
                stream_url = await task
                if stream_url:
                    results[name] = stream_url
                    logger.info(f"✓ Found stream for {name}")
                else:
                    logger.warning(f"✗ No stream found for {name}")
            except Exception as e:
                logger.error(f"Error scanning {name}: {e}")
        
        return results
    
    def save_discovered_streams(self, streams: Dict[str, str], filename: str = "data/tv_streams.json"):
        """Save discovered streams to file"""
        try:
            # Load existing config
            existing = {}
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    existing = json.load(f)
            
            # Update with new discoveries
            for name, url in streams.items():
                if name not in existing:
                    existing[name] = {"stream_url": url, "enabled": True, "weight": 5}
                else:
                    existing[name]["stream_url"] = url
            
            # Save back
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(existing, f, indent=2)
            
            logger.info(f"Saved {len(streams)} stream URLs to {filename}")
            
        except Exception as e:
            logger.error(f"Failed to save streams: {e}")

async def main():
    """Main discovery function"""
    # List of station websites to scan
    stations_to_scan = {
        "NTV Uganda": "https://ntv.co.ug/live",
        "NBS Television": "https://nbs.ug/live",
        "Bukedde TV 1": "https://newvision.co.ug/tv/4",
        "UBC TV": "https://ubc.go.ug/live-tv",
        "Baba TV": "https://babatv.co.ug/live",
        "TV West": "https://tvwest.co.ug/live",
        "BBS Terefayina": "https://bbstv.ug/live",
        "Urban TV": "https://urbantv.co.ug/live",
        "Spark TV": "https://ntv.co.ug/sparktv/live",
        "Sanyuka TV": "https://sanyukatv.ug/live"
    }
    
    finder = TVStreamFinder()
    logger.info(f"Scanning {len(stations_to_scan)} stations for stream URLs...")
    
    discovered = await finder.scan_multiple_stations(stations_to_scan)
    
    if discovered:
        finder.save_discovered_streams(discovered)
        print(f"\nDiscovered {len(discovered)} stream URLs:")
        for name, url in discovered.items():
            print(f"  • {name}: {url}")
    else:
        print("No stream URLs discovered")

if __name__ == "__main__":
    asyncio.run(main())
