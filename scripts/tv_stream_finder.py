#!/usr/bin/env python3
"""
tv_stream_finder.py - Discover .m3u8 stream URLs for Ugandan TV stations
"""

import asyncio
import aiohttp
import json
import re
import logging
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

class StreamURLFinder:
    """Discover and validate .m3u8 stream URLs"""
    
    def __init__(self):
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        # Known patterns for Ugandan TV streams
        self.m3u8_patterns = [
            r'(https?://[^\s"\']+\.m3u8[^\s"\']*)',
            r'src=["\'][^"\']+\.m3u8[^"\']*["\']',
            r'file["\']?\s*:\s*["\'][^"\']+\.m3u8',
            r'hlsUrl["\']?\s*:\s*["\'][^"\']+\.m3u8',
            r'streamUrl["\']?\s*:\s*["\'][^"\']+\.m3u8'
        ]
    
    async def find_stream_url(self, station_url: str) -> Optional[str]:
        """Find .m3u8 stream URL from station website"""
        try:
            async with self.session.get(station_url, headers=self.headers, timeout=10) as response:
                if response.status != 200:
                    logger.warning(f"HTTP {response.status} for {station_url}")
                    return None
                
                html = await response.text()
                
                # Method 1: Direct .m3u8 pattern matching
                for pattern in self.m3u8_patterns:
                    matches = re.findall(pattern, html, re.IGNORECASE)
                    for match in matches:
                        # Clean the URL
                        url = match.replace('src="', '').replace("src='", "")
                        url = url.replace('"', '').replace("'", "")
                        
                        if url.startswith('http') and '.m3u8' in url:
                            logger.info(f"Found stream URL in source: {url}")
                            return url
                
                # Method 2: Parse HTML for video elements
                soup = BeautifulSoup(html, 'html.parser')
                
                # Check video tags
                for video in soup.find_all('video'):
                    src = video.get('src')
                    if src and '.m3u8' in src:
                        return src if src.startswith('http') else urljoin(station_url, src)
                
                # Check iframes
                for iframe in soup.find_all('iframe'):
                    src = iframe.get('src')
                    if src and ('m3u8' in src or 'live' in src.lower() or 'stream' in src.lower()):
                        full_url = src if src.startswith('http') else urljoin(station_url, src)
                        return full_url
                
                # Method 3: Look for common streaming scripts
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string:
                        for pattern in self.m3u8_patterns:
                            matches = re.findall(pattern, script.string, re.IGNORECASE)
                            for match in matches:
                                url = match.replace('"', '').replace("'", "")
                                if url.startswith('http') and '.m3u8' in url:
                                    return url
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to find stream for {station_url}: {e}")
            return None
    
    async def test_stream(self, stream_url: str) -> bool:
        """Test if a stream URL is valid and accessible"""
        try:
            # First, check if URL is accessible
            async with self.session.head(stream_url, timeout=5) as response:
                if response.status == 200:
                    # Check if it's an m3u8 file
                    content_type = response.headers.get('content-type', '').lower()
                    if 'm3u8' in content_type or 'application/vnd.apple.mpegurl' in content_type:
                        return True
                    
                    # Get first few bytes to check content
                    async with self.session.get(stream_url, timeout=5) as get_response:
                        content = await get_response.text()
                        if '#EXTM3U' in content[:100]:
                            return True
            return False
            
        except Exception:
            return False
    
    async def discover_multiple(self, stations: Dict[str, str]) -> Dict[str, str]:
        """Discover stream URLs for multiple stations"""
        results = {}
        
        tasks = []
        for name, url in stations.items():
            task = self.find_stream_url(url)
            tasks.append((name, task))
        
        # Process with concurrency limit
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
        
        async def process_with_semaphore(name, task):
            async with semaphore:
                stream_url = await task
                if stream_url:
                    # Test the stream
                    is_valid = await self.test_stream(stream_url)
                    if is_valid:
                        results[name] = stream_url
                        logger.info(f"✅ Found valid stream for {name}")
                        return True
                    else:
                        logger.warning(f"⚠️ Found stream for {name} but it's not valid")
                else:
                    logger.warning(f"❌ No stream found for {name}")
                return False
        
        # Run all tasks
        await asyncio.gather(*[process_with_semaphore(name, task) for name, task in tasks])
        
        return results
    
    def save_results(self, streams: Dict[str, str], output_file: str = "data/tv_streams.json"):
        """Save discovered streams to configuration file"""
        try:
            # Load existing configuration if it exists
            existing_config = {"stations": {}}
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    existing_config = json.load(f)
            
            # Update with new discoveries
            for name, url in streams.items():
                if name not in existing_config["stations"]:
                    existing_config["stations"][name] = {
                        "stream_url": url,
                        "enabled": True,
                        "weight": 5,
                        "region": "ug",
                        "language": "english",
                        "check_interval": 300,
                        "capture_duration": 7
                    }
                else:
                    existing_config["stations"][name]["stream_url"] = url
            
            # Add metadata
            existing_config["last_discovery"] = datetime.now().isoformat()
            existing_config["total_streams_found"] = len(streams)
            
            # Save to file
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            logger.info(f"💾 Saved {len(streams)} stream URLs to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

async def discover_streams():
    """Main discovery function for Ugandan TV stations"""
    from datetime import datetime
    
    # Ugandan TV stations to scan
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
        "Sanyuka TV": "https://sanyukatv.ug/live",
        "NBS Sport": "https://nbssport.co.ug/live",
        "Salt TV": "https://saltmedia.ug",
        "Galaxy TV": "https://galaxy.co.ug/tv-live",
        "Smart 24 TV": "https://smart24.ug/live",
        "Channel 44": "https://channel44.ug"
    }
    
    print("\n" + "="*60)
    print("🔍 UGANDAN TV STREAM URL DISCOVERY")
    print("="*60)
    print(f"Scanning {len(stations_to_scan)} stations...\n")
    
    finder = StreamURLFinder()
    
    async with aiohttp.ClientSession() as session:
        finder.session = session
        discovered = await finder.discover_multiple(stations_to_scan)
    
    # Print results
    print("\n" + "="*60)
    print("📊 DISCOVERY RESULTS")
    print("="*60)
    
    if discovered:
        print(f"\n✅ Found {len(discovered)} valid stream URLs:\n")
        for name, url in discovered.items():
            print(f"  • {name}")
            print(f"    {url[:80]}..." if len(url) > 80 else f"    {url}")
            print()
        
        # Save results
        finder.save_results(discovered)
    else:
        print("\n❌ No stream URLs discovered")
    
    print("="*60)

def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Discover Ugandan TV stream URLs")
    parser.add_argument("--output", default="data/tv_streams.json", help="Output file path")
    parser.add_argument("--station", help="Scan specific station URL")
    
    args = parser.parse_args()
    
    if args.station:
        # Single station scan
        stations = {"Custom Station": args.station}
    else:
        # Use default stations
        stations = {
            "NTV Uganda": "https://ntv.co.ug/live",
            "NBS Television": "https://nbs.ug/live",
            "Bukedde TV 1": "https://newvision.co.ug/tv/4"
        }
    
    asyncio.run(discover_streams())

if __name__ == "__main__":
    main()
