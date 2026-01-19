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
