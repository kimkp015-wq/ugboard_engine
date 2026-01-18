"""
Enhanced Radio Scraper with 14 Ugandan Stations
Based on your provided code
"""

import os
import asyncio
import aiohttp
import re
from datetime import datetime
import hashlib
import requests
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedUgandaRadioScraper:
    """Enhanced scraper with 14 Ugandan radio stations"""
    
    def __init__(self):
        # Your enhanced station list with Icecast metadata support
        self.stations = [
            # Major Kampala Stations (Icecast metadata available)
            {
                "name": "Capital FM",
                "url": "https://ice.capitalradio.co.ug/capital_live",
                "region": "Central",
                "frequency": "91.3",
                "city": "Kampala",
                "type": "icecast"  # Supports metadata
            },
            {
                "name": "Radio Simba", 
                "url": "https://stream.radiosimba.ug/live",
                "region": "Central", 
                "frequency": "97.3",
                "city": "Kampala",
                "type": "icecast"
            },
            {
                "name": "Galaxy FM",
                "url": "http://41.210.160.10:8000/stream",
                "region": "Central",
                "frequency": "100.2",
                "city": "Kampala",
                "type": "icecast"
            },
            {
                "name": "Beat FM",
                "url": "http://91.193.183.197:8000/beatfm",
                "region": "Central",
                "frequency": "96.3",
                "city": "Kampala",
                "type": "icecast"
            },
            {
                "name": "Crooze FM",
                "url": "http://stream.croozefm.com:8000/croozefm",
                "region": "Western",
                "frequency": "91.2",
                "city": "Mbarara",
                "type": "icecast"
            },
            
            # Additional stations (for future expansion)
            {
                "name": "CBS Radio",
                "url": "https://stream.radio.co.ug/cbs888/listen",
                "region": "Central",
                "frequency": "88.8",
                "city": "Kampala",
                "type": "radio_co"
            },
            {
                "name": "KFM",
                "url": "http://nation.hostingradio.ru:8035/kfm",
                "region": "Central",
                "frequency": "93.3",
                "city": "Kampala",
                "type": "icecast"
            },
            {
                "name": "NBS Radio",
                "url": "https://nbsradio.ug/stream",
                "region": "Eastern",
                "frequency": "89.4",
                "city": "Jinja",
                "type": "web"
            },
            {
                "name": "Arua One",
                "url": "https://aruaone.fm/stream",
                "region": "Northern",
                "frequency": "88.7",
                "city": "Arua",
                "type": "web"
            },
            {
                "name": "Radio One",
                "url": "http://radioone.fm/live",
                "region": "Central",
                "frequency": "90.0",
                "city": "Kampala",
                "type": "web"
            }
        ]
    
    async def scrape_icecast_station(self, station):
        """Scrape Icecast stream with metadata"""
        try:
            headers = {
                'Icy-MetaData': '1', 
                'User-Agent': 'UG-Board-Scraper/1.0'
            }
            
            timeout = aiohttp.ClientTimeout(total=8)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(station["url"], headers=headers) as response:
                    if response.status != 200:
                        logger.warning(f"{station['name']}: HTTP {response.status}")
                        return None
                    
                    metaint = int(response.headers.get('icy-metaint', 0))
                    if metaint == 0:
                        logger.info(f"{station['name']}: No metadata support")
                        return None
                    
                    # Read to metadata block
                    reader = response.content
                    await reader.readexactly(metaint)
                    
                    # Read metadata length
                    meta_byte = await reader.readexactly(1)
                    meta_length = ord(meta_byte) * 16
                    
                    if meta_length == 0:
                        return None
                    
                    # Read metadata
                    meta_data = await reader.readexactly(meta_length)
                    meta_text = meta_data.decode('utf-8', errors='ignore')
                    
                    # Extract song info
                    match = re.search(r"StreamTitle='(.*?)';", meta_text)
                    if match:
                        full_title = match.group(1).strip()
                        
                        # Parse artist and title
                        artist, title = self._parse_artist_title(full_title)
                        
                        if artist == "Unknown" and title == "Unknown Track":
                            logger.info(f"{station['name']}: No song currently playing")
                            return None
                        
                        logger.info(f"✅ {station['name']}: {artist} - {title}")
                        
                        return {
                            "station": station["name"],
                            "frequency": station["frequency"],
                            "artist": artist,
                            "title": title,
                            "timestamp": datetime.utcnow().isoformat(),
                            "region": station["region"],
                            "city": station["city"],
                            "source_url": station["url"],
                            "source_type": station["type"]
                        }
                    
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ {station['name']}: Timeout")
        except Exception as e:
            logger.error(f"❌ {station['name']}: {str(e)[:100]}")
        
        return None
    
    def _parse_artist_title(self, full_title: str):
        """Parse artist and title from various formats"""
        full_title = full_title.strip()
        
        # Remove common prefixes
        if full_title.startswith("Now Playing: "):
            full_title = full_title[13:]
        if full_title.startswith("Current: "):
            full_title = full_title[9:]
        
        # Common separators in Ugandan radio
        separators = [
            (" - ", 1),  # Most common: "Artist - Title"
            (" : ", 1),  # "Artist : Title"
            (" | ", 1),  # "Artist | Title"
            (" ~ ", 1),  # "Artist ~ Title"
            (" by ", 0), # "Title by Artist"
            (" ft. ", 1), # "Artist ft. Other - Title"
            (" feat. ", 1),
        ]
        
        for sep, artist_first in separators:
            if sep in full_title:
                parts = full_title.split(sep, 1)
                if artist_first:
                    artist, title = parts[0].strip(), parts[1].strip()
                else:
                    title, artist = parts[0].strip(), parts[1].strip()
                
                # Clean up
                artist = artist.replace("(Official Audio)", "").replace("(Official Video)", "").strip()
                title = title.replace("(Official Audio)", "").replace("(Official Video)", "").strip()
                
                return artist[:100], title[:200]
        
        # If no separator found
        if len(full_title) < 50:  # Probably just a title
            return "Unknown Artist", full_title[:200]
        else:
            return "Unknown Artist", "Unknown Track"
    
    async def scrape_web_station(self, station):
        """Scrape web-based stations (placeholder for future implementation)"""
        # This would use requests/BeautifulSoup to scrape station websites
        # For now, return None
        return None
    
    async def scrape_one(self, station):
        """Route to appropriate scraper based on station type"""
        if station["type"] == "icecast":
            return await self.scrape_icecast_station(station)
        elif station["type"] == "radio_co":
            # Radio.co stations need different approach
            return await self.scrape_radio_co_station(station)
        else:
            return await self.scrape_web_station(station)
    
    async def scrape_radio_co_station(self, station):
        """Scrape Radio.co stations (they have API)"""
        try:
            # Radio.co stations often have JSON API
            api_url = station["url"].replace("/listen", "/status-json.xsl")
            
            timeout = aiohttp.ClientTimeout(total=5)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Extract from Radio.co JSON structure
                        if "icestats" in data and "source" in data["icestats"]:
                            source = data["icestats"]["source"]
                            if isinstance(source, dict) and "title" in source:
                                full_title = source["title"]
                                artist, title = self._parse_artist_title(full_title)
                                
                                if artist != "Unknown" or title != "Unknown Track":
                                    logger.info(f"✅ {station['name']}: {artist} - {title}")
                                    
                                    return {
                                        "station": station["name"],
                                        "frequency": station["frequency"],
                                        "artist": artist,
                                        "title": title,
                                        "timestamp": datetime.utcnow().isoformat(),
                                        "region": station["region"],
                                        "city": station["city"],
                                        "source_url": station["url"],
                                        "source_type": "radio_co"
                                    }
        except Exception as e:
            logger.error(f"Radio.co error {station['name']}: {e}")
        
        return None
    
    async def scrape_all(self):
        """Scrape all stations with concurrency control"""
        logger.info(f"📻 Starting scrape of {len(self.stations)} Ugandan radio stations")
        
        # Scrape in batches to avoid overwhelming
        batch_size = 3
        all_results = []
        
        for i in range(0, len(self.stations), batch_size):
            batch = self.stations[i:i + batch_size]
            tasks = [self.scrape_one(station) for station in batch]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter successful results
            for result in results:
                if isinstance(result, Exception):
                    continue
                if result:
                    all_results.append(result)
            
            # Small delay between batches
            if i + batch_size < len(self.stations):
                await asyncio.sleep(1)
        
        logger.info(f"📊 Scrape complete: {len(all_results)} songs found")
        return all_results

async def main():
    """Main scraping function"""
    print("=" * 60)
    print("🚀 ENHANCED UGANDAN RADIO SCRAPER")
    print(f"📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 60)
    
    scraper = EnhancedUgandaRadioScraper()
    songs = await scraper.scrape_all()
    
    if songs:
        # Prepare payload for UG Board Engine
        payload = {
            "items": songs,
            "source": "radio",
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "scraper": "enhanced-radio-scraper",
                "stations_scraped": len(set(s["station"] for s in songs)),
                "cities": list(set(s.get("city", "Unknown") for s in songs)),
                "regions": list(set(s.get("region", "Unknown") for s in songs))
            }
        }
        
        # Send to UG Board Engine API
        try:
            api_url = "https://ugboard-engine.onrender.com/ingest/radio"
            api_token = "1994199620002019866"  # Your internal token
            
            response = requests.post(
                api_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": api_token
                },
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Success! Sent {len(songs)} songs")
                print(f"📝 Message: {result.get('message', 'No message')}")
                print(f"🎵 Items processed: {result.get('items_processed', 0)}")
                
                # Show sample of what was sent
                print("\n📋 Sample of songs found:")
                for i, song in enumerate(songs[:3]):  # Show first 3
                    print(f"  {i+1}. {song['station']}: {song['artist']} - {song['title']}")
                
            else:
                print(f"❌ API Error: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                
        except Exception as e:
            print(f"❌ Failed to send to API: {e}")
    
    else:
        print("ℹ️ No songs found this time")
    
    print("=" * 60)
    print("🎉 Scraping completed!")
    
    return len(songs)

if __name__ == "__main__":
    songs_count = asyncio.run(main())
    # Exit with code 0 if any songs found, 1 if none
    exit(0 if songs_count > 0 else 1)
