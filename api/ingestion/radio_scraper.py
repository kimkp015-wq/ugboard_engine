import asyncio
import aiohttp
import re
from datetime import datetime
import hashlib
import os

from data.store import load_items, save_items
from data.scoring import compute_score, calculate_scores  # ← ADD calculate_scores

class UgandaRadioScraper:
    """Simple radio scraper for UG Board"""
    
    def __init__(self):
        # UPDATED: 5 stations with proper UG Board region mapping
        self.stations = [
            {
                "name": "Capital FM",
                "url": "https://ice.capitalradio.co.ug/capital_live",
                "region": "Eastern",  # Kampala-based → Eastern region
                "frequency": "91.3"
            },
            {
                "name": "Galaxy FM", 
                "url": "http://41.210.160.10:8000/stream",
                "region": "Eastern",  # Kampala-based → Eastern region
                "frequency": "100.2"
            },
            {
                "name": "Radio Simba",
                "url": "https://stream.radiosimba.ug/live", 
                "region": "Eastern",  # Kampala-based → Eastern region
                "frequency": "97.3"
            },
            {
                "name": "Beat FM",
                "url": "http://91.193.183.197:8000/beatfm",  # ADD THIS STATION
                "region": "Eastern",  # Kampala-based → Eastern region
                "frequency": "96.3"
            },
            {
                "name": "Crooze FM",
                "url": "http://stream.croozefm.com:8000/croozefm",  # ADD THIS STATION
                "region": "Western",  # Mbarara-based → Western region
                "frequency": "91.2"
            }
        ]
    
    async def scrape_all(self):
        """Scrape all stations at once"""
        tasks = []
        for station in self.stations:
            tasks.append(self.scrape_one(station))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_songs = []
        for result in results:
            if isinstance(result, Exception):
                print(f"Station failed: {result}")
                continue
            if result:
                all_songs.extend(result)
        
        return all_songs
    
    async def scrape_one(self, station):
        """Scrape one station"""
        try:
            headers = {'Icy-MetaData': '1', 'User-Agent': 'UG-Board-Scraper/1.0'}
            
            async with aiohttp.ClientSession(timeout=5) as session:
                async with session.get(station["url"], headers=headers) as response:
                    if response.status != 200:
                        print(f"[{station['name']}] HTTP {response.status}")
                        return []
                    
                    # Check if station supports metadata
                    metaint = int(response.headers.get('icy-metaint', 0))
                    if metaint == 0:
                        print(f"[{station['name']}] No metadata support")
                        return []
                    
                    # Read audio data until metadata
                    reader = response.content
                    await reader.readexactly(metaint)
                    
                    # Read metadata length
                    meta_byte = await reader.readexactly(1)
                    meta_length = ord(meta_byte) * 16
                    
                    if meta_length == 0:
                        return []
                    
                    # Read metadata
                    meta_data = await reader.readexactly(meta_length)
                    meta_text = meta_data.decode('utf-8', errors='ignore')
                    
                    # Find song title
                    match = re.search(r"StreamTitle='(.*?)';", meta_text)
                    if match:
                        full_title = match.group(1).strip()
                        
                        # Split artist and title
                        if " - " in full_title:
                            parts = full_title.split(" - ", 1)
                            artist, title = parts[0].strip(), parts[1].strip()
                        elif ": " in full_title:
                            parts = full_title.split(": ", 1)
                            artist, title = parts[0].strip(), parts[1].strip()
                        elif "|" in full_title:
                            parts = full_title.split("|", 1)
                            artist, title = parts[0].strip(), parts[1].strip()
                        else:
                            artist, title = "Unknown", full_title
                        
                        # Clean up common prefixes
                        if title.startswith("By "):
                            title = title[3:]
                        if artist.startswith("By "):
                            artist = artist[3:]
                        
                        return [{
                            "station": station["name"],
                            "frequency": station["frequency"],
                            "artist": artist[:100],  # Limit length
                            "title": title[:200],     # Limit length
                            "timestamp": datetime.utcnow().isoformat(),
                            "region": station["region"]
                        }]
                    else:
                        print(f"[{station['name']}] No song title found in metadata")
        
        except asyncio.TimeoutError:
            print(f"[{station['name']}] Timeout")
        except Exception as e:
            print(f"[RADIO ERROR] {station['name']}: {str(e)[:100]}")
        
        return []
    
    async def scrape_and_save(self):
        """Scrape and save to database with proper scoring"""
        songs = await self.scrape_all()
        
        if not songs:
            return {"found": 0, "saved": 0}
        
        # Load existing data
        all_items = load_items()
        
        # Add new songs
        new_items = []
        for song in songs:
            # Create unique ID
            song_id = hashlib.md5(
                f"radio_{song['station']}_{song['artist']}_{song['title']}".encode()
            ).hexdigest()[:16]
            
            # Check if already exists (same song in last 30 mins)
            is_new = True
            current_time = datetime.fromisoformat(song["timestamp"].replace("Z", ""))
            
            for item in all_items[-200:]:  # Check last 200 items
                if (item.get("source") == "radio" and 
                    item.get("artist") == song["artist"] and 
                    item.get("title") == song["title"] and
                    item.get("station") == song["station"]):
                    
                    # Check time
                    try:
                        item_time = datetime.fromisoformat(item.get("timestamp", "").replace("Z", ""))
                        minutes_diff = (current_time - item_time).total_seconds() / 60
                        if minutes_diff < 30:
                            is_new = False
                            # Update existing item's radio_plays count
                            item["radio_plays"] = item.get("radio_plays", 0) + 1
                            item["timestamp"] = song["timestamp"]
                            break
                    except:
                        pass
            
            if is_new:
                new_item = {
                    "source": "radio",
                    "external_id": f"radio_{song_id}",
                    "station": song["station"],
                    "frequency": song.get("frequency", ""),
                    "artist": song["artist"],
                    "title": song["title"],
                    "radio_plays": 1,  # Each detection = 1 play
                    "timestamp": song["timestamp"],
                    "published_at": song["timestamp"],  # For scoring time decay
                    "region": song["region"],
                    "ingested_at": datetime.utcnow().isoformat()
                }
                
                new_items.append(new_item)
        
        # Add all new items to the database
        all_items.extend(new_items)
        
        # FIX: Use calculate_scores instead of compute_score per item
        if new_items:
            # Calculate scores for ALL items (ensures consistency)
            calculate_scores(all_items)
            # Save all items with updated scores
            save_items(all_items)
        
        return {
            "found": len(songs),
            "saved": len(new_items),
            "stations_scraped": len([s for s in self.stations if any(song["station"] == s["name"] for song in songs)]),
            "songs": songs[:3]  # Return first 3 as sample
        }
