import asyncio
import aiohttp
import re
from datetime import datetime
import hashlib
import os

from data.store import load_items, save_items
from data.scoring import compute_score

class UgandaRadioScraper:
    """Simple radio scraper for UG Board"""
    
    def __init__(self):
        # Start with these 4 reliable stations
        self.stations = [
            {
                "name": "Capital FM",
                "url": "https://ice.capitalradio.co.ug/capital_live",
                "region": "central"
            },
            {
                "name": "Galaxy FM", 
                "url": "http://41.210.160.10:8000/stream",
                "region": "central"
            },
            {
                "name": "Radio Simba",
                "url": "https://stream.radiosimba.ug/live", 
                "region": "central"
            },
        ]
    
    async def scrape_all(self):
        """Scrape all stations at once"""
        tasks = []
        for station in self.stations:
            tasks.append(self.scrape_one(station))
        
        results = await asyncio.gather(*tasks)
        
        all_songs = []
        for songs in results:
            if songs:
                all_songs.extend(songs)
        
        return all_songs
    
    async def scrape_one(self, station):
        """Scrape one station"""
        try:
            headers = {'Icy-MetaData': '1'}
            
            async with aiohttp.ClientSession(timeout=5) as session:
                async with session.get(station["url"], headers=headers) as response:
                    if response.status != 200:
                        return []
                    
                    # Check if station supports metadata
                    metaint = int(response.headers.get('icy-metaint', 0))
                    if metaint == 0:
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
                        else:
                            artist, title = "Unknown", full_title
                        
                        return [{
                            "station": station["name"],
                            "artist": artist,
                            "title": title,
                            "timestamp": datetime.utcnow().isoformat(),
                            "region": station["region"]
                        }]
        
        except Exception as e:
            print(f"[RADIO ERROR] {station['name']}: {e}")
        
        return []
    
    async def scrape_and_save(self):
        """Scrape and save to database"""
        songs = await self.scrape_all()
        
        if not songs:
            return {"found": 0, "saved": 0}
        
        # Load existing data
        all_items = load_items()
        
        # Add new songs
        new_count = 0
        for song in songs:
            # Create unique ID
            song_id = hashlib.md5(
                f"radio_{song['station']}_{song['artist']}_{song['title']}".encode()
            ).hexdigest()[:16]
            
            # Check if already exists (same song in last 30 mins)
            is_new = True
            for item in all_items[-100:]:  # Check last 100 items
                if (item.get("source") == "radio" and 
                    item.get("artist") == song["artist"] and 
                    item.get("title") == song["title"] and
                    item.get("station") == song["station"]):
                    
                    # Check time
                    try:
                        item_time = datetime.fromisoformat(item.get("timestamp", "").replace("Z", ""))
                        song_time = datetime.fromisoformat(song["timestamp"].replace("Z", ""))
                        minutes_diff = (song_time - item_time).total_seconds() / 60
                        if minutes_diff < 30:
                            is_new = False
                            break
                    except:
                        pass
            
            if is_new:
                new_item = {
                    "source": "radio",
                    "external_id": f"radio_{song_id}",
                    "station": song["station"],
                    "artist": song["artist"],
                    "title": song["title"],
                    "radio_plays": 1,  # Each detection = 1 play
                    "timestamp": song["timestamp"],
                    "region": song["region"],
                    "ingested_at": datetime.utcnow().isoformat()
                }
                
                # Calculate score
                new_item["score"] = compute_score(new_item)
                
                all_items.append(new_item)
                new_count += 1
        
        # Save all items
        save_items(all_items)
        
        # Update scoring
        try:
            from api.scoring.auto import safe_auto_recalculate
            safe_auto_recalculate(all_items)
        except Exception as e:
            print(f"Scoring update failed: {e}")
        
        return {
            "found": len(songs),
            "saved": new_count,
            "songs": songs[:3]  # Return first 3 as sample
        }
