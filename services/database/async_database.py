"""
Async file-based database operations using aiofiles
"""
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
import aiofiles

class AsyncJSONDatabase:
    """
    Async file-based database using aiofiles
    
    Suitable for high-concurrency scenarios
    """
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True)
        
        # Initialize data structures
        self.songs: List[Dict[str, Any]] = []
        self.chart_history: List[Dict[str, Any]] = []
        self.regions: Dict[str, Dict[str, Any]] = {
            "ug": {"name": "Uganda", "songs": []},
            "ke": {"name": "Kenya", "songs": []},
            "tz": {"name": "Tanzania", "songs": []},
            "rw": {"name": "Rwanda", "songs": []}
        }
        
        # Load data asynchronously on startup
        self._load_task = asyncio.create_task(self._load_all_data())
    
    async def _load_all_data(self):
        """Load all data files asynchronously"""
        await asyncio.gather(
            self._load_songs(),
            self._load_chart_history(),
            self._load_regions()
        )
        print(f"✅ Loaded {len(self.songs)} songs asynchronously")
    
    async def _load_songs(self):
        """Load songs from JSON file"""
        songs_file = self.data_dir / "songs.json"
        if await songs_file.exists():
            try:
                async with aiofiles.open(songs_file, 'r') as f:
                    content = await f.read()
                    self.songs = json.loads(content)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading songs: {e}")
                self.songs = []
    
    async def _load_chart_history(self):
        """Load chart history from JSON file"""
        history_file = self.data_dir / "chart_history.json"
        if await history_file.exists():
            try:
                async with aiofiles.open(history_file, 'r') as f:
                    content = await f.read()
                    self.chart_history = json.loads(content)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading chart history: {e}")
                self.chart_history = []
    
    async def _load_regions(self):
        """Load regions from JSON file"""
        regions_file = self.data_dir / "regions.json"
        if await regions_file.exists():
            try:
                async with aiofiles.open(regions_file, 'r') as f:
                    content = await f.read()
                    loaded_regions = json.loads(content)
                    for region, data in loaded_regions.items():
                        if region in self.regions:
                            self.regions[region].update(data)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading regions: {e}")
    
    async def _save_all_data(self):
        """Save all data files asynchronously"""
        await asyncio.gather(
            self._save_songs(),
            self._save_chart_history(),
            self._save_regions()
        )
    
    async def _save_songs(self):
        """Save songs to JSON file"""
        songs_file = self.data_dir / "songs.json"
        try:
            async with aiofiles.open(songs_file, 'w') as f:
                await f.write(json.dumps(self.songs, indent=2, default=str))
        except IOError as e:
            print(f"Error saving songs: {e}")
    
    async def _save_chart_history(self):
        """Save chart history to JSON file"""
        history_file = self.data_dir / "chart_history.json"
        try:
            async with aiofiles.open(history_file, 'w') as f:
                await f.write(json.dumps(self.chart_history, indent=2, default=str))
        except IOError as e:
            print(f"Error saving chart history: {e}")
    
    async def _save_regions(self):
        """Save regions to JSON file"""
        regions_file = self.data_dir / "regions.json"
        try:
            async with aiofiles.open(regions_file, 'w') as f:
                await f.write(json.dumps(self.regions, indent=2, default=str))
        except IOError as e:
            print(f"Error saving regions: {e}")
    
    async def add_songs(self, songs_data: List[Dict[str, Any]], source: str) -> int:
        """Add songs asynchronously"""
        added_count = 0
        
        for song in songs_data:
            song["source"] = source
            song["ingested_at"] = datetime.utcnow().isoformat()
            song["id"] = f"song_{len(self.songs) + added_count + 1}"
            
            self.songs.append(song)
            
            # Add to region
            region = song.get("region", "ug").lower()
            if region in self.regions:
                self.regions[region]["songs"].append(song)
            
            added_count += 1
        
        # Limit total songs
        if len(self.songs) > 10000:
            self.songs = self.songs[-10000:]
        
        # Save asynchronously
        if added_count > 0:
            asyncio.create_task(self._save_all_data())
        
        return added_count
    
    async def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs asynchronously"""
        # Wait for initial load if still loading
        if self._load_task and not self._load_task.done():
            await self._load_task
        
        source_list = self.regions[region]["songs"] if region and region in self.regions else self.songs
        
        sorted_songs = sorted(
            source_list,
            key=lambda x: x.get("score", 0),
            reverse=True
        )
        return sorted_songs[:limit]
    
    async def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs asynchronously"""
        # Wait for initial load
        if self._load_task and not self._load_task.done():
            await self._load_task
        
        if not self.songs:
            return []
        
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        
        def trending_score(song: Dict[str, Any]) -> float:
            score = song.get("score", 0) * 0.7
            plays = song.get("plays", 0) * 0.3
            
            ingested_at = song.get("ingested_at")
            if ingested_at:
                try:
                    ingest_time = datetime.fromisoformat(ingested_at.replace('Z', '+00:00'))
                    if ingest_time > recent_cutoff:
                        score += 10.0
                except (ValueError, AttributeError):
                    pass
            
            return score + plays
        
        sorted_trending = sorted(self.songs, key=trending_score, reverse=True)
        return sorted_trending[:limit]
