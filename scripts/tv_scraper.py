# scripts/tv_scraper.py - UPDATED FOR PRODUCTION
import asyncio
import aiohttp
import yaml
from datetime import datetime
import logging
import os
from pathlib import Path

# Update the TV scraper configuration
TV_SCRAPER_CONFIG = {
    "engine_url": "https://ugboard-engine.onrender.com",
    "ingest_token": "YOUR_ACTUAL_TOKEN_HERE",  # Get from Render
    "stations": [
        {"name": "NTV Uganda", "url": "https://ntv.m3u8", "enabled": True},
        {"name": "NBS Television", "url": "https://nbs.m3u8", "enabled": True},
        {"name": "Bukedde TV", "url": "https://bukedde.m3u8", "enabled": True},
    ],
    "scrape_interval": 1800  # 30 minutes
}

logger = logging.getLogger(__name__)

class TVScraper:
    def __init__(self, engine_url: str, ingest_token: str):
        self.engine_url = engine_url.rstrip('/')
        self.ingest_token = ingest_token
        self.session = None
        
    async def connect(self):
        """Establish connection to UG Board Engine"""
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.ingest_token}",
                "Content-Type": "application/json"
            }
        )
        
    async def scrape_station(self, station_config: dict):
        """Scrape a single TV station"""
        try:
            # Simulate scraping (replace with actual scraping logic)
            scraped_songs = [
                {
                    "title": "Nalumansi",
                    "artist": "Bobi Wine",
                    "plays": 100,
                    "score": 95.5,
                    "station": station_config["name"]
                },
                {
                    "title": "Sitya Loss", 
                    "artist": "Eddy Kenzo",
                    "plays": 85,
                    "score": 92.3,
                    "station": station_config["name"]
                }
            ]
            
            # Send to UG Board Engine
            result = await self.send_to_engine(station_config["name"], scraped_songs)
            
            if result:
                logger.info(f"Successfully scraped {station_config['name']}: {len(scraped_songs)} songs")
            return result
            
        except Exception as e:
            logger.error(f"Failed to scrape {station_config['name']}: {e}")
            return None
    
    async def send_to_engine(self, station: str, songs: list):
        """Send scraped data to UG Board Engine"""
        payload = {
            "items": songs,
            "timestamp": datetime.utcnow().isoformat(),
            "source": station,
            "metadata": {
                "scraper_version": "1.0.0",
                "type": "tv"
            }
        }
        
        try:
            async with self.session.post(
                f"{self.engine_url}/ingest/tv",
                json=payload
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error = await response.text()
                    logger.error(f"Engine rejected data: {error}")
                    return None
        except Exception as e:
            logger.error(f"Failed to send to engine: {e}")
            return None
    
    async def run_scraping_cycle(self):
        """Run scraping cycle for all configured stations"""
        # Load station configuration
        config_path = Path("config/tv_stations.yaml")
        if not config_path.exists():
            logger.error("TV stations config not found")
            return
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        stations = config.get("stations", [])
        
        # Scrape each station
        tasks = []
        for station in stations:
            if station.get("enabled", True):
                task = asyncio.create_task(self.scrape_station(station))
                tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if r and not isinstance(r, Exception))
        logger.info(f"Scraping cycle complete: {successful}/{len(stations)} stations successful")
        
        return successful
    
    async def close(self):
        """Cleanup"""
        if self.session:
            await self.session.close()

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="UG Board TV Scraper")
    parser.add_argument("--engine-url", default="https://ugboard-engine.onrender.com")
    parser.add_argument("--token", required=True, help="Ingestion token")
    parser.add_argument("--stations", help="Comma-separated station names to scrape")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Update the configuration section
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "your-token-from-render")
ENGINE_URL = "https://ugboard-engine.onrender.com"

    # Initialize scraper
    scraper = TVScraper(args.engine_url, args.token)
    
    try:
        await scraper.connect()
        await scraper.run_scraping_cycle()
    finally:
        await scraper.close()

if __name__ == "__main__":
    asyncio.run(main())
