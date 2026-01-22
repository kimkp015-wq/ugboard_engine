# Add to imports section
import asyncio
from streams_scraper import EnhancedStreamsScraper
from streams_scheduler import AsyncStreamsScheduler

# Update Config class to include Spotify credentials
class Config:
    # ... existing config ...
    
    # Spotify API (add to existing config)
    SPOTIFY_CLIENT_ID = "your_client_id_here"  # Get from environment
    SPOTIFY_CLIENT_SECRET = "your_client_secret_here"  # Get from environment
    
    # Streams scraper settings
    STREAMS_SCRAPE_INTERVAL_HOURS = 6
    STREAMS_PLATFORMS = ["songboost", "spotify", "boomplay", "audiomack"]
    STREAMS_ENABLE_PLAYWRIGHT = True

# Initialize streams scraper
streams_scraper = EnhancedStreamsScraper(db_service=db_service, config=config)
streams_scheduler = AsyncStreamsScheduler(
    scraper=streams_scraper, 
    interval_hours=config.STREAMS_SCRAPE_INTERVAL_HOURS
)

# Update lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle"""
    
    # Startup
    logger.info("=" * 70)
    logger.info(f"🚀 UG BOARD ENGINE v12.0.0 - WITH ENHANCED STREAMS SCRAPER")
    logger.info(f"📅 Chart Week: {current_chart_week}")
    logger.info(f"🗺️  Regions: {', '.join(sorted(config.VALID_REGIONS))}")
    logger.info(f"📺 TV Stations: {len(tv_scraper.stations)} configured")
    logger.info(f"📻 Radio Stations: {len(radio_scraper.stations)} configured")
    logger.info(f"📹 YouTube Channels: {len(youtube_scheduler.channels)} configured")
    logger.info(f"🎵 Streams Platforms: {len(streams_scraper.platforms)} configured")
    logger.info(f"🤖 Playwright Available: {PLAYWRIGHT_AVAILABLE}")
    logger.info("=" * 70)
    
    # Start YouTube scheduler
    try:
        youtube_scheduler.start_scheduler()
        logger.info("✅ YouTube scheduler started")
    except Exception as e:
        logger.error(f"Failed to start YouTube scheduler: {e}")
    
    # Start Streams scheduler
    try:
        streams_scheduler.start_scheduler()
        logger.info(f"✅ Streams scheduler started ({config.STREAMS_SCRAPE_INTERVAL_HOURS}-hour interval)")
    except Exception as e:
        logger.error(f"Failed to start streams scheduler: {e}")
    
    # Create sample data if database is empty (existing code)
    # ...
    
    yield
    
    # Shutdown
    logger.info("=" * 70)
    logger.info(f"🛑 UG Board Engine Shutting Down")
    logger.info(f"📊 Total Requests: {request_count}")
    
    # Stop YouTube scheduler
    youtube_scheduler.stop_scheduler()
    logger.info("✅ YouTube scheduler stopped")
    
    # Stop Streams scheduler
    streams_scheduler.stop_scheduler()
    logger.info("✅ Streams scheduler stopped")
    
    # Close streams scraper resources
    try:
        await streams_scraper.close()
        logger.info("✅ Streams scraper resources cleaned up")
    except Exception as e:
        logger.error(f"Error closing streams scraper: {e}")
    
    logger.info("✅ Shutdown complete")
    logger.info("=" * 70)

# Add new streams endpoints
@app.get("/streams/status", tags=["Streams"])
async def get_streams_status(auth: bool = Depends(AuthService.verify_ingest)):
    """Get streams scheduler status"""
    status = streams_scheduler.get_status()
    
    # Add platform info
    platforms_info = {}
    for platform_id, config in streams_scraper.platforms.items():
        platforms_info[platform_id] = {
            "name": config.get("name", platform_id),
            "enabled": config.get("enabled", True),
            "requires_js": config.get("requires_js", False),
            "weight": config.get("weight", 1.0)
        }
    
    status["platforms"] = platforms_info
    status["playwright_available"] = PLAYWRIGHT_AVAILABLE
    status["spotify_api_available"] = streams_scraper.spotify_client is not None
    
    return status

@app.post("/streams/scrape", tags=["Streams"])
async def trigger_streams_scraping(
    platform: Optional[str] = Query(None),
    background: bool = Query(False),
    auth: bool = Depends(AuthService.verify_ingest)
):
    """Trigger streams scraping manually"""
    try:
        if platform:
            # Validate platform
            if platform not in streams_scraper.platforms:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unknown platform: {platform}. Available: {list(streams_scraper.platforms.keys())}"
                )
            
            if background:
                # Run in background thread
                threading.Thread(
                    target=lambda: asyncio.run(
                        getattr(streams_scraper, f"scrape_{platform}")()
                    ),
                    daemon=True
                ).start()
                
                return {
                    "status": "queued",
                    "platform": platform,
                    "message": f"{platform} scraping queued in background"
                }
            else:
                # Run synchronously
                scraper_method = getattr(streams_scraper, f"scrape_{platform}")
                songs = await scraper_method()
                save_result = await streams_scraper.save_to_database(songs)
                
                return {
                    "status": "success",
                    "platform": platform,
                    "songs_found": len(songs),
                    "songs_saved": save_result
                }
        else:
            # Scrape all platforms
            if background:
                # Trigger scheduler to run now in background
                threading.Thread(
                    target=streams_scheduler.trigger_now,
                    daemon=True
                ).start()
                
                return {
                    "status": "queued",
                    "message": "All platforms scraping queued in background"
                }
            else:
                # Run synchronously
                result = await streams_scraper.scrape_all()
                return result
                
    except Exception as e:
        logger.error(f"Streams scraping error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Streams scraping failed: {str(e)}"
        )

# Add admin endpoint for detailed platform control
@app.get("/admin/streams/platforms/{platform_id}", tags=["Admin", "Streams"])
async def get_platform_details(
    platform_id: str,
    auth: bool = Depends(AuthService.verify_admin)
):
    """Get detailed platform information"""
    if platform_id not in streams_scraper.platforms:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Platform {platform_id} not found"
        )
    
    platform_config = streams_scraper.platforms[platform_id]
    
    # Get last scrape stats from database
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT status, items_found, items_added, execution_time, created_at
        FROM scraper_history 
        WHERE scraper_type = 'streams' AND station_id = ?
        ORDER BY created_at DESC 
        LIMIT 5
    ''', (platform_id,))
    
    history = cursor.fetchall()
    conn.close()
    
    return {
        "platform": platform_id,
        "config": platform_config,
        "last_scrapes": [
            {
                "status": row[0],
                "items_found": row[1],
                "items_added": row[2],
                "execution_time": row[3],
                "created_at": row[4]
            } for row in history
        ],
        "total_songs": db_service.get_song_count_by_source(f"stream_{platform_id}")
    }

# Update admin stats endpoint
@app.get("/admin/stats", tags=["Admin"])
async def admin_stats(auth: bool = Depends(AuthService.verify_admin)):
    """Get detailed system statistics"""
    # ... existing code ...
    
    # Add streams stats
    cursor.execute("SELECT COUNT(*) FROM songs WHERE source LIKE 'stream_%'")
    streams_songs = cursor.fetchone()[0]
    
    cursor.execute("SELECT source, COUNT(*) FROM songs WHERE source LIKE 'stream_%' GROUP BY source")
    streams_by_source = dict(cursor.fetchall())
    
    # ... rest of existing code ...
    
    stats["streams"] = {
        "total_songs": streams_songs,
        "by_platform": streams_by_source,
        "scheduler_running": streams_scheduler.is_running,
        "interval_hours": streams_scheduler.interval_hours,
        "platforms_configured": len(streams_scraper.platforms),
        "platforms_enabled": len([p for p in streams_scraper.platforms.values() if p.get("enabled", True)])
    }
    
    return stats
