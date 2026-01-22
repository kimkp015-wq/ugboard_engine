"""
streams_scheduler.py - Async scheduler for streams scraping
"""

import asyncio
import threading
import time
from datetime import datetime, timedelta
import logging
from typing import Optional

logger = logging.getLogger("streams_scheduler")

class StreamsScheduler:
    """Async scheduler for streams scraping every 6 hours"""
    
    def __init__(self, scraper, interval_hours: int = 6):
        self.scraper = scraper
        self.interval_hours = interval_hours
        self.is_running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
    def calculate_next_run(self) -> datetime:
        """Calculate next run time"""
        if not self.last_run:
            # Start immediately
            return datetime.utcnow()
        
        # Calculate next run (every X hours)
        next_run = self.last_run + timedelta(hours=self.interval_hours)
        
        # If next run is in the past, schedule for next interval
        now = datetime.utcnow()
        while next_run < now:
            next_run += timedelta(hours=self.interval_hours)
        
        return next_run
    
    async def run_scheduled_job_async(self):
        """Run the scheduled streams scraping asynchronously"""
        if not self.is_running:
            return
        
        self.last_run = datetime.utcnow()
        logger.info(f"🚀 Starting scheduled streams scraping at {self.last_run}")
        
        try:
            result = await self.scraper.scrape_all_async()
            logger.info(f"✅ Scheduled streams scraping completed: {result}")
            
            # Calculate next run
            self.next_run = self.calculate_next_run()
            logger.info(f"⏰ Next streams scraping scheduled for {self.next_run}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Scheduled streams scraping failed: {e}")
            return {"status": "error", "error": str(e)}
    
    def start_scheduler(self):
        """Start the scheduler in a background thread"""
        if self.is_running:
            logger.warning("Streams scheduler is already running")
            return
        
        self.is_running = True
        
        def scheduler_loop():
            # Create event loop for this thread
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # Run immediately on start
            self.loop.run_until_complete(self.run_scheduled_job_async())
            
            # Schedule every X hours
            while self.is_running:
                try:
                    now = datetime.utcnow()
                    
                    if self.next_run and now >= self.next_run:
                        # Time to run
                        self.loop.run_until_complete(self.run_scheduled_job_async())
                    
                    # Sleep for 1 minute and check again
                    time.sleep(60)
                    
                except Exception as e:
                    logger.error(f"Scheduler loop error: {e}")
                    time.sleep(60)  # Wait a minute before retrying
            
            # Clean up
            if self.loop:
                self.loop.close()
        
        # Start scheduler in background thread
        self.scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        logger.info(f"🚀 Streams scheduler started with {self.interval_hours}-hour interval")
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        logger.info("🛑 Streams scheduler stopped")
    
    async def trigger_now_async(self):
        """Trigger scraping immediately asynchronously"""
        if self.is_running:
            result = await self.run_scheduled_job_async()
            return {"status": "success", "result": result}
        else:
            return {"status": "error", "message": "Scheduler not running"}
    
    def trigger_now(self):
        """Synchronous version of trigger_now"""
        if self.is_running and self.loop:
            # Run in existing loop if available
            future = asyncio.run_coroutine_threadsafe(
                self.run_scheduled_job_async(),
                self.loop
            )
            try:
                result = future.result(timeout=300)  # 5 minute timeout
                return {"status": "queued", "message": "Streams scraping started"}
            except Exception as e:
                return {"status": "error", "error": str(e)}
        else:
            # Create new event loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(self.run_scheduled_job_async())
            return {"status": "success", "result": result}
