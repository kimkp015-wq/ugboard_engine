"""
streams_scheduler.py - Async scheduler for streams scraping
"""

import asyncio
import threading
import time
from datetime import datetime, timedelta
import logging
from typing import Optional, Callable

logger = logging.getLogger("streams_scheduler")

class AsyncStreamsScheduler:
    """Async scheduler for streams scraping every 6 hours"""
    
    def __init__(self, scraper, interval_hours: int = 6):
        self.scraper = scraper
        self.interval_hours = interval_hours
        self.is_running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.loop = None
        
    def calculate_next_run(self) -> datetime:
        """Calculate next run time"""
        now = datetime.utcnow()
        
        if not self.last_run:
            # First run should be now
            return now
        
        # Calculate next run (every N hours)
        next_run = self.last_run + timedelta(hours=self.interval_hours)
        
        # If next run is in the past, schedule for next interval
        while next_run < now:
            next_run += timedelta(hours=self.interval_hours)
        
        return next_run
    
    async def run_scheduled_job_async(self):
        """Run the scheduled streams scraping (async)"""
        if not self.is_running:
            return None
        
        self.last_run = datetime.utcnow()
        logger.info(f"🚀 Starting scheduled streams scraping at {self.last_run}")
        
        try:
            result = await self.scraper.scrape_all()
            logger.info(f"✅ Scheduled streams scraping completed: {result}")
            
            # Calculate next run
            self.next_run = self.calculate_next_run()
            logger.info(f"⏰ Next streams scraping scheduled for {self.next_run}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Scheduled streams scraping failed: {e}")
            return {"status": "error", "error": str(e)}
    
    def run_scheduled_job(self):
        """Run scheduled job in event loop"""
        if not self.loop:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        
        try:
            result = self.loop.run_until_complete(self.run_scheduled_job_async())
            return result
        except Exception as e:
            logger.error(f"Error running scheduled job: {e}")
            return {"status": "error", "error": str(e)}
    
    def scheduler_loop(self):
        """Main scheduler loop"""
        # Run immediately on start
        self.run_scheduled_job()
        
        # Schedule every N hours
        while self.is_running:
            try:
                now = datetime.utcnow()
                
                if self.next_run and now >= self.next_run:
                    # Time to run
                    self.run_scheduled_job()
                
                # Sleep for 1 minute and check again
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                time.sleep(60)  # Wait a minute before retrying
    
    def start_scheduler(self):
        """Start the scheduler"""
        if self.is_running:
            logger.warning("Streams scheduler is already running")
            return
        
        self.is_running = True
        
        # Start scheduler in background thread
        self.scheduler_thread = threading.Thread(target=self.scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        # Calculate next run
        self.next_run = self.calculate_next_run()
        
        logger.info(f"🚀 Streams scheduler started with {self.interval_hours}-hour interval")
        logger.info(f"⏰ Next run scheduled for: {self.next_run}")
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        # Close event loop
        if self.loop and not self.loop.is_closed():
            self.loop.close()
        
        logger.info("🛑 Streams scheduler stopped")
    
    async def trigger_now_async(self):
        """Trigger scraping immediately (async)"""
        if self.is_running:
            return await self.scraper.scrape_all()
        else:
            return {"status": "error", "message": "Scheduler not running"}
    
    def trigger_now(self):
        """Trigger scraping immediately (sync wrapper)"""
        if not self.loop:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        
        try:
            result = self.loop.run_until_complete(self.trigger_now_async())
            return result
        except Exception as e:
            logger.error(f"Error triggering scraping: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_status(self) -> dict:
        """Get scheduler status"""
        now = datetime.utcnow()
        
        if self.next_run:
            time_until_next = self.next_run - now
            hours = int(time_until_next.total_seconds() // 3600)
            minutes = int((time_until_next.total_seconds() % 3600) // 60)
            seconds = int(time_until_next.total_seconds() % 60)
        else:
            hours = minutes = seconds = 0
        
        return {
            "running": self.is_running,
            "interval_hours": self.interval_hours,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "time_until_next": f"{hours}h {minutes}m {seconds}s",
            "uptime": (now - self.last_run).total_seconds() if self.last_run else 0
        }
