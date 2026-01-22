# src/application/services/progress_tracker.py
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
import json


class ProgressStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    AUTH_FAILED = "auth_failed"


@dataclass
class ProgressEvent:
    job_id: str
    platform: str
    current: int
    total: int
    status: ProgressStatus
    metadata: Dict[str, Any]
    timestamp: datetime
    
    @property
    def percentage(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.current / self.total) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['status'] = self.status.value
        data['timestamp'] = self.timestamp.isoformat()
        data['percentage'] = self.percentage
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class ProgressRepository(ABC):
    @abstractmethod
    def save_progress(self, event: ProgressEvent) -> None:
        pass
    
    @abstractmethod
    def get_progress(self, job_id: str) -> Optional[ProgressEvent]:
        pass
    
    @abstractmethod
    def get_recent_jobs(self, platform: str, limit: int = 100) -> List[ProgressEvent]:
        pass


class ProgressTracker:
    """Tracks scraping progress with real-time updates"""
    
    def __init__(
        self,
        progress_repo: ProgressRepository,
        event_publisher: Optional[Any] = None
    ):
        self.progress_repo = progress_repo
        self.event_publisher = event_publisher
        self._active_jobs: Dict[str, ProgressEvent] = {}
    
    def start_job(
        self,
        job_id: str,
        platform: str,
        total_items: int,
        metadata: Optional[Dict] = None
    ) -> ProgressEvent:
        """Initialize a new scraping job"""
        event = ProgressEvent(
            job_id=job_id,
            platform=platform,
            current=0,
            total=total_items,
            status=ProgressStatus.RUNNING,
            metadata=metadata or {},
            timestamp=datetime.utcnow()
        )
        
        self.progress_repo.save_progress(event)
        self._active_jobs[job_id] = event
        
        if self.event_publisher:
            self.event_publisher.publish("progress.start", event.to_dict())
        
        return event
    
    def update_progress(
        self,
        job_id: str,
        current: int,
        total: Optional[int] = None,
        metadata: Optional[Dict] = None
    ) -> Optional[ProgressEvent]:
        """Update progress for an existing job"""
        existing = self.progress_repo.get_progress(job_id)
        if not existing:
            return None
        
        if total is not None:
            existing.total = total
        existing.current = current
        existing.timestamp = datetime.utcnow()
        
        if metadata:
            existing.metadata.update(metadata)
        
        # Update status based on progress
        if existing.current >= existing.total:
            existing.status = ProgressStatus.COMPLETED
        elif existing.status != ProgressStatus.FAILED:
            existing.status = ProgressStatus.RUNNING
        
        self.progress_repo.save_progress(existing)
        
        if self.event_publisher:
            self.event_publisher.publish("progress.update", existing.to_dict())
        
        return existing
    
    def mark_auth_failed(
        self,
        job_id: str,
        platform: str,
        error_message: str,
        metadata: Optional[Dict] = None
    ) -> ProgressEvent:
        """Mark job as failed due to authentication"""
        event = ProgressEvent(
            job_id=job_id,
            platform=platform,
            current=0,
            total=0,
            status=ProgressStatus.AUTH_FAILED,
            metadata={
                "error": error_message,
                "error_type": "authentication",
                **(metadata or {})
            },
            timestamp=datetime.utcnow()
        )
        
        self.progress_repo.save_progress(event)
        
        if self.event_publisher:
            self.event_publisher.publish("progress.auth_failed", event.to_dict())
        
        return event
    
    def get_job_progress(self, job_id: str) -> Optional[ProgressEvent]:
        """Get current progress for a job"""
        return self.progress_repo.get_progress(job_id)
    
    def get_active_jobs(self) -> List[ProgressEvent]:
        """Get all currently active jobs"""
        return [
            event for event in self._active_jobs.values()
            if event.status in [ProgressStatus.RUNNING, ProgressStatus.PAUSED]
        ]
