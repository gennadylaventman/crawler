"""
Crawl session management for the web crawler system.
"""

import time
import uuid
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

from crawler.utils.config import CrawlConfig


@dataclass
class CrawlSession:
    """Represents a crawling session with metadata and statistics."""
    
    session_id: str
    name: str
    config: CrawlConfig
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    status: str = "pending"  # pending, running, completed, failed, paused
    
    # Statistics
    pages_crawled: int = 0
    pages_failed: int = 0
    pages_skipped: int = 0
    total_words: int = 0
    unique_words: int = 0
    total_bytes: int = 0
    
    # Performance metrics
    avg_response_time: float = 0.0
    avg_processing_time: float = 0.0
    pages_per_second: float = 0.0
    
    # Error tracking
    error_count: int = 0
    last_error: Optional[str] = None
    
    # Progress tracking
    urls_discovered: int = 0
    urls_in_queue: int = 0
    current_depth: int = 0
    max_depth_reached: int = 0
    
    def __post_init__(self):
        """Initialize session after creation."""
        if not self.session_id:
            self.session_id = str(uuid.uuid4())
    
    @property
    def elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def is_running(self) -> bool:
        """Check if session is currently running."""
        return self.status == "running"
    
    @property
    def is_completed(self) -> bool:
        """Check if session is completed."""
        return self.status in ["completed", "failed"]
    
    def start(self) -> None:
        """Start the crawling session."""
        self.status = "running"
        self.start_time = time.time()
    
    def complete(self, success: bool = True) -> None:
        """Complete the crawling session."""
        self.status = "completed" if success else "failed"
        self.end_time = time.time()
        self._update_performance_metrics()
    
    def pause(self) -> None:
        """Pause the crawling session."""
        self.status = "paused"
    
    def resume(self) -> None:
        """Resume the crawling session."""
        self.status = "running"
    
    def update_statistics(self, **kwargs) -> None:
        """Update session statistics."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        self._update_performance_metrics()
    
    def increment_pages_crawled(self, word_count: int = 0, byte_count: int = 0) -> None:
        """Increment pages crawled counter."""
        self.pages_crawled += 1
        self.total_words += word_count
        self.total_bytes += byte_count
        self._update_performance_metrics()
    
    def increment_pages_failed(self, error_message: Optional[str] = None) -> None:
        """Increment pages failed counter."""
        self.pages_failed += 1
        self.error_count += 1
        if error_message:
            self.last_error = error_message
    
    def increment_pages_skipped(self) -> None:
        """Increment pages skipped counter."""
        self.pages_skipped += 1
    
    def _update_performance_metrics(self) -> None:
        """Update performance metrics based on current statistics."""
        if self.elapsed_time > 0:
            self.pages_per_second = self.pages_crawled / self.elapsed_time
    
    def get_summary(self) -> Dict[str, Any]:
        """Get session summary as dictionary."""
        return {
            'session_id': self.session_id,
            'name': self.name,
            'status': self.status,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'elapsed_time': self.elapsed_time,
            'pages_crawled': self.pages_crawled,
            'pages_failed': self.pages_failed,
            'pages_skipped': self.pages_skipped,
            'total_words': self.total_words,
            'unique_words': self.unique_words,
            'total_bytes': self.total_bytes,
            'avg_response_time': self.avg_response_time,
            'avg_processing_time': self.avg_processing_time,
            'pages_per_second': self.pages_per_second,
            'error_count': self.error_count,
            'last_error': self.last_error,
            'urls_discovered': self.urls_discovered,
            'urls_in_queue': self.urls_in_queue,
            'current_depth': self.current_depth,
            'max_depth_reached': self.max_depth_reached,
            'success_rate': (self.pages_crawled / (self.pages_crawled + self.pages_failed) * 100) 
                           if (self.pages_crawled + self.pages_failed) > 0 else 0
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert session to dictionary for serialization."""
        return {
            'session_id': self.session_id,
            'name': self.name,
            'config': self.config.dict() if self.config else None,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'status': self.status,
            'pages_crawled': self.pages_crawled,
            'pages_failed': self.pages_failed,
            'pages_skipped': self.pages_skipped,
            'total_words': self.total_words,
            'unique_words': self.unique_words,
            'total_bytes': self.total_bytes,
            'avg_response_time': self.avg_response_time,
            'avg_processing_time': self.avg_processing_time,
            'pages_per_second': self.pages_per_second,
            'error_count': self.error_count,
            'last_error': self.last_error,
            'urls_discovered': self.urls_discovered,
            'urls_in_queue': self.urls_in_queue,
            'current_depth': self.current_depth,
            'max_depth_reached': self.max_depth_reached
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], config: CrawlConfig) -> 'CrawlSession':
        """Create session from dictionary."""
        session = cls(
            session_id=data['session_id'],
            name=data['name'],
            config=config
        )
        
        # Update all fields from data
        for key, value in data.items():
            if hasattr(session, key) and key not in ['session_id', 'name', 'config']:
                setattr(session, key, value)
        
        return session
