"""
Queue factory for creating configurable URL queues.
"""

from typing import Optional, TYPE_CHECKING
from crawler.utils.config import CrawlConfig
from crawler.url_management.queue import URLQueue
from crawler.storage.persistent_queue import PersistentURLQueue

if TYPE_CHECKING:
    from crawler.storage.database import DatabaseManager


class QueueFactory:
    """Factory for creating URL queues based on configuration."""
    
    @staticmethod
    def create_queue(
        config: CrawlConfig,
        db_manager: Optional['DatabaseManager'] = None,
        session_id: Optional[str] = None
    ) -> URLQueue:
        """
        Create a URL queue based on configuration.
        
        Args:
            config: Crawler configuration
            db_manager: Database manager (required for persistent queue)
            session_id: Session ID (required for persistent queue)
            
        Returns:
            URLQueue instance (either in-memory or persistent)
        """
        # Check if persistent queue is enabled
        if config.crawler.enable_persistent_queue:
            if not db_manager:
                raise ValueError("DatabaseManager is required for persistent queue")
            if not session_id:
                raise ValueError("Session ID is required for persistent queue")
            
            # Create persistent queue
            return PersistentURLQueue(
                session_id=session_id,
                db_manager=db_manager,
                max_size=config.crawler.url_queue_size,
                enable_bloom_filter=config.crawler.enable_bloom_filter
            )
        else:
            # Create in-memory queue
            return URLQueue(
                max_size=config.crawler.url_queue_size,
                enable_bloom_filter=config.crawler.enable_bloom_filter
            )
    
    @staticmethod
    def is_persistent_queue_enabled(config: CrawlConfig) -> bool:
        """Check if persistent queue is enabled in configuration."""
        return config.crawler.enable_persistent_queue
    
    @staticmethod
    def get_queue_type_name(config: CrawlConfig) -> str:
        """Get human-readable queue type name."""
        return "Persistent" if config.crawler.enable_persistent_queue else "In-Memory"