"""
Database-backed persistent URL queue implementation.
Extends the in-memory URLQueue with PostgreSQL persistence using existing DatabaseManager.
"""

import asyncio
import json
import time
import uuid
from typing import Dict, Optional, Any, TYPE_CHECKING
from datetime import datetime, timedelta

from crawler.url_management.queue import URLQueue, QueuedURL
from crawler.utils.exceptions import QueueError, DatabaseError

if TYPE_CHECKING:
    from crawler.storage.database import DatabaseManager


from crawler.utils.logging import get_logger


logger = get_logger('persistent_queue')


class PersistentURLQueue(URLQueue):
    """
    PostgreSQL-backed persistent URL queue with all features of URLQueue.
    
    Features:
    - Persistent storage across restarts using existing DatabaseManager
    - Priority-based ordering
    - Domain-based rate limiting
    - Duplicate detection with bloom filter
    - Queue recovery and cleanup
    - Comprehensive metrics and monitoring
    """
    
    def __init__(self, session_id: str, db_manager: 'DatabaseManager', 
                 max_size: int = 100000, enable_bloom_filter: bool = True,
                 enable_persistence: bool = True):
        super().__init__(max_size, enable_bloom_filter)
        
        self.session_id = session_id
        self.db_manager = db_manager
        self.enable_persistence = enable_persistence
        
        # Persistence settings
        self.batch_size = 100  # Batch size for database operations
        self.sync_interval = 5.0  # Seconds between database syncs
        self.cleanup_interval = 300.0  # Seconds between cleanup operations
        
        # Background tasks
        self._sync_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
        # Persistence stats
        self._persistence_stats = {
            'urls_persisted': 0,
            'urls_loaded': 0,
            'sync_operations': 0,
            'cleanup_operations': 0,
            'last_sync_at': None,
            'last_cleanup_at': None,
            'persistence_errors': 0
        }
    
    async def initialize(self) -> None:
        """Initialize the persistent queue."""
        if not self.enable_persistence:
            return
        
        try:
            # Load existing queue state from database
            await self._load_queue_state()
            
            # Start background tasks
            self._sync_task = asyncio.create_task(self._sync_worker())
            self._cleanup_task = asyncio.create_task(self._cleanup_worker())
            
        except Exception as e:
            raise QueueError(f"Failed to initialize persistent queue: {e}")
    
    async def close(self) -> None:
        """Close the persistent queue and cleanup resources."""
        if not self.enable_persistence:
            return
        
        try:
            # Signal shutdown
            self._shutdown_event.set()
            
            # Wait for background tasks to complete
            if self._sync_task:
                try:
                    await asyncio.wait_for(self._sync_task, timeout=10.0)
                except asyncio.TimeoutError:
                    self._sync_task.cancel()
            
            if self._cleanup_task:
                try:
                    await asyncio.wait_for(self._cleanup_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._cleanup_task.cancel()
            
            # Final sync to database
            await self._sync_to_database()
                
        except Exception as e:
            logger.error(f"Error closing persistent queue: {e}")
    
    async def put(self, url: str, depth: int, priority: int = 0,
                  parent_url: Optional[str] = None, **metadata) -> bool:
        """Add URL to queue with persistence."""
        # First add to in-memory queue
        added = await super().put(url, depth, priority, parent_url, **metadata)
        
        if added and self.enable_persistence:
            # Persistence will be handled by sync worker
            pass
        
        return added
    
    async def get(self, timeout: Optional[float] = None) -> Optional[QueuedURL]:
        """Get URL from queue with persistence tracking."""
        queued_url = await super().get(timeout)
        
        if queued_url and self.enable_persistence:
            # Mark as processing in database
            await self.mark_url_processing(queued_url)
        
        return queued_url
    
    async def _load_queue_state(self) -> None:
        """Load queue state from database."""
        try:
            async with self.db_manager.get_connection() as conn:
                # Load pending URLs
                pending_urls = await conn.fetch("""
                    SELECT url, depth, priority, parent_url, discovered_at,
                           scheduled_at, attempts, last_attempt_at, metadata
                    FROM url_queue 
                    WHERE session_id = $1 AND status = 'pending'
                    ORDER BY priority DESC, depth ASC, discovered_at ASC
                """, uuid.UUID(self.session_id))
                
                # Load visited URLs (completed/failed)
                visited_urls = await conn.fetch("""
                    SELECT url_hash FROM url_queue 
                    WHERE session_id = $1 AND status IN ('completed', 'failed')
                """, uuid.UUID(self.session_id))
                
                # Restore in-memory state
                for row in pending_urls:
                    queued_url = QueuedURL(
                        url=row['url'],
                        depth=row['depth'],
                        priority=row['priority'],
                        parent_url=row['parent_url'],
                        discovered_at=row['discovered_at'].timestamp() if row['discovered_at'] else time.time(),
                        scheduled_at=row['scheduled_at'].timestamp() if row['scheduled_at'] else None,
                        attempts=row['attempts'],
                        last_attempt_at=row['last_attempt_at'].timestamp() if row['last_attempt_at'] else None,
                        metadata=json.loads(row['metadata']) if row['metadata'] else {}
                    )
                    
                    # Add to in-memory structures
                    await self._queue.put(queued_url)
                    self._pending_urls[queued_url.url_hash] = queued_url
                    
                    # Add to bloom filter
                    if self._bloom_filter:
                        self._bloom_filter.add(queued_url.url_hash)
                    
                    # Track domain
                    domain = queued_url.domain
                    if domain and domain not in self._discovered_domains:
                        self._discovered_domains.add(domain)
                
                # Restore visited URLs
                for row in visited_urls:
                    self._visited_urls.add(row['url_hash'])
                    if self._bloom_filter:
                        self._bloom_filter.add(row['url_hash'])
                
                self._persistence_stats['urls_loaded'] = len(pending_urls)

                recovered_sessions = await self.recover_interrupted_session()
                
                logger.info(f"Loaded {len(pending_urls) + recovered_sessions} pending URLs and {len(visited_urls)} visited URLs from database")
                
        except Exception as e:
            raise DatabaseError(f"Failed to load queue state: {e}")
    
    async def _sync_to_database(self) -> None:
        """Sync current queue state to database."""
        try:
            async with self.db_manager.get_connection() as conn:
                async with conn.transaction():
                    # Get current pending URLs from memory
                    pending_data = []
                    for queued_url in self._pending_urls.values():
                        pending_data.append((
                            uuid.UUID(self.session_id),
                            queued_url.url,
                            queued_url.url_hash,
                            queued_url.depth,
                            queued_url.priority,
                            queued_url.parent_url,
                            datetime.fromtimestamp(queued_url.discovered_at),
                            datetime.fromtimestamp(queued_url.scheduled_at) if queued_url.scheduled_at else None,
                            queued_url.attempts,
                            datetime.fromtimestamp(queued_url.last_attempt_at) if queued_url.last_attempt_at else None,
                            json.dumps(queued_url.metadata) if queued_url.metadata else None,
                            'pending'
                        ))
                    
                    # Clear existing pending URLs for this session
                    await conn.execute("""
                        DELETE FROM url_queue 
                        WHERE session_id = $1 AND status = 'pending'
                    """, uuid.UUID(self.session_id))
                    
                    # Insert current pending URLs
                    if pending_data:
                        await conn.executemany("""
                            INSERT INTO url_queue (
                                session_id, url, url_hash, depth, priority, parent_url,
                                discovered_at, scheduled_at, attempts, last_attempt_at,
                                metadata, status
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                            ON CONFLICT (session_id, url_hash) DO UPDATE SET
                                priority = EXCLUDED.priority,
                                attempts = EXCLUDED.attempts,
                                last_attempt_at = EXCLUDED.last_attempt_at,
                                metadata = EXCLUDED.metadata,
                                status = EXCLUDED.status,
                                updated_at = NOW()
                        """, pending_data)
                    
                    self._persistence_stats['urls_persisted'] = len(pending_data)
                    self._persistence_stats['sync_operations'] += 1
                    self._persistence_stats['last_sync_at'] = time.time()
                    
        except Exception as e:
            self._persistence_stats['persistence_errors'] += 1
            logger.error(f"Failed to sync queue to database: {e}")
    
    async def _sync_worker(self) -> None:
        """Background worker for periodic database synchronization."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.sync_interval
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                # Perform sync
                await self._sync_to_database()
    
    async def _cleanup_worker(self) -> None:
        """Background worker for periodic cleanup operations."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.cleanup_interval
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                # Perform cleanup
                await self._cleanup_old_entries()
    
    async def _cleanup_old_entries(self) -> None:
        """Clean up old completed/failed URLs from database."""
        try:
            async with self.db_manager.get_connection() as conn:
                # Remove URLs older than 24 hours that are completed/failed
                cutoff_time = datetime.now() - timedelta(hours=24)
                
                result = await conn.execute("""
                    DELETE FROM url_queue 
                    WHERE session_id = $1 
                        AND status IN ('completed', 'failed')
                        AND updated_at < $2
                """, uuid.UUID(self.session_id), cutoff_time)
                
                self._persistence_stats['cleanup_operations'] += 1
                self._persistence_stats['last_cleanup_at'] = time.time()
                
                logger.debug(f"Cleaned up old queue entries: {result}")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old queue entries: {e}")
    
    async def mark_url_processing(self, queued_url: QueuedURL) -> None:
        """Mark URL as being processed."""
        if not self.enable_persistence:
            return
        
        try:
            async with self.db_manager.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO url_queue (
                        session_id, url, url_hash, depth, priority, parent_url,
                        discovered_at, attempts, metadata, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (session_id, url_hash) DO UPDATE SET
                        status = 'processing',
                        updated_at = NOW()
                """, 
                    uuid.UUID(self.session_id),
                    queued_url.url,
                    queued_url.url_hash,
                    queued_url.depth,
                    queued_url.priority,
                    queued_url.parent_url,
                    datetime.fromtimestamp(queued_url.discovered_at),
                    queued_url.attempts,
                    json.dumps(queued_url.metadata) if queued_url.metadata else None,
                    'processing'
                )
        except Exception as e:
            logger.error(f"Failed to mark URL as processing: {e}")
    
    async def mark_url_completed(self, queued_url: QueuedURL) -> None:
        """Mark URL as completed."""
        if not self.enable_persistence:
            return
        
        try:
            async with self.db_manager.get_connection() as conn:
                await conn.execute("""
                    UPDATE url_queue 
                    SET status = 'completed', updated_at = NOW()
                    WHERE session_id = $1 AND url_hash = $2
                """, uuid.UUID(self.session_id), queued_url.url_hash)
        except Exception as e:
            logger.error(f"Failed to mark URL as completed: {e}")
    
    async def mark_url_failed(self, queued_url: QueuedURL, error_message: Optional[str] = None) -> None:
        """Mark URL as failed."""
        if not self.enable_persistence:
            return
        
        try:
            async with self.db_manager.get_connection() as conn:
                await conn.execute("""
                    UPDATE url_queue 
                    SET status = 'failed', error_message = $3, updated_at = NOW()
                    WHERE session_id = $1 AND url_hash = $2
                """, uuid.UUID(self.session_id), queued_url.url_hash, error_message)
        except Exception as e:
            logger.error(f"Failed to mark URL as failed: {e}")
    
    def get_persistence_stats(self) -> Dict[str, Any]:
        """Get persistence-related statistics."""
        return {
            **self.get_stats(),
            'persistence': self._persistence_stats.copy()
        }
    
    async def recover_interrupted_session(self) -> int:
        """Recover URLs that were being processed when session was interrupted."""
        if not self.enable_persistence:
            return 0
        
        try:
            async with self.db_manager.get_connection() as conn:
                # Find URLs that were marked as processing but never completed
                interrupted_urls = await conn.fetch("""
                    SELECT url, depth, priority, parent_url, discovered_at,
                           scheduled_at, attempts, last_attempt_at, metadata
                    FROM url_queue 
                    WHERE session_id = $1 
                        AND status = 'processing'
                        AND updated_at < NOW()
                """, uuid.UUID(self.session_id))
                
                # Reset them to pending
                if interrupted_urls:
                    await conn.execute("""
                        UPDATE url_queue 
                        SET status = 'pending', updated_at = NOW()
                        WHERE session_id = $1 
                            AND status = 'processing'
                            AND updated_at < NOW()
                    """, uuid.UUID(self.session_id))
                
                logger.info(f"Recovered {len(interrupted_urls)} interrupted URLs")
                
                return len(interrupted_urls)
                
        except Exception as e:
            logger.error(f"Failed to recover interrupted session: {e}")
            return 0
    
    async def get_queue_statistics(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics from database."""
        if not self.enable_persistence:
            return self.get_persistence_stats()
        
        try:
            async with self.db_manager.get_connection() as conn:
                # Get queue status counts
                status_counts = await conn.fetch("""
                    SELECT status, COUNT(*) as count
                    FROM url_queue 
                    WHERE session_id = $1
                    GROUP BY status
                """, uuid.UUID(self.session_id))
                
                # Get priority distribution
                priority_dist = await conn.fetch("""
                    SELECT priority, COUNT(*) as count
                    FROM url_queue 
                    WHERE session_id = $1 AND status = 'pending'
                    GROUP BY priority
                    ORDER BY priority DESC
                """, uuid.UUID(self.session_id))
                
                # Get depth distribution
                depth_dist = await conn.fetch("""
                    SELECT depth, COUNT(*) as count
                    FROM url_queue 
                    WHERE session_id = $1 AND status = 'pending'
                    GROUP BY depth
                    ORDER BY depth
                """, uuid.UUID(self.session_id))
                
                # Get domain distribution
                domain_dist = await conn.fetch("""
                    SELECT 
                        CASE 
                            WHEN url ~ '^https?://([^/]+)' THEN 
                                substring(url from '^https?://([^/]+)')
                            ELSE 'unknown'
                        END as domain,
                        COUNT(*) as count
                    FROM url_queue 
                    WHERE session_id = $1 AND status = 'pending'
                    GROUP BY domain
                    ORDER BY count DESC
                    LIMIT 10
                """, uuid.UUID(self.session_id))
                
                return {
                    **self.get_persistence_stats(),
                    'database_stats': {
                        'status_counts': {row['status']: row['count'] for row in status_counts},
                        'priority_distribution': {row['priority']: row['count'] for row in priority_dist},
                        'depth_distribution': {row['depth']: row['count'] for row in depth_dist},
                        'top_domains': {row['domain']: row['count'] for row in domain_dist}
                    }
                }
                
        except Exception as e:
            logger.error(f"Failed to get queue statistics: {e}")
            return self.get_persistence_stats()
    
    async def clear_session_queue(self) -> int:
        """Clear all queue entries for this session."""
        if not self.enable_persistence:
            return 0
        
        try:
            async with self.db_manager.get_connection() as conn:
                result = await conn.execute("""
                    DELETE FROM url_queue WHERE session_id = $1
                """, uuid.UUID(self.session_id))
                
                # Also clear in-memory state
                await self.clear()
                
                logger.info(f"Cleared queue for session {self.session_id}")
                
                # Extract count from result string like "DELETE 123"
                return int(result.split()[-1]) if result else 0
                
        except Exception as e:
            logger.error(f"Failed to clear session queue: {e}")
            return 0