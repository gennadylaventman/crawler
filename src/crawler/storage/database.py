"""
Database management for the web crawler system with migration support.
"""

import asyncio
import time
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from contextlib import asynccontextmanager

import asyncpg

from crawler.utils.config import DatabaseConfig
from crawler.utils.exceptions import DatabaseError
from crawler.core.session import CrawlSession
from crawler.monitoring.metrics import PageMetrics
from crawler.storage.migrations import MigrationManager

if TYPE_CHECKING:
    from crawler.core.engine import CrawlResult


class DatabaseManager:
    """
    Database manager for PostgreSQL operations with connection pooling and migrations.
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        self.migration_manager: Optional[MigrationManager] = None
        self._initialized = False
    
    async def initialize(self, auto_migrate: bool = True) -> None:
        """Initialize database connection pool and run migrations."""
        if self._initialized:
            return
        
        try:
            self.pool = await asyncpg.create_pool(
                self.config.url,
                min_size=5,
                max_size=self.config.pool_size,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                command_timeout=self.config.pool_timeout,
                server_settings={
                    'application_name': 'webcrawler',
                    'tcp_keepalives_idle': '600',
                    'tcp_keepalives_interval': '30',
                    'tcp_keepalives_count': '3',
                }
            )
            
            # Initialize migration manager
            self.migration_manager = MigrationManager(self.config)
            await self.migration_manager.initialize()
            
            # Run migrations if requested
            if auto_migrate:
                await self.migrate_to_latest()
            
            self._initialized = True
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {e}")
    
    async def close(self) -> None:
        """Close database connection pool."""
        if self.migration_manager:
            await self.migration_manager.close()
        
        if self.pool:
            await self.pool.close()
            self._initialized = False
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool."""
        if not self.pool:
            raise DatabaseError("Database not initialized")
        
        async with self.pool.acquire() as connection:
            yield connection
    
    # Migration methods
    async def migrate_to_latest(self) -> bool:
        """Apply all pending migrations."""
        if not self.migration_manager:
            raise DatabaseError("Migration manager not initialized")
        return await self.migration_manager.migrate_to_latest()
    
    async def migrate_to_version(self, version: str) -> bool:
        """Migrate to a specific version."""
        if not self.migration_manager:
            raise DatabaseError("Migration manager not initialized")
        return await self.migration_manager.migrate_to_version(version)
    
    async def rollback_to_version(self, version: str) -> bool:
        """Rollback to a specific version."""
        if not self.migration_manager:
            raise DatabaseError("Migration manager not initialized")
        return await self.migration_manager.rollback_to_version(version)
    
    async def recreate_schema(self) -> bool:
        """Drop all tables and recreate from scratch."""
        if not self.migration_manager:
            raise DatabaseError("Migration manager not initialized")
        return await self.migration_manager.recreate_schema()
    
    async def get_migration_status(self) -> Dict[str, Any]:
        """Get migration status."""
        if not self.migration_manager:
            raise DatabaseError("Migration manager not initialized")
        return await self.migration_manager.get_migration_status()
    
    # Crawl session methods
    async def create_crawl_session(self, session: CrawlSession) -> str:
        """Create a new crawl session in the database."""
        try:
            async with self.get_connection() as conn:
                session_id = await conn.fetchval(
                    """
                    INSERT INTO crawl_sessions (
                        id, name, start_url, max_depth, max_pages, 
                        concurrent_workers, rate_limit_delay, status,
                        started_at, configuration
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    RETURNING id
                    """,
                    uuid.UUID(session.session_id),
                    session.name,
                    session.config.start_urls[0] if session.config.start_urls else "",
                    session.config.crawler.max_depth,
                    session.config.crawler.max_pages,
                    session.config.crawler.concurrent_workers,
                    session.config.crawler.rate_limit_delay,
                    session.status,
                    datetime.now(),
                    json.dumps(session.config.dict())
                )
                return str(session_id)
        except Exception as e:
            raise DatabaseError(f"Failed to create crawl session: {e}")
    
    async def update_crawl_session(self, session: CrawlSession) -> None:
        """Update crawl session statistics."""
        try:
            async with self.get_connection() as conn:
                await conn.execute(
                    """
                    UPDATE crawl_sessions SET
                        status = $2,
                        completed_at = $3,
                        total_pages_crawled = $4,
                        total_words_found = $5,
                        error_count = $6
                    WHERE id = $1
                    """,
                    uuid.UUID(session.session_id),
                    session.status,
                    datetime.now() if session.status == "completed" else None,
                    session.pages_crawled,
                    session.total_words,
                    session.pages_failed
                )
        except Exception as e:
            raise DatabaseError(f"Failed to update crawl session: {e}")
    
    async def get_crawl_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get crawl session by ID."""
        try:
            async with self.get_connection() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT * FROM crawl_sessions WHERE id = $1
                    """,
                    uuid.UUID(session_id)
                )
                return dict(result) if result else None
        except Exception as e:
            raise DatabaseError(f"Failed to get crawl session: {e}")
    
    # Page storage methods
    async def store_page_result(self, result: 'CrawlResult', session_id: str) -> str:
        """Store page crawl result in the database."""
        try:
            async with self.get_connection() as conn:
                # Generate URL hash
                import hashlib
                url_hash = hashlib.md5(result.url.encode()).hexdigest()
                
                # Prepare metrics data
                metrics = result.metrics
                
                # Insert page record with comprehensive metrics
                page_id = await conn.fetchval(
                    """
                    INSERT INTO pages (
                        session_id, url, url_hash, depth, status_code,
                        content_type, title, total_words, 
                        
                        -- Timing metrics
                        dns_lookup_time, tcp_connect_time, tls_handshake_time,
                        server_response_time, content_download_time, total_network_time,
                        html_parse_time, text_extraction_time, text_cleaning_time,
                        word_tokenization_time, word_counting_time, link_extraction_time,
                        total_processing_time, db_insert_time, total_db_time,
                        queue_wait_time, total_page_time,
                        
                        -- Content metrics
                        raw_content_size, extracted_text_size, unique_words,
                        average_word_length,
                        
                        -- Network metrics
                        connection_reused,
                        
                        -- Error information
                        error_message, retry_count
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8,
                        $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21,
                        $22, $23, $24, $25,
                        $26, $27, $28, $29,
                        $30, $31, $32
                    )
                    RETURNING id
                    """,
                    uuid.UUID(session_id), result.url, url_hash, result.depth, result.status_code,
                    metrics.content_type if metrics else None,
                    result.title, result.word_count,
                    
                    # Timing metrics
                    metrics.dns_lookup_time if metrics else None,
                    metrics.tcp_connect_time if metrics else None,
                    metrics.tls_handshake_time if metrics else None,
                    metrics.server_response_time if metrics else None,
                    metrics.content_download_time if metrics else None,
                    metrics.total_network_time if metrics else None,
                    metrics.html_parse_time if metrics else None,
                    metrics.text_extraction_time if metrics else None,
                    metrics.text_cleaning_time if metrics else None,
                    metrics.word_tokenization_time if metrics else None,
                    metrics.word_counting_time if metrics else None,
                    metrics.link_extraction_time if metrics else None,
                    metrics.total_processing_time if metrics else None,
                    metrics.db_insert_time if metrics else None,
                    metrics.total_db_time if metrics else None,
                    metrics.queue_wait_time if metrics else None,
                    metrics.total_time if metrics else None,
                    
                    # Content metrics
                    metrics.raw_content_size if metrics else None,
                    metrics.extracted_text_size if metrics else None,
                    metrics.unique_words if metrics else None,
                    metrics.average_word_length if metrics else None,
                    
                    # Network metrics
                    metrics.connection_reused if metrics else None,
                    
                    # Error information
                    result.error,
                    metrics.retry_count if metrics else 0
                )
                
                # Store links if any
                if result.links:
                    await self._store_links(conn, session_id, page_id, result.links)
                
                return str(page_id)
                
        except Exception as e:
            raise DatabaseError(f"Failed to store page result: {e}")
    
    async def _store_links(self, conn, session_id: str, page_id: uuid.UUID, links: List[str]) -> None:
        """Store discovered links."""
        import hashlib
        from urllib.parse import urlparse
        
        link_data = []
        for link in links:
            link_hash = hashlib.md5(link.encode()).hexdigest()
            
            # Determine link type (basic implementation)
            try:
                parsed = urlparse(link)
                link_type = "external" if parsed.netloc else "internal"
            except:
                link_type = "unknown"
            
            link_data.append((
                uuid.UUID(session_id),
                page_id,
                link,
                link_hash,
                link_type,
                False
            ))
        
        if link_data:
            await conn.executemany(
                """
                INSERT INTO links (
                    session_id, source_page_id, target_url, 
                    target_url_hash, link_type, is_crawled
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT DO NOTHING
                """,
                link_data
            )
    
    async def store_word_frequencies(self, session_id: str, page_id: str, word_counts: Dict[str, int]) -> None:
        """Store word frequency data."""
        try:
            async with self.get_connection() as conn:
                word_data = []
                for word, count in word_counts.items():
                    word_data.append((
                        uuid.UUID(page_id),
                        uuid.UUID(session_id),
                        word,
                        count,
                        len(word),
                        False  # is_stopword - would be determined by analysis
                    ))
                
                if word_data:
                    await conn.executemany(
                        """
                        INSERT INTO word_frequencies (
                            page_id, session_id, word, frequency, word_length, is_stopword
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        word_data
                    )
        except Exception as e:
            raise DatabaseError(f"Failed to store word frequencies: {e}")
    
    async def store_error_event(self, session_id: str, url: str, error_message: str,
                               depth: int = 0, operation_name: str = "page_processing",
                               page_id: Optional[str] = None) -> str:
        """Store error event in the database."""
        try:
            async with self.get_connection() as conn:
                error_id = await conn.fetchval(
                    """
                    INSERT INTO error_events (
                        id, session_id, page_id, occurred_at, error_type, error_category,
                        error_severity, error_message, url, depth, operation_name
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id
                    """,
                    uuid.uuid4(),
                    uuid.UUID(session_id),
                    uuid.UUID(page_id) if page_id else None,
                    datetime.now(),
                    'processing_failure',
                    'crawler',
                    'error',
                    error_message,
                    url,
                    depth,
                    operation_name
                )
                return str(error_id)
        except Exception as e:
            raise DatabaseError(f"Failed to store error event: {e}")
    
    # Analytics and reporting methods
    async def get_session_statistics(self, session_id: str) -> Dict[str, Any]:
        """Get comprehensive session statistics."""
        try:
            async with self.get_connection() as conn:
                # Get session info
                session_info = await conn.fetchrow(
                    """
                    SELECT 
                        name, status, created_at, started_at, completed_at,
                        total_pages_crawled, total_words_found, error_count
                    FROM crawl_sessions 
                    WHERE id = $1
                    """,
                    uuid.UUID(session_id)
                )
                
                if not session_info:
                    return {}
                
                # Get page statistics with enhanced metrics
                page_stats = await conn.fetchrow(
                    """
                    SELECT 
                        COUNT(*) as pages_processed,
                        AVG(server_response_time) as avg_response_time,
                        AVG(total_processing_time) as avg_processing_time,
                        AVG(total_page_time) as avg_total_time,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY server_response_time) as p50_response_time,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY server_response_time) as p95_response_time,
                        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY server_response_time) as p99_response_time,
                        MAX(depth) as max_depth_reached,
                        SUM(total_words) as total_words,
                        AVG(total_words) as avg_words_per_page,
                        COUNT(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 END) as error_pages,
                        COUNT(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 END)::DECIMAL / COUNT(*) * 100 as error_rate,
                        SUM(raw_content_size) as total_bytes_processed,
                        AVG(raw_content_size) as avg_page_size
                    FROM pages 
                    WHERE session_id = $1
                    """,
                    uuid.UUID(session_id)
                )
                
                # Get top words
                top_words = await conn.fetch(
                    """
                    SELECT word, SUM(frequency) as total_frequency,
                           COUNT(DISTINCT page_id) as pages_containing_word
                    FROM word_frequencies 
                    WHERE session_id = $1
                    GROUP BY word
                    ORDER BY total_frequency DESC
                    LIMIT 20
                    """,
                    uuid.UUID(session_id)
                )
                
                # Get performance breakdown
                timing_breakdown = await conn.fetchrow(
                    """
                    SELECT 
                        AVG(dns_lookup_time) as avg_dns_time,
                        AVG(tcp_connect_time) as avg_connect_time,
                        AVG(server_response_time) as avg_server_time,
                        AVG(html_parse_time) as avg_parse_time,
                        AVG(text_extraction_time) as avg_extraction_time,
                        AVG(word_counting_time) as avg_counting_time,
                        AVG(total_processing_time) as avg_processing_time,
                        AVG(db_insert_time) as avg_db_time
                    FROM pages 
                    WHERE session_id = $1
                    """,
                    uuid.UUID(session_id)
                )
                
                return {
                    'session_info': dict(session_info),
                    'page_statistics': dict(page_stats) if page_stats else {},
                    'timing_breakdown': dict(timing_breakdown) if timing_breakdown else {},
                    'top_words': [
                        {
                            'word': row['word'], 
                            'frequency': row['total_frequency'],
                            'pages': row['pages_containing_word']
                        } 
                        for row in top_words
                    ]
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to get session statistics: {e}")
    
    async def get_performance_metrics(self, session_id: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Get performance metrics for the last N hours."""
        try:
            async with self.get_connection() as conn:
                metrics = await conn.fetch(
                    """
                    SELECT 
                        DATE_TRUNC('hour', crawled_at) as hour,
                        COUNT(*) as pages_crawled,
                        AVG(server_response_time) as avg_response_time,
                        AVG(total_processing_time) as avg_processing_time,
                        AVG(total_page_time) as avg_total_time,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY server_response_time) as p95_response_time,
                        COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_count,
                        COUNT(CASE WHEN status_code >= 400 THEN 1 END)::DECIMAL / COUNT(*) * 100 as error_rate,
                        SUM(total_words) as words_processed,
                        SUM(raw_content_size) as bytes_processed
                    FROM pages 
                    WHERE session_id = $1 
                        AND crawled_at >= NOW() - INTERVAL '%s hours'
                    GROUP BY DATE_TRUNC('hour', crawled_at)
                    ORDER BY hour
                    """,
                    uuid.UUID(session_id),
                    hours
                )
                
                return [dict(row) for row in metrics]
                
        except Exception as e:
            raise DatabaseError(f"Failed to get performance metrics: {e}")
    
    async def get_word_frequency_analysis(self, session_id: str, limit: int = 100) -> Dict[str, Any]:
        """Get comprehensive word frequency analysis."""
        try:
            async with self.get_connection() as conn:
                # Get top words with statistics
                top_words = await conn.fetch(
                    """
                    SELECT 
                        word,
                        SUM(frequency) as total_frequency,
                        COUNT(DISTINCT page_id) as pages_containing_word,
                        AVG(frequency) as avg_frequency_per_page,
                        word_length,
                        MAX(frequency) as max_frequency_on_page
                    FROM word_frequencies 
                    WHERE session_id = $1
                    GROUP BY word, word_length
                    ORDER BY total_frequency DESC
                    LIMIT $2
                    """,
                    uuid.UUID(session_id),
                    limit
                )
                
                # Get word length distribution
                length_distribution = await conn.fetch(
                    """
                    SELECT 
                        word_length,
                        COUNT(DISTINCT word) as unique_words,
                        SUM(frequency) as total_occurrences
                    FROM word_frequencies 
                    WHERE session_id = $1
                    GROUP BY word_length
                    ORDER BY word_length
                    """,
                    uuid.UUID(session_id)
                )
                
                # Get overall statistics
                overall_stats = await conn.fetchrow(
                    """
                    SELECT 
                        COUNT(DISTINCT word) as total_unique_words,
                        SUM(frequency) as total_word_occurrences,
                        AVG(word_length) as avg_word_length,
                        COUNT(DISTINCT page_id) as pages_with_words
                    FROM word_frequencies 
                    WHERE session_id = $1
                    """,
                    uuid.UUID(session_id)
                )
                
                return {
                    'top_words': [dict(row) for row in top_words],
                    'length_distribution': [dict(row) for row in length_distribution],
                    'overall_statistics': dict(overall_stats) if overall_stats else {}
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to get word frequency analysis: {e}")
    
    # Maintenance methods
    async def cleanup_old_sessions(self, days_old: int = 30) -> int:
        """Clean up old completed sessions."""
        try:
            async with self.get_connection() as conn:
                result = await conn.execute(
                    """
                    DELETE FROM crawl_sessions 
                    WHERE created_at < NOW() - INTERVAL '%s days'
                        AND status IN ('completed', 'failed')
                    """,
                    days_old
                )
                
                # Extract number of deleted rows from result
                return int(result.split()[-1]) if result else 0
                
        except Exception as e:
            raise DatabaseError(f"Failed to cleanup old sessions: {e}")
    
    async def get_database_size_info(self) -> Dict[str, Any]:
        """Get database size information."""
        try:
            async with self.get_connection() as conn:
                # Get table sizes
                table_sizes = await conn.fetch("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                """)
                
                # Get index usage
                index_usage = await conn.fetch("""
                    SELECT 
                        schemaname,
                        tablename,
                        indexname,
                        idx_scan,
                        idx_tup_read,
                        idx_tup_fetch
                    FROM pg_stat_user_indexes
                    ORDER BY idx_scan DESC
                    LIMIT 20
                """)
                
                return {
                    'table_sizes': [dict(row) for row in table_sizes],
                    'index_usage': [dict(row) for row in index_usage]
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to get database size info: {e}")
    
    # Queue persistence methods
    async def create_persistent_queue(self, session_id: str) -> 'PersistentURLQueue':
        """Create a persistent URL queue for the given session."""
        from crawler.storage.persistent_queue import PersistentURLQueue
        
        queue = PersistentURLQueue(
            session_id=session_id,
            db_manager=self,
            enable_persistence=True
        )
        await queue.initialize()
        return queue
    
    async def get_queue_statistics(self, session_id: str) -> Dict[str, Any]:
        """Get comprehensive queue statistics for a session."""
        try:
            async with self.get_connection() as conn:
                # Get queue status counts
                status_counts = await conn.fetch("""
                    SELECT status, COUNT(*) as count
                    FROM url_queue
                    WHERE session_id = $1
                    GROUP BY status
                """, uuid.UUID(session_id))
                
                # Get priority distribution
                priority_dist = await conn.fetch("""
                    SELECT priority, COUNT(*) as count
                    FROM url_queue
                    WHERE session_id = $1 AND status = 'pending'
                    GROUP BY priority
                    ORDER BY priority DESC
                """, uuid.UUID(session_id))
                
                # Get depth distribution
                depth_dist = await conn.fetch("""
                    SELECT depth, COUNT(*) as count
                    FROM url_queue
                    WHERE session_id = $1 AND status = 'pending'
                    GROUP BY depth
                    ORDER BY depth
                """, uuid.UUID(session_id))
                
                # Get recent activity
                recent_activity = await conn.fetch("""
                    SELECT
                        DATE_TRUNC('hour', updated_at) as hour,
                        status,
                        COUNT(*) as count
                    FROM url_queue
                    WHERE session_id = $1
                        AND updated_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY DATE_TRUNC('hour', updated_at), status
                    ORDER BY hour DESC, status
                """, uuid.UUID(session_id))
                
                return {
                    'status_counts': {row['status']: row['count'] for row in status_counts},
                    'priority_distribution': {row['priority']: row['count'] for row in priority_dist},
                    'depth_distribution': {row['depth']: row['count'] for row in depth_dist},
                    'recent_activity': [
                        {
                            'hour': row['hour'],
                            'status': row['status'],
                            'count': row['count']
                        } for row in recent_activity
                    ]
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to get queue statistics: {e}")
    
    async def recover_interrupted_queue_urls(self, session_id: str, timeout_minutes: int = 5) -> int:
        """Recover URLs that were being processed when session was interrupted."""
        try:
            async with self.get_connection() as conn:
                # Find URLs that were marked as processing but never completed
                result = await conn.execute("""
                    UPDATE url_queue
                    SET status = 'pending', updated_at = NOW()
                    WHERE session_id = $1
                        AND status = 'processing'
                        AND updated_at < NOW() - INTERVAL '%s minutes'
                """, uuid.UUID(session_id), timeout_minutes)
                
                # Extract count from result string like "UPDATE 123"
                recovered_count = int(result.split()[-1]) if result else 0
                
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"Recovered {recovered_count} interrupted URLs for session {session_id}")
                
                return recovered_count
                
        except Exception as e:
            raise DatabaseError(f"Failed to recover interrupted queue URLs: {e}")
    
    async def cleanup_old_queue_entries(self, session_id: str, hours_old: int = 24) -> int:
        """Clean up old completed/failed URLs from queue."""
        try:
            async with self.get_connection() as conn:
                result = await conn.execute("""
                    DELETE FROM url_queue
                    WHERE session_id = $1
                        AND status IN ('completed', 'failed')
                        AND updated_at < NOW() - INTERVAL '%s hours'
                """, uuid.UUID(session_id), hours_old)
                
                # Extract count from result string
                cleaned_count = int(result.split()[-1]) if result else 0
                
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"Cleaned up {cleaned_count} old queue entries for session {session_id}")
                
                return cleaned_count
                
        except Exception as e:
            raise DatabaseError(f"Failed to cleanup old queue entries: {e}")
    
    async def clear_session_queue(self, session_id: str) -> int:
        """Clear all queue entries for a session."""
        try:
            async with self.get_connection() as conn:
                result = await conn.execute("""
                    DELETE FROM url_queue WHERE session_id = $1
                """, uuid.UUID(session_id))
                
                # Extract count from result string
                cleared_count = int(result.split()[-1]) if result else 0
                
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"Cleared {cleared_count} queue entries for session {session_id}")
                
                return cleared_count
                
        except Exception as e:
            raise DatabaseError(f"Failed to clear session queue: {e}")
    
    async def get_queue_health_metrics(self) -> Dict[str, Any]:
        """Get overall queue health metrics across all sessions."""
        try:
            async with self.get_connection() as conn:
                # Get overall queue statistics
                overall_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*) as total_urls,
                        COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_urls,
                        COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_urls,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_urls,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_urls,
                        COUNT(DISTINCT session_id) as active_sessions,
                        AVG(attempts) as avg_attempts,
                        MAX(updated_at) as last_activity
                    FROM url_queue
                """)
                
                # Get sessions with stuck processing URLs
                stuck_sessions = await conn.fetch("""
                    SELECT
                        session_id,
                        COUNT(*) as stuck_urls,
                        MIN(updated_at) as oldest_stuck
                    FROM url_queue
                    WHERE status = 'processing'
                        AND updated_at < NOW() - INTERVAL '10 minutes'
                    GROUP BY session_id
                    ORDER BY stuck_urls DESC
                """)
                
                # Get queue size by session
                session_sizes = await conn.fetch("""
                    SELECT
                        s.name as session_name,
                        s.id as session_id,
                        COUNT(q.*) as queue_size,
                        COUNT(CASE WHEN q.status = 'pending' THEN 1 END) as pending_count
                    FROM crawl_sessions s
                    LEFT JOIN url_queue q ON s.id = q.session_id
                    WHERE s.status IN ('running', 'pending')
                    GROUP BY s.id, s.name
                    ORDER BY queue_size DESC
                    LIMIT 10
                """)
                
                return {
                    'overall_statistics': dict(overall_stats) if overall_stats else {},
                    'stuck_sessions': [
                        {
                            'session_id': str(row['session_id']),
                            'stuck_urls': row['stuck_urls'],
                            'oldest_stuck': row['oldest_stuck']
                        } for row in stuck_sessions
                    ],
                    'top_sessions_by_queue_size': [
                        {
                            'session_name': row['session_name'],
                            'session_id': str(row['session_id']),
                            'queue_size': row['queue_size'],
                            'pending_count': row['pending_count']
                        } for row in session_sizes
                    ]
                }
                
        except Exception as e:
            raise DatabaseError(f"Failed to get queue health metrics: {e}")