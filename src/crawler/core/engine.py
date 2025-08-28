"""
Core crawler engine with asyncio for concurrent processing.
"""

import asyncio
import time
import hashlib
from typing import Dict, List, Optional, Set, Any, Tuple, TYPE_CHECKING
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import aiohttp

from crawler.utils.config import CrawlConfig
from crawler.utils.exceptions import CrawlerError, NetworkError, ContentError
from crawler.utils.logging import get_logger
from crawler.monitoring.metrics import PageMetrics
from crawler.content.processor import ContentProcessor
from crawler.url_management.queue import URLQueue
from crawler.url_management.robots import RobotsChecker, SitemapParser
from crawler.url_management.validator import URLValidator
from crawler.core.worker import WorkerPool
from crawler.core.session import CrawlSession
from crawler.core.queue_factory import QueueFactory

if TYPE_CHECKING:
    from crawler.storage.database import DatabaseManager


logger = get_logger('engine')


@dataclass
class CrawlResult:
    """Result of crawling a single page."""
    url: str
    status_code: int
    content: Optional[str] = None
    title: Optional[str] = None
    links: List[str] = field(default_factory=list)
    word_count: int = 0
    error: Optional[str] = None
    metrics: Optional[PageMetrics] = None
    depth: int = 0




class CrawlerEngine:
    """
    Main crawler engine with asyncio for concurrent processing.
    
    Features:
    - Asynchronous HTTP requests with connection pooling
    - Configurable concurrency limits
    - Rate limiting and politeness policies
    - Comprehensive metrics collection
    - Error handling and retry mechanisms
    """
    
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.visited_urls: Set[str] = set()
        self.url_queue: Optional[URLQueue] = None  # Will be initialized in start_crawl
        self.db_manager: Optional['DatabaseManager'] = None
        self.content_processor: Optional[ContentProcessor] = None
        self.crawl_session: Optional[CrawlSession] = None
        self.robots_checker: Optional[RobotsChecker] = None
        self.sitemap_parser: Optional[SitemapParser] = None
        self.worker_pool: Optional[WorkerPool] = None
        self.url_validator: Optional[URLValidator] = None
        
        # Rate limiting
        self._last_request_time: Dict[str, float] = {}
        self._request_lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()
    
    async def initialize(self) -> None:
        """Initialize the crawler engine."""
        logger.info("Initializing crawler engine")
        
        # Create HTTP session with optimized settings and increased header limits
        connector = aiohttp.TCPConnector(
            limit=self.config.crawler.max_connections,
            limit_per_host=self.config.crawler.max_connections_per_host,
            ttl_dns_cache=self.config.crawler.dns_cache_ttl,
            use_dns_cache=True,
            keepalive_timeout=self.config.crawler.keepalive_timeout,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(
            total=self.config.crawler.request_timeout,
            connect=10,
            sock_read=10
        )
        
        headers = {
            'User-Agent': self.config.crawler.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Create session with increased header size limits (20KB)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
            connector_owner=False,  # Don't close connector automatically
            read_bufsize=65536,     # 64KB read buffer
            max_line_size=20480,    # 20KB max line size (for headers)
            max_field_size=20480    # 20KB max field size (for header values)
        )
        
        # Initialize semaphore for concurrency control
        self.semaphore = asyncio.Semaphore(self.config.crawler.concurrent_workers)
        
        # Initialize components
        from crawler.storage.database import DatabaseManager
        
        self.db_manager = DatabaseManager(self.config.database)
        self.content_processor = ContentProcessor(self.config.content)
        self.robots_checker = RobotsChecker(
            user_agent=self.config.crawler.user_agent,
            cache_ttl=3600
        )
        self.sitemap_parser = SitemapParser()
        self.url_validator = URLValidator()
        
        # Initialize worker pool with proper config format
        worker_config = {
            'crawler': {
                'request_timeout': self.config.crawler.request_timeout,
                'max_depth': self.config.crawler.max_depth,
                'max_retries': self.config.crawler.max_retries,
                'user_agent': self.config.crawler.user_agent
            },
            'content': {
                'allowed_content_types': self.config.content.allowed_content_types,
                'max_page_size': self.config.content.max_page_size
            }
        }
        
        self.worker_pool = WorkerPool(
            session=self.session,
            config=worker_config,
            pool_size=self.config.crawler.concurrent_workers
        )
        
        logger.info("Initializing database manager")
        await self.db_manager.initialize()
        
        logger.info("Initializing robots checker and sitemap parser")
        await self.robots_checker._create_session()
        await self.sitemap_parser._create_session()
        
        logger.info(f"Starting worker pool with {self.config.crawler.concurrent_workers} workers")
        await self.worker_pool.start()
        
        logger.info("Crawler engine initialization completed successfully")
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Starting crawler engine cleanup")
        
        if self.session:
            logger.debug("Closing HTTP session")
            await self.session.close()
        
        # Clean up persistent queue if needed
        if self.url_queue and hasattr(self.url_queue, 'cleanup'):
            logger.debug("Cleaning up URL queue")
            await self.url_queue.cleanup()
        
        if self.db_manager:
            logger.debug("Closing database manager")
            await self.db_manager.close()
        
        if self.robots_checker:
            logger.debug("Closing robots checker session")
            await self.robots_checker._close_session()
        
        if self.sitemap_parser:
            logger.debug("Closing sitemap parser session")
            await self.sitemap_parser._close_session()
        
        if self.worker_pool:
            logger.debug("Stopping worker pool")
            await self.worker_pool.stop()
        
        logger.info("Crawler engine cleanup completed")
    
    async def start_crawl(self, start_urls: List[str], session_name: str = "default") -> str:
        """
        Start a new crawling session.
        
        Args:
            start_urls: List of URLs to start crawling from
            session_name: Name for this crawling session
            
        Returns:
            Session ID
        """
        logger.info(f"Starting crawl session '{session_name}' with {len(start_urls)} start URLs")
        
        # Create crawl session
        session_id = hashlib.md5(f"{session_name}_{time.time()}".encode()).hexdigest()
        logger.debug(f"Generated session ID: {session_id}")
        
        self.crawl_session = CrawlSession(
            session_id=session_id,
            name=session_name,
            config=self.config
        )
        
        # Store session in database
        logger.debug("Storing session in database")
        await self.db_manager.create_crawl_session(self.crawl_session)
        
        # Initialize URL queue using factory (after session is created)
        queue_type = QueueFactory.get_queue_type_name(self.config)
        logger.info(f"Initializing {queue_type} queue for session")
        self.url_queue = QueueFactory.create_queue(
            config=self.config,
            session_id=self.crawl_session.session_id,
            db_manager=self.db_manager
        )
        
        # Discover sitemaps and add URLs
        logger.info("Discovering sitemap URLs")
        await self._discover_and_add_sitemap_urls(start_urls)
        
        # Add start URLs to queue
        logger.info(f"Adding {len(start_urls)} start URLs to queue")
        for url in start_urls:
            try:
                # Normalize URL before adding to queue
                normalized_url = self.url_validator.normalize_url(url)
                await self.url_queue.put(normalized_url, depth=0, priority=10)  # High priority for start URLs
                logger.debug(f"Normalized and queued start URL: {url} -> {normalized_url}")
            except Exception as e:
                logger.warning(f"Failed to normalize start URL {url}: {e}, using original URL")
                await self.url_queue.put(url, depth=0, priority=10)
        
        # Start crawling
        logger.info("Starting main crawl loop")
        await self._crawl_loop()
        
        # Update session status
        logger.info("Crawl completed, updating session status")
        self.crawl_session.status = "completed"
        await self.db_manager.update_crawl_session(self.crawl_session)
        
        logger.info(f"Crawl session '{session_name}' completed successfully")
        return session_id
    
    async def _crawl_loop(self) -> None:
        """Main crawling loop using WorkerPool."""
        if not self.worker_pool:
            raise CrawlerError("Worker pool not initialized")
        
        submitted_tasks = 0
        processed_results = 0
        pending_tasks = 0
        
        logger.info(f"Starting crawl loop with max_pages={self.config.crawler.max_pages}")
        
        # Process URLs until queue is empty or limits reached
        loop_iteration = 0
        max_iterations = 5000  # Safety limit to prevent infinite loops
        last_queue_size = self.url_queue.size()
        stalled_iterations = 0
        
        while (not self.url_queue.empty() or pending_tasks > 0) and self._should_continue_crawling() and loop_iteration < max_iterations:
            loop_iteration += 1
            current_queue_size = self.url_queue.size()
            
            # Detect stalled queue (no progress)
            if current_queue_size == last_queue_size and pending_tasks == 0:
                stalled_iterations += 1
                if stalled_iterations >= 5:
                    break
            else:
                stalled_iterations = 0
            
            last_queue_size = current_queue_size
            
            
            # Submit tasks up to worker pool size to avoid blocking
            inner_loop_iteration = 0
            while (pending_tasks < self.config.crawler.concurrent_workers and
                   not self.url_queue.empty() and
                   self._should_continue_crawling()):
                
                inner_loop_iteration += 1
                
                # Get URL from queue with proper timeout that accounts for rate limiting
                # Timeout should be at least 2x the rate limit delay to allow for waiting
                queue_timeout = max(5.0, self.config.crawler.rate_limit_delay * 2)
                queued_url = await self.url_queue.get_with_rate_limit(
                    domain_delay=self.config.crawler.rate_limit_delay,
                    timeout=queue_timeout
                )
                
                
                if not queued_url:
                    break
                
                # Check robots.txt compliance
                if not await self._check_robots_compliance(queued_url.url):
                    continue
                
                if not self._should_crawl_url(queued_url.url, queued_url.depth):
                    continue
                
                # Submit to worker pool
                await self.worker_pool.submit_task(
                    url=queued_url.url,
                    depth=queued_url.depth,
                    session_id=self.crawl_session.session_id,
                    parent_url=queued_url.parent_url
                )
                submitted_tasks += 1
                pending_tasks += 1
            
            
            # Process available results (non-blocking)
            result_loop_iteration = 0
            while pending_tasks > 0:
                result_loop_iteration += 1
                try:
                    result = await asyncio.wait_for(
                        self.worker_pool.get_result(),
                        timeout=0.5  # Short timeout to avoid blocking
                    )
                    await self._handle_worker_result(result)
                    processed_results += 1
                    pending_tasks -= 1
                except asyncio.TimeoutError:
                    # No result ready yet, break and continue main loop
                    break
            
            # Small delay to prevent tight loop
            if pending_tasks >= self.config.crawler.concurrent_workers:
                await asyncio.sleep(0.1)
        
        
        # Process any remaining results
        while pending_tasks > 0:
            try:
                result = await asyncio.wait_for(
                    self.worker_pool.get_result(),
                    timeout=10.0  # Longer timeout for final cleanup
                )
                await self._handle_worker_result(result)
                processed_results += 1
                pending_tasks -= 1
            except asyncio.TimeoutError:
                break
        
        print(f"✅ Crawl loop completed: submitted {submitted_tasks} tasks, processed {processed_results} results")
    
    def _should_continue_crawling(self) -> bool:
        """Check if crawling should continue."""
        if not self.crawl_session:
            return False
        
        # Check page limit
        if self.crawl_session.pages_crawled >= self.config.crawler.max_pages:
            return False
        
        return True
    
    async def _handle_worker_result(self, result: Dict[str, Any]) -> None:
        """Handle result from worker pool."""
        if not result:
            return
        
        # Create QueuedURL object for queue status updates
        from crawler.url_management.queue import QueuedURL
        import time
        
        url = result.get('url', '')
        
        # Create QueuedURL object - url_hash will be calculated automatically as a property
        queued_url = QueuedURL(
            url=url,
            depth=result.get('depth', 0),
            priority=5,  # Default priority
            parent_url=result.get('parent_url'),
            discovered_at=time.time()
        )
        
        # Update queue status based on result
        if hasattr(self.url_queue, 'mark_url_completed') and hasattr(self.url_queue, 'mark_url_failed'):
            try:
                if result.get('success'):
                    await self.url_queue.mark_url_completed(queued_url)
                else:
                    error_message = result.get('error', 'Processing failed')
                    await self.url_queue.mark_url_failed(queued_url, error_message)
            except Exception as e:
                logger.error(f"Error marking URL as failed: {e}")
                pass
        
        # Store result in database for BOTH successful AND failed results FIRST
        page_id = None
        if self.db_manager:
            try:
                # Create CrawlResult object for database storage
                from crawler.core.engine import CrawlResult
                from crawler.monitoring.metrics import PageMetrics
                
                content_data = result.get('content', {})
                word_frequencies = result.get('word_frequencies', {})
                timing_data = result.get('timing', {})
                metadata = result.get('metadata', {})
                error_message = result.get('error', 'Processing failed') if not result.get('success') else None
                
                # Create PageMetrics object with timing data from worker
                page_metrics = PageMetrics(
                    url=result['url'],
                    depth=result.get('depth', 0)
                )
                
                # Map worker timing data to PageMetrics fields
                if timing_data:
                    # Network timing (from fetch operation)
                    page_metrics.server_response_time = timing_data.get('fetch', 0) * 1000  # Convert to ms
                    page_metrics.total_network_time = timing_data.get('fetch', 0) * 1000
                    
                    # Processing timing
                    page_metrics.html_parse_time = timing_data.get('extract', 0) * 1000
                    page_metrics.text_extraction_time = timing_data.get('extract', 0) * 1000
                    page_metrics.text_cleaning_time = timing_data.get('process', 0) * 1000
                    page_metrics.word_counting_time = timing_data.get('analyze', 0) * 1000
                    page_metrics.link_extraction_time = timing_data.get('links', 0) * 1000
                    page_metrics.total_processing_time = (
                        timing_data.get('extract', 0) +
                        timing_data.get('process', 0) +
                        timing_data.get('analyze', 0) +
                        timing_data.get('links', 0)
                    ) * 1000
                    page_metrics.total_time = timing_data.get('total', 0) * 1000
                
                # Content metrics
                page_metrics.raw_content_size = result.get('size_bytes', 0)
                page_metrics.extracted_text_size = len(content_data.get('text', '')) if content_data else 0
                page_metrics.unique_words = len(word_frequencies) if isinstance(word_frequencies, dict) else 0
                
                # Content type from metadata
                page_metrics.content_type = metadata.get('content_type', '')
                
                crawl_result = CrawlResult(
                    url=result['url'],
                    status_code=metadata.get('status_code', 0 if error_message else 200),
                    content=content_data.get('text', '') if content_data else '',
                    title=content_data.get('metadata', {}).get('title', '') if content_data else '',
                    links=result.get('links', []),
                    word_count=len(word_frequencies) if isinstance(word_frequencies, dict) else 0,
                    error=error_message,  # Include error message in CrawlResult
                    depth=result.get('depth', 0),
                    metrics=page_metrics
                )
                
                # Store page result in database
                page_id = await self.db_manager.store_page_result(crawl_result, result['session_id'])
                
                # Store word frequencies if available (only for successful results)
                if result.get('success') and isinstance(word_frequencies, dict) and word_frequencies:
                    await self.db_manager.store_word_frequencies(
                        result['session_id'],
                        page_id,
                        word_frequencies
                    )
                
            except Exception as e:
                logger.error(f"Error storing crawl result in database: {e}")
                pass
        
        # Store error events for failed results AFTER page is stored (so we have page_id)
        if not result.get('success') and self.db_manager and page_id:
            try:
                error_message = result.get('error', 'Processing failed')
                
                await self.db_manager.store_error_event(
                    session_id=result['session_id'],
                    url=url,
                    error_message=error_message,
                    depth=result.get('depth', 0),
                    operation_name='page_processing',
                    page_id=page_id  # Now we have the page_id!
                )
            except Exception as e:
                logger.error(f"Error storing error event in database: {e}")
                pass
        
        # Update session statistics
        if self.crawl_session:
            if result.get('success'):
                self.crawl_session.pages_crawled += 1
                word_frequencies = result.get('word_frequencies', {})
                if isinstance(word_frequencies, dict):
                    self.crawl_session.total_words += sum(word_frequencies.values())
                
                # Add discovered links to queue
                links = result.get('links', [])
                if links:
                    await self._add_links_to_queue(links, result.get('depth', 0) + 1)
            else:
                self.crawl_session.pages_failed += 1
    
    async def _process_remaining_results(self) -> None:
        """Process any remaining results from worker pool."""
        if not self.worker_pool:
            return
        
        # Give workers time to finish current tasks
        await asyncio.sleep(1.0)
        
        # Process remaining results
        while True:
            try:
                result = await asyncio.wait_for(
                    self.worker_pool.get_result(),
                    timeout=0.5
                )
                await self._handle_worker_result(result)
            except asyncio.TimeoutError:
                break
    
    @asynccontextmanager
    async def _track_timing(self, metrics: PageMetrics, operation: str):
        """Context manager for tracking operation timing."""
        start_time = time.perf_counter()
        try:
            yield
        finally:
            duration = (time.perf_counter() - start_time) * 1000
            setattr(metrics, f"{operation}_time", duration)
    
    async def _apply_rate_limit(self, url: str) -> None:
        """Apply rate limiting based on domain."""
        domain = urlparse(url).netloc
        
        async with self._request_lock:
            last_request = self._last_request_time.get(domain, 0)
            time_since_last = time.time() - last_request
            
            if time_since_last < self.config.crawler.rate_limit_delay:
                sleep_time = self.config.crawler.rate_limit_delay - time_since_last
                await asyncio.sleep(sleep_time)
            
            self._last_request_time[domain] = time.time()
    
    def _should_crawl_url(self, url: str, depth: int) -> bool:
        """Check if URL should be crawled."""
        # Check depth limit
        if depth > self.config.crawler.max_depth:
            return False
        
        # Check page limit (only if crawl session exists)
        if self.crawl_session and self.crawl_session.pages_crawled >= self.config.crawler.max_pages:
            return False
        
        # Check domain restrictions
        domain = urlparse(url).netloc
        
        if self.config.allowed_domains:
            if domain not in self.config.allowed_domains:
                return False
        
        if self.config.blocked_domains:
            if domain in self.config.blocked_domains:
                return False
        
        # URL is valid for crawling - let queue handle deduplication
        url_hash = hashlib.md5(url.encode()).hexdigest()
        self.visited_urls.add(url_hash)  # Keep for statistics only
        return True
    
    async def _add_links_to_queue(self, links: List[str], depth: int) -> None:
        """Add discovered links to the crawling queue."""
        # Check each link individually and normalize
        urls_to_add = []
        for link in links:
            if self._should_crawl_url(link, depth):
                try:
                    # Normalize URL before adding to queue
                    normalized_link = self.url_validator.normalize_url(link)
                    urls_to_add.append((normalized_link, depth))
                    logger.debug(f"Normalized discovered link: {link} -> {normalized_link}")
                except Exception as e:
                    logger.warning(f"Failed to normalize discovered link {link}: {e}, using original URL")
                    urls_to_add.append((link, depth))
        
        if urls_to_add:
            logger.info(f"Adding {len(urls_to_add)} normalized links to queue at depth {depth}")
            await self.url_queue.put_batch(urls_to_add, priority=5)  # Medium priority for discovered links
    
    async def get_crawl_statistics(self) -> Dict[str, Any]:
        """Get current crawling statistics."""
        if not self.crawl_session:
            return {}
        
        elapsed_time = time.time() - self.crawl_session.start_time
        
        return {
            'session_id': self.crawl_session.session_id,
            'session_name': self.crawl_session.name,
            'status': self.crawl_session.status,
            'elapsed_time': elapsed_time,
            'pages_crawled': self.crawl_session.pages_crawled,
            'pages_failed': self.crawl_session.pages_failed,
            'total_words': self.crawl_session.total_words,
            'pages_per_second': self.crawl_session.pages_crawled / elapsed_time if elapsed_time > 0 else 0,
            'queue_size': self.url_queue.size(),
            'queue_stats': self.url_queue.get_stats(),
            'visited_urls': len(self.visited_urls)
        }
    
    async def _discover_and_add_sitemap_urls(self, start_urls: List[str]) -> None:
        """Discover and add URLs from sitemaps."""
        if not self.sitemap_parser:
            logger.debug("No sitemap parser available, skipping sitemap discovery")
            return
        
        logger.info(f"Discovering sitemaps for {len(start_urls)} start URLs")
        
        for start_url in start_urls:
            try:
                logger.debug(f"Checking for sitemaps at {start_url}")
                # Get sitemaps from robots.txt
                if self.robots_checker:
                    sitemap_urls = await self.robots_checker.get_sitemaps(start_url)
                else:
                    sitemap_urls = []
                
                # Discover common sitemap locations
                discovered_sitemaps = await self.sitemap_parser.discover_sitemaps(start_url)
                sitemap_urls.extend(discovered_sitemaps)
                
                # Parse sitemaps and add URLs
                for sitemap_url in sitemap_urls:
                    logger.debug(f"Parsing sitemap: {sitemap_url}")
                    urls = await self.sitemap_parser.parse_sitemap(
                        sitemap_url,
                        max_urls=self.config.crawler.max_pages // 4  # Limit sitemap URLs
                    )
                    
                    # Normalize and add sitemap URLs to queue with lower priority
                    urls_to_add = []
                    for url in urls:
                        try:
                            # Normalize URL before adding to queue
                            normalized_url = self.url_validator.normalize_url(url)
                            urls_to_add.append((normalized_url, 1))  # Start at depth 1
                            logger.debug(f"Normalized sitemap URL: {url} -> {normalized_url}")
                        except Exception as e:
                            logger.warning(f"Failed to normalize sitemap URL {url}: {e}, using original URL")
                            urls_to_add.append((url, 1))
                    
                    if urls_to_add:
                        logger.info(f"Adding {len(urls_to_add)} normalized URLs from sitemap {sitemap_url}")
                        await self.url_queue.put_batch(urls_to_add, priority=3)
                        
            except Exception as e:
                logger.error(f"Error processing sitemaps for {start_url}: {e}")
    
    async def _check_robots_compliance(self, url: str) -> bool:
        """Check if URL can be crawled according to robots.txt."""
        if not self.robots_checker:
            return True
        
        try:
            return await self.robots_checker.can_fetch(url)
        except Exception as e:
            # On error, allow crawling (fail open)
            logger.warning(f"Error checking robots.txt for {url}: {e}, allowing crawl")
            return True
    
    async def _apply_robots_delay(self, url: str) -> None:
        """Apply crawl delay from robots.txt."""
        if not self.robots_checker:
            return
        
        try:
            wait_time = await self.robots_checker.should_wait_for_crawl_delay(url)
            if wait_time > 0:
                logger.debug(f"Applying robots.txt delay of {wait_time}s for {url}")
                await asyncio.sleep(wait_time)
        except Exception as e:
            # On error, continue without delay
            logger.warning(f"Error applying robots.txt delay for {url}: {e}")
            pass