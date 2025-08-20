"""
Async worker implementation for the web crawler.

This module provides the worker class that handles individual URL processing
in the crawler's async worker pool.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from crawler.utils.exceptions import CrawlerError, NetworkError, ContentError
from crawler.content.extractor import ContentExtractor
from crawler.content.processor import ContentProcessor
from crawler.content.analyzer import WordFrequencyAnalyzer
from crawler.url_management.validator import URLValidator
from crawler.monitoring.profiler import get_performance_profiler, async_profile_operation


logger = logging.getLogger(__name__)


class CrawlerWorker:
    """
    Async worker for processing individual URLs in the crawler.
    
    Each worker handles the complete pipeline for a single URL:
    - HTTP request and response handling
    - Content extraction and processing
    - Link discovery and validation
    - Metrics collection and error handling
    """
    
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: Dict[str, Any],
        worker_id: int = 0
    ):
        """
        Initialize the crawler worker.
        
        Args:
            session: Shared aiohttp client session
            config: Crawler configuration
            worker_id: Unique identifier for this worker
        """
        self.session = session
        self.config = config
        self.worker_id = worker_id
        
        # Initialize processing components
        self.content_extractor = ContentExtractor()
        self.content_processor = ContentProcessor(config.get('content', {}))
        self.content_analyzer = WordFrequencyAnalyzer()
        self.url_validator = URLValidator()
        
        # Worker statistics
        self.pages_processed = 0
        self.errors_encountered = 0
        self.start_time = time.time()
        
        # Initialize profiler
        self.profiler = get_performance_profiler()
        
        logger.info(f"Worker {worker_id} initialized")
    
    async def process_url(
        self,
        url: str,
        depth: int,
        session_id: str,
        parent_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process a single URL through the complete crawler pipeline.
        
        Args:
            url: URL to process
            depth: Current crawl depth
            session_id: Crawl session identifier
            parent_url: URL of the parent page (if any)
            
        Returns:
            Dictionary containing processing results and extracted data
        """
        # 🎯 PROFILING: Wrap entire URL processing operation
        async with async_profile_operation(
            "url_processing",
            worker_id=self.worker_id,
            depth=depth,
            url_domain=url.split('/')[2] if '://' in url else 'unknown'
        ):
            start_time = time.time()
            result = {
                'url': url,
                'depth': depth,
                'session_id': session_id,
                'parent_url': parent_url,
                'worker_id': self.worker_id,
                'success': False,
                'error': None,
                'content': {},
                'links': [],
                'word_frequencies': {},
                'metadata': {},
                'timing': {},
                'size_bytes': 0
            }
            
            try:
                # Validate URL before processing
                if not self.url_validator.is_valid_url(url):
                    raise ContentError(f"Invalid URL: {url}")
                
                # 🎯 PROFILING: Wrap page fetch operation
                async with async_profile_operation("page_fetch", worker_id=self.worker_id):
                    fetch_start = time.time()
                    response_data = await self._fetch_page(url)  # No decorator needed!
                    fetch_time = time.time() - fetch_start
                    
                    result['timing']['fetch'] = fetch_time
                    result['size_bytes'] = len(response_data['content'])
                    result['metadata'].update(response_data['metadata'])
                
                # 🎯 PROFILING: Wrap content extraction operation
                async with async_profile_operation("content_extraction", worker_id=self.worker_id):
                    extract_start = time.time()
                    extracted_text = await self.content_extractor.extract_text(
                        response_data['content']
                    )  # No decorator needed!
                    extracted_metadata = await self.content_extractor.extract_metadata(
                        response_data['content']
                    )  # No decorator needed!
                    extracted_content = {
                        'text': extracted_text,
                        'metadata': extracted_metadata
                    }
                    extract_time = time.time() - extract_start
                    
                    result['timing']['extract'] = extract_time
                    result['content'] = extracted_content
                
                # 🎯 PROFILING: Wrap text processing operation
                async with async_profile_operation("text_processing", worker_id=self.worker_id):
                    process_start = time.time()
                    processed_text = self.content_processor._clean_text(
                        extracted_content['text']
                    )  # No decorator needed!
                    process_time = time.time() - process_start
                    
                    result['timing']['process'] = process_time
                
                # 🎯 PROFILING: Wrap word analysis operation
                async with async_profile_operation("word_analysis", worker_id=self.worker_id):
                    analyze_start = time.time()
                    word_analysis = self.content_analyzer.analyze_text(processed_text)  # No decorator needed!
                    word_frequencies = word_analysis.word_frequencies
                    analyze_time = time.time() - analyze_start
                    
                    result['timing']['analyze'] = analyze_time
                    result['word_frequencies'] = word_frequencies
                
                # 🎯 PROFILING: Wrap link extraction operation
                async with async_profile_operation("link_extraction", worker_id=self.worker_id):
                    links_start = time.time()
                    links = await self._extract_and_validate_links(
                        response_data['content'],
                        url,
                        depth
                    )  # No decorator needed!
                    links_time = time.time() - links_start
                    
                    result['timing']['links'] = links_time
                    result['links'] = links
                
                # Mark as successful
                result['success'] = True
                self.pages_processed += 1
                
                logger.debug(
                    f"Worker {self.worker_id} successfully processed {url} "
                    f"(depth: {depth}, links: {len(links)}, words: {len(word_frequencies)})"
                )
                
            except Exception as e:
                self.errors_encountered += 1
                result['error'] = str(e)
                result['success'] = False
                
                logger.error(
                    f"Worker {self.worker_id} failed to process {url}: {e}",
                    exc_info=True
                )
            
            finally:
                # Record total processing time
                total_time = time.time() - start_time
                result['timing']['total'] = total_time
                                            
            return result
    
    async def _fetch_page(self, url: str) -> Dict[str, Any]:
        """
        Fetch a web page using the HTTP session.
        
        Args:
            url: URL to fetch
            
        Returns:
            Dictionary containing response content and metadata
            
        Raises:
            NetworkError: If the request fails
            ContentError: If the content is invalid
        """
        timeout = aiohttp.ClientTimeout(
            total=self.config.get('crawler', {}).get('request_timeout', 30)
        )
        
        try:
            async with self.session.get(url, timeout=timeout) as response:
                # Check response status
                if response.status >= 400:
                    raise NetworkError(f"HTTP {response.status} for {url}")
                
                # Check content type
                content_type = response.headers.get('content-type', '').lower()
                allowed_types = self.config.get('content', {}).get(
                    'allowed_content_types', ['text/html']
                )
                
                # 🐛 DEBUG: Log content type validation for diagnosis
                logger.debug(
                    f"Worker {self.worker_id} content type check for {url}: "
                    f"received='{content_type}', allowed={allowed_types}"
                )
                
                if not any(allowed_type in content_type for allowed_type in allowed_types):
                    # 🐛 DEBUG: Enhanced error logging for content type rejection
                    logger.warning(
                        f"Worker {self.worker_id} rejecting {url} due to content type: "
                        f"'{content_type}' not in {allowed_types}"
                    )
                    raise ContentError(f"Unsupported content type: {content_type}")
                
                # Check content size
                content_length = response.headers.get('content-length')
                max_size = self.config.get('content', {}).get('max_page_size', 10485760)
                
                if content_length and int(content_length) > max_size:
                    raise ContentError(f"Content too large: {content_length} bytes")
                
                # Read content with size limit
                content = await response.read()
                if len(content) > max_size:
                    raise ContentError(f"Content too large: {len(content)} bytes")
                
                # Decode content
                try:
                    text_content = content.decode('utf-8')
                except UnicodeDecodeError:
                    # Try other common encodings
                    for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            text_content = content.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        raise ContentError("Unable to decode content")
                
                return {
                    'content': text_content,
                    'metadata': {
                        'status_code': response.status,
                        'content_type': content_type,
                        'content_length': len(content),
                        'headers': dict(response.headers),
                        'url': str(response.url),
                        'final_url': str(response.url) if response.url != url else None
                    }
                }
                
        except asyncio.TimeoutError:
            raise NetworkError(f"Timeout fetching {url}")
        except aiohttp.ClientError as e:
            raise NetworkError(f"Client error fetching {url}: {e}")
    
    async def _extract_and_validate_links(
        self,
        html_content: str,
        base_url: str,
        current_depth: int
    ) -> List[str]:
        """
        Extract and validate links from HTML content.
        
        Args:
            html_content: HTML content to parse
            base_url: Base URL for resolving relative links
            current_depth: Current crawl depth
            
        Returns:
            List of validated absolute URLs
        """
        if current_depth >= self.config.get('crawler', {}).get('max_depth', 3):
            return []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = []
            
            # Extract links from anchor tags
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href or href.startswith('#'):
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, href)
                
                # Validate the URL
                if self.url_validator.is_valid_url(absolute_url):
                    links.append(absolute_url)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_links = []
            for link in links:
                if link not in seen:
                    seen.add(link)
                    unique_links.append(link)
            
            return unique_links
            
        except Exception as e:
            logger.warning(f"Failed to extract links from {base_url}: {e}")
            return []
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """
        Get worker statistics.
        
        Returns:
            Dictionary containing worker performance statistics
        """
        runtime = time.time() - self.start_time
        
        return {
            'worker_id': self.worker_id,
            'pages_processed': self.pages_processed,
            'errors_encountered': self.errors_encountered,
            'runtime_seconds': runtime,
            'pages_per_second': self.pages_processed / runtime if runtime > 0 else 0,
            'error_rate': self.errors_encountered / max(self.pages_processed, 1)
        }
    
    async def shutdown(self):
        """
        Gracefully shutdown the worker.
        """
        logger.info(
            f"Worker {self.worker_id} shutting down. "
            f"Processed {self.pages_processed} pages with {self.errors_encountered} errors"
        )


class WorkerPool:
    """
    Manages a pool of crawler workers for concurrent URL processing.
    """
    
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: Dict[str, Any],
        pool_size: int = 10
    ):
        """
        Initialize the worker pool.
        
        Args:
            session: Shared aiohttp client session
            config: Crawler configuration
            pool_size: Number of workers in the pool
        """
        self.session = session
        self.config = config
        self.pool_size = pool_size
        
        # Create workers
        self.workers = [
            CrawlerWorker(session, config, worker_id=i)
            for i in range(pool_size)
        ]
        
        # Task management
        self.task_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()
        self.worker_tasks = []
        self.running = False
        
        logger.info(f"Worker pool initialized with {pool_size} workers")
    
    async def start(self):
        """
        Start all workers in the pool.
        """
        if self.running:
            return
        
        self.running = True
        
        # Start worker tasks
        for worker in self.workers:
            task = asyncio.create_task(self._worker_loop(worker))
            self.worker_tasks.append(task)
        
        logger.info(f"Started {len(self.workers)} workers")
    
    async def stop(self):
        """
        Stop all workers in the pool.
        """
        if not self.running:
            return
        
        self.running = False
        
        # Add sentinel values to stop workers
        for _ in self.workers:
            await self.task_queue.put(None)
        
        # Wait for all worker tasks to complete
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        
        # Shutdown workers
        for worker in self.workers:
            await worker.shutdown()
        
        logger.info("All workers stopped")
    
    async def submit_task(
        self,
        url: str,
        depth: int,
        session_id: str,
        parent_url: Optional[str] = None
    ):
        """
        Submit a URL processing task to the worker pool.
        
        Args:
            url: URL to process
            depth: Current crawl depth
            session_id: Crawl session identifier
            parent_url: URL of the parent page (if any)
        """
        task = {
            'url': url,
            'depth': depth,
            'session_id': session_id,
            'parent_url': parent_url
        }
        await self.task_queue.put(task)
    
    async def get_result(self) -> Dict[str, Any]:
        """
        Get a processing result from the worker pool.
        
        Returns:
            Dictionary containing processing results
        """
        return await self.result_queue.get()
    
    async def _worker_loop(self, worker: CrawlerWorker):
        """
        Main loop for a worker in the pool.
        
        Args:
            worker: Worker instance to run
        """
        while self.running:
            try:
                # Get next task
                task = await self.task_queue.get()
                
                # Check for sentinel value (shutdown signal)
                if task is None:
                    break
                
                # Process the task
                result = await worker.process_url(
                    url=task['url'],
                    depth=task['depth'],
                    session_id=task['session_id'],
                    parent_url=task['parent_url']
                )
                
                # Put result in result queue
                await self.result_queue.put(result)
                
                # Mark task as done
                self.task_queue.task_done()
                
            except Exception as e:
                logger.error(f"Worker {worker.worker_id} encountered error: {e}")
                # Continue processing other tasks
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get statistics for the entire worker pool.
        
        Returns:
            Dictionary containing pool performance statistics
        """
        worker_stats = [worker.get_worker_stats() for worker in self.workers]
        
        total_pages = sum(stats['pages_processed'] for stats in worker_stats)
        total_errors = sum(stats['errors_encountered'] for stats in worker_stats)
        avg_pages_per_second = sum(stats['pages_per_second'] for stats in worker_stats)
        
        return {
            'pool_size': self.pool_size,
            'total_pages_processed': total_pages,
            'total_errors': total_errors,
            'average_pages_per_second': avg_pages_per_second,
            'overall_error_rate': total_errors / max(total_pages, 1),
            'worker_stats': worker_stats,
            'task_queue_size': self.task_queue.qsize(),
            'result_queue_size': self.result_queue.qsize()
        }