"""
URL queue management with priority-based processing and duplicate detection.
"""

import asyncio
import hashlib
import time
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from collections import deque
from urllib.parse import urlparse

from crawler.utils.exceptions import QueueError


@dataclass
class QueuedURL:
    """Represents a URL in the crawling queue."""
    url: str
    depth: int
    priority: int = 0
    parent_url: Optional[str] = None
    discovered_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None
    attempts: int = 0
    last_attempt_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def url_hash(self) -> str:
        """Get URL hash for deduplication."""
        return hashlib.md5(self.url.encode()).hexdigest()
    
    @property
    def domain(self) -> str:
        """Get domain from URL."""
        try:
            return urlparse(self.url).netloc
        except:
            return ""
    
    def __lt__(self, other: 'QueuedURL') -> bool:
        """Compare for priority queue ordering."""
        # Higher priority first, then lower depth, then earlier discovery
        return (
            self.priority > other.priority or
            (self.priority == other.priority and self.depth < other.depth) or
            (self.priority == other.priority and self.depth == other.depth and 
             self.discovered_at < other.discovered_at)
        )


class BloomFilter:
    """Simple bloom filter for URL deduplication."""
    
    def __init__(self, capacity: int = 100000, error_rate: float = 0.1):
        self.capacity = capacity
        self.error_rate = error_rate
        self.bit_array_size = self._calculate_bit_array_size()
        self.hash_count = self._calculate_hash_count()
        self.bit_array = [False] * self.bit_array_size
        self.item_count = 0
    
    def _calculate_bit_array_size(self) -> int:
        """Calculate optimal bit array size."""
        import math
        return int(-self.capacity * math.log(self.error_rate) / (math.log(2) ** 2))
    
    def _calculate_hash_count(self) -> int:
        """Calculate optimal number of hash functions."""
        import math
        return int(self.bit_array_size * math.log(2) / self.capacity)
    
    def _hash(self, item: str, seed: int) -> int:
        """Hash function with seed."""
        hash_value = hash(item + str(seed))
        return abs(hash_value) % self.bit_array_size
    
    def add(self, item: str) -> None:
        """Add item to bloom filter."""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            self.bit_array[index] = True
        self.item_count += 1
    
    def contains(self, item: str) -> bool:
        """Check if item might be in the set."""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            if not self.bit_array[index]:
                return False
        return True
    
    @property
    def is_full(self) -> bool:
        """Check if bloom filter is approaching capacity."""
        return self.item_count >= self.capacity * 0.8


class URLQueue:
    """
    Priority-based URL queue with duplicate detection and domain-based rate limiting.
    """
    
    def __init__(self, max_size: int = 100000, enable_bloom_filter: bool = True):
        self.max_size = max_size
        self.enable_bloom_filter = enable_bloom_filter
        
        # Main queue storage
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_size)
        self._pending_urls: Dict[str, QueuedURL] = {}  # url_hash -> QueuedURL
        
        # Duplicate detection
        self._visited_urls: Set[str] = set()  # URL hashes
        self._bloom_filter: Optional[BloomFilter] = None
        if enable_bloom_filter:
            self._bloom_filter = BloomFilter(capacity=max_size * 2)
        
        # Domain-based rate limiting
        self._domain_last_access: Dict[str, float] = {}
        self._domain_delays: Dict[str, float] = {}
        self._discovered_domains: Set[str] = set()  # Track unique domains
        
        # Statistics
        self._stats = {
            'urls_added': 0,
            'urls_processed': 0,
            'urls_skipped_duplicate': 0,
            'urls_skipped_depth': 0,
            'domains_discovered': 0
        }
        
        # Queue management
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition()
    
    async def put(self, url: str, depth: int, priority: int = 0,
                  parent_url: Optional[str] = None, **metadata) -> bool:
        """
        Add URL to the queue.
        
        Returns:
            True if URL was added, False if skipped (duplicate, etc.)
        """
        async with self._lock:
            # Create queued URL
            queued_url = QueuedURL(
                url=url,
                depth=depth,
                priority=priority,
                parent_url=parent_url,
                metadata=metadata
            )
            
            # Check for duplicates using bloom filter only (not visited set)
            # The visited set should only track URLs that have been processed, not queued
            if self._bloom_filter and self._bloom_filter.contains(queued_url.url_hash):
                self._stats['urls_skipped_duplicate'] += 1
                return False
            
            # Check if already in pending queue
            if queued_url.url_hash in self._pending_urls:
                self._stats['urls_skipped_duplicate'] += 1
                return False
            
            # Check if queue is full
            if self._queue.full():
                # Log warning but don't crash - skip this URL
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"URL queue is full (size: {self.size()}/{self.max_size}), skipping URL: {url[:100]}...")
                self._stats['urls_skipped_duplicate'] += 1  # Count as skipped
                return False
            
            # Add to bloom filter (for future duplicate detection)
            if self._bloom_filter:
                self._bloom_filter.add(queued_url.url_hash)
            
            # Track domain
            domain = queued_url.domain
            if domain and domain not in self._discovered_domains:
                self._discovered_domains.add(domain)
                self._stats['domains_discovered'] += 1
            
            # Add to queue - let QueuedURL.__lt__ handle the ordering
            await self._queue.put(queued_url)
            self._pending_urls[queued_url.url_hash] = queued_url
            self._stats['urls_added'] += 1
            
            # Notify waiting consumers
            async with self._not_empty:
                self._not_empty.notify()
            
            return True
    
    async def get(self, timeout: Optional[float] = None) -> Optional[QueuedURL]:
        """
        Get next URL from queue with optional timeout.
        
        Returns:
            QueuedURL or None if timeout
        """
        try:
            if timeout:
                queued_url = await asyncio.wait_for(
                    self._queue.get(), timeout=timeout
                )
            else:
                queued_url = await self._queue.get()
            
            # Remove from pending and mark as visited (processed)
            async with self._lock:
                self._pending_urls.pop(queued_url.url_hash, None)
                self._visited_urls.add(queued_url.url_hash)  # Mark as visited when taken from queue
                self._stats['urls_processed'] += 1
            
            # Update domain access time
            domain = queued_url.domain
            if domain:
                self._domain_last_access[domain] = time.time()
            
            return queued_url
            
        except asyncio.TimeoutError:
            return None
    
    async def get_with_rate_limit(self, domain_delay: float = 1.0,
                                  timeout: Optional[float] = None) -> Optional[QueuedURL]:
        """
        Get next URL with domain-based rate limiting.
        
        Args:
            domain_delay: Minimum delay between requests to same domain
            timeout: Maximum time to wait
            
        Returns:
            QueuedURL or None if timeout
        """
        print(f"🔍 DEBUG: get_with_rate_limit called with domain_delay={domain_delay}, timeout={timeout}")
        print(f"🔍 DEBUG: Queue size at start: {self.size()}")
        print(f"🔍 DEBUG: Queue empty at start: {self.empty()}")
        print(f"🔍 DEBUG: Queue stats at start: {self.get_stats()}")
        
        start_time = time.time()
        iteration = 0
        max_iterations = 100  # Prevent infinite loops
        consecutive_empty_gets = 0
        max_consecutive_empty = 5  # Max consecutive empty gets before giving up
        
        while iteration < max_iterations:
            iteration += 1
            print(f"🔍 DEBUG: get_with_rate_limit loop iteration {iteration}/{max_iterations}, elapsed={time.time() - start_time:.2f}s")
            
            # Check timeout
            if timeout and (time.time() - start_time) >= timeout:
                print(f"🔍 DEBUG: get_with_rate_limit timeout reached ({timeout}s)")
                return None
            
            # Get next URL
            remaining_timeout = None
            if timeout:
                remaining_timeout = timeout - (time.time() - start_time)
                if remaining_timeout <= 0:
                    print(f"🔍 DEBUG: get_with_rate_limit no remaining timeout")
                    return None
            
            print(f"🔍 DEBUG: About to call self.get() with timeout={min(1.0, remaining_timeout) if remaining_timeout else 1.0}")
            print(f"🔍 DEBUG: Queue size before get(): {self.size()}")
            
            queued_url = await self.get(timeout=min(1.0, remaining_timeout) if remaining_timeout else 1.0)
            
            print(f"🔍 DEBUG: self.get() returned: {queued_url}")
            print(f"🔍 DEBUG: Queue size after get(): {self.size()}")
            
            if not queued_url:
                consecutive_empty_gets += 1
                print(f"🔍 DEBUG: No URL from queue (consecutive empty: {consecutive_empty_gets}/{max_consecutive_empty})")
                print(f"🔍 DEBUG: Queue empty: {self.empty()}, Queue size: {self.size()}")
                print(f"🔍 DEBUG: Pending URLs: {len(self._pending_urls)}")
                
                if self.empty():
                    print(f"🔍 DEBUG: Queue is truly empty, returning None")
                    return None
                
                if consecutive_empty_gets >= max_consecutive_empty:
                    print(f"🔍 DEBUG: Too many consecutive empty gets, assuming queue is stuck")
                    return None
                
                print(f"🔍 DEBUG: Queue not empty but get() returned None, sleeping 0.1s and continuing")
                await asyncio.sleep(0.1)
                continue
            
            # Reset consecutive empty counter
            consecutive_empty_gets = 0
            
            # Check domain rate limit
            domain = queued_url.domain
            print(f"🔍 DEBUG: Checking rate limit for domain: {domain}")
            
            if domain:
                last_access = self._domain_last_access.get(domain, 0)
                time_since_last = time.time() - last_access
                print(f"🔍 DEBUG: Domain {domain} last_access={last_access}, time_since_last={time_since_last:.2f}s, domain_delay={domain_delay}")
                
                if time_since_last < domain_delay:
                    # Need to wait - check if we have enough time left
                    wait_time = domain_delay - time_since_last
                    print(f"🔍 DEBUG: Rate limit hit, need to wait {wait_time:.2f}s")
                    
                    if timeout:
                        remaining_time = timeout - (time.time() - start_time)
                        print(f"🔍 DEBUG: Remaining timeout: {remaining_time:.2f}s, wait_time: {wait_time:.2f}s")
                        if wait_time > remaining_time:
                            # Not enough time left, put URL back and return None (timeout)
                            print(f"🔍 DEBUG: Not enough time left, putting URL back and returning None")
                            print(f"🔍 DEBUG: About to call _put_back_url for: {queued_url.url}")
                            await self._put_back_url(queued_url)
                            print(f"🔍 DEBUG: _put_back_url completed, queue size now: {self.size()}")
                            return None
                    
                    print(f"🔍 DEBUG: Sleeping for rate limit: {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    
                    # Update the domain access time after waiting
                    self._domain_last_access[domain] = time.time()
                    print(f"🔍 DEBUG: Updated domain access time for {domain}")
            
            print(f"🔍 DEBUG: Returning URL: {queued_url.url}")
            return queued_url
        
        # If we reach here, we've hit the max iterations
        print(f"🚨 DEBUG: get_with_rate_limit hit max iterations ({max_iterations}), returning None")
        return None
    
    async def _put_back_url(self, queued_url: QueuedURL) -> None:
        """
        Put a URL back into the queue (used when rate limiting prevents processing).
        """
        async with self._lock:
            # Check if queue is full
            if self._queue.full():
                # Log warning but don't crash - this shouldn't happen often
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Cannot put URL back, queue is full: {queued_url.url[:100]}...")
                return
            
            # Put URL back in queue and pending tracking
            await self._queue.put(queued_url)
            self._pending_urls[queued_url.url_hash] = queued_url
            
            # Adjust stats (we're putting it back, so decrement processed count)
            self._stats['urls_processed'] -= 1
            
            # Notify waiting consumers
            async with self._not_empty:
                self._not_empty.notify()
    
    async def put_batch(self, urls: List[Tuple[str, int]], priority: int = 0,
                        parent_url: Optional[str] = None) -> int:
        """
        Add multiple URLs to queue in batch.
        
        Returns:
            Number of URLs actually added
        """
        added_count = 0
        for url, depth in urls:
            if await self.put(url, depth, priority, parent_url):
                added_count += 1
        return added_count
    
    def _is_duplicate(self, url_hash: str) -> bool:
        """Check if URL is duplicate."""
        # Check visited set first (definitive)
        if url_hash in self._visited_urls:
            return True
        
        # Check bloom filter (probabilistic)
        if self._bloom_filter and self._bloom_filter.contains(url_hash):
            return True
        
        return False
    
    async def mark_failed(self, queued_url: QueuedURL, max_retries: int = 3) -> bool:
        """
        Mark URL as failed and optionally retry.
        
        Returns:
            True if URL was requeued for retry, False if max retries reached
        """
        queued_url.attempts += 1
        queued_url.last_attempt_at = time.time()
        
        if queued_url.attempts < max_retries:
            # Requeue with lower priority and exponential backoff
            delay = 2 ** queued_url.attempts  # Exponential backoff
            queued_url.scheduled_at = time.time() + delay
            queued_url.priority -= 1  # Lower priority
            
            await asyncio.sleep(delay)
            
            # Re-add directly to queue bypassing duplicate check
            async with self._lock:
                # Check if queue is full
                if self._queue.full():
                    raise QueueError("URL queue is full")
                
                # Add to queue directly (don't increment domain count for retries)
                await self._queue.put(queued_url)
                self._pending_urls[queued_url.url_hash] = queued_url
                
                # Notify waiting consumers
                async with self._not_empty:
                    self._not_empty.notify()
            
            return True
        
        return False
    
    def size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty()
    
    def full(self) -> bool:
        """Check if queue is full."""
        return self._queue.full()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            **self._stats,
            'current_size': self.size(),
            'max_size': self.max_size,
            'visited_urls': len(self._visited_urls),
            'pending_urls': len(self._pending_urls),
            'domains_tracked': len(self._domain_last_access),
            'bloom_filter_items': self._bloom_filter.item_count if self._bloom_filter else 0,
            'bloom_filter_full': self._bloom_filter.is_full if self._bloom_filter else False
        }
    
    async def clear(self) -> None:
        """Clear the queue."""
        async with self._lock:
            # Clear queue
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            
            # Clear tracking data
            self._pending_urls.clear()
            self._visited_urls.clear()
            self._domain_last_access.clear()
            self._discovered_domains.clear()
            
            # Reset bloom filter
            if self._bloom_filter:
                self._bloom_filter = BloomFilter(capacity=self.max_size * 2)
            
            # Reset stats
            self._stats = {
                'urls_added': 0,
                'urls_processed': 0,
                'urls_skipped_duplicate': 0,
                'urls_skipped_depth': 0,
                'domains_discovered': 0
            }
    
    async def get_pending_urls_by_domain(self, domain: str) -> List[QueuedURL]:
        """Get pending URLs for a specific domain."""
        return [
            url for url in self._pending_urls.values()
            if url.domain == domain
        ]
    
    async def remove_domain_urls(self, domain: str) -> int:
        """Remove all URLs for a specific domain."""
        removed_count = 0
        to_remove = []
        
        async with self._lock:
            # Find URLs to remove from pending
            for url_hash, queued_url in self._pending_urls.items():
                if queued_url.domain == domain:
                    to_remove.append(url_hash)
            
            # Remove from pending and visited sets
            for url_hash in to_remove:
                self._pending_urls.pop(url_hash, None)
                self._visited_urls.discard(url_hash)
                removed_count += 1
            
            # Need to rebuild the queue without the removed URLs
            if to_remove:
                # Get all remaining URLs from queue
                remaining_urls = []
                while not self._queue.empty():
                    try:
                        url = self._queue.get_nowait()
                        if url.url_hash not in to_remove:
                            remaining_urls.append(url)
                    except asyncio.QueueEmpty:
                        break
                
                # Put remaining URLs back
                for url in remaining_urls:
                    await self._queue.put(url)
        
        return removed_count
    
    def set_domain_delay(self, domain: str, delay: float) -> None:
        """Set custom delay for a specific domain."""
        self._domain_delays[domain] = delay
    
    def get_domain_delay(self, domain: str, default: float = 1.0) -> float:
        """Get delay for a specific domain."""
        return self._domain_delays.get(domain, default)