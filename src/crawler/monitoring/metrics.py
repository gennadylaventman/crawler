"""
Metrics collection and monitoring for the web crawler system.
"""

import time
import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from collections import defaultdict, deque

import psutil

from crawler.utils.exceptions import MetricsError


@dataclass
class PageMetrics:
    """Metrics for a single page crawl."""
    url: str
    depth: int
    start_time: float = field(default_factory=time.perf_counter)
    
    # Network timing metrics (milliseconds)
    dns_lookup_time: Optional[float] = None
    tcp_connect_time: Optional[float] = None
    tls_handshake_time: Optional[float] = None
    request_send_time: Optional[float] = None
    server_response_time: Optional[float] = None
    content_download_time: Optional[float] = None
    total_network_time: Optional[float] = None
    
    # Processing timing metrics (milliseconds)
    html_parse_time: Optional[float] = None
    text_extraction_time: Optional[float] = None
    text_cleaning_time: Optional[float] = None
    word_tokenization_time: Optional[float] = None
    word_counting_time: Optional[float] = None
    link_extraction_time: Optional[float] = None
    total_processing_time: Optional[float] = None
    
    # Database timing metrics (milliseconds)
    db_insert_time: Optional[float] = None
    db_query_time: Optional[float] = None
    total_db_time: Optional[float] = None
    
    # Overall timing
    queue_wait_time: Optional[float] = None
    total_time: Optional[float] = None
    
    # Content metrics
    raw_content_size: Optional[int] = None
    compressed_size: Optional[int] = None
    extracted_text_size: Optional[int] = None
    total_words: Optional[int] = None
    unique_words: Optional[int] = None
    average_word_length: Optional[float] = None
    
    # Network metrics
    status_code: Optional[int] = None
    content_type: Optional[str] = None
    remote_ip: Optional[str] = None
    connection_reused: Optional[bool] = None
    
    # Error information
    error: Optional[str] = None
    retry_count: int = 0
    
    def update_network_metrics(self, metrics: Dict[str, Any]) -> None:
        """Update network-related metrics."""
        self.server_response_time = metrics.get('response_time')
        self.content_download_time = metrics.get('download_time')
        self.status_code = metrics.get('status_code')
        self.content_type = metrics.get('content_type')
        self.raw_content_size = metrics.get('content_length')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'url': self.url,
            'depth': self.depth,
            'timing_metrics': {
                'dns_lookup_time': self.dns_lookup_time,
                'tcp_connect_time': self.tcp_connect_time,
                'server_response_time': self.server_response_time,
                'html_parse_time': self.html_parse_time,
                'text_extraction_time': self.text_extraction_time,
                'word_counting_time': self.word_counting_time,
                'total_processing_time': self.total_processing_time,
                'db_insert_time': self.db_insert_time,
                'total_time': self.total_time
            },
            'content_metrics': {
                'raw_content_size': self.raw_content_size,
                'extracted_text_size': self.extracted_text_size,
                'total_words': self.total_words,
                'unique_words': self.unique_words,
                'average_word_length': self.average_word_length
            },
            'network_metrics': {
                'status_code': self.status_code,
                'content_type': self.content_type,
                'remote_ip': self.remote_ip,
                'connection_reused': self.connection_reused
            },
            'error': self.error,
            'retry_count': self.retry_count
        }


@dataclass
class QueueMetrics:
    """Metrics for persistent queue operations."""
    session_id: str
    timestamp: float = field(default_factory=time.time)
    
    # Queue size metrics
    total_urls: int = 0
    pending_urls: int = 0
    processing_urls: int = 0
    completed_urls: int = 0
    failed_urls: int = 0
    
    # Queue operation metrics
    urls_added: int = 0
    urls_processed: int = 0
    urls_recovered: int = 0
    urls_cleaned: int = 0
    
    # Performance metrics
    avg_queue_wait_time: float = 0.0
    avg_processing_time: float = 0.0
    queue_throughput: float = 0.0  # URLs per second
    
    # Sync operation metrics
    sync_operations: int = 0
    sync_failures: int = 0
    last_sync_time: Optional[float] = None
    sync_duration: Optional[float] = None
    
    # Recovery metrics
    recovery_operations: int = 0
    recovery_failures: int = 0
    last_recovery_time: Optional[float] = None
    
    # Cleanup metrics
    cleanup_operations: int = 0
    cleanup_failures: int = 0
    last_cleanup_time: Optional[float] = None
    
    # Error tracking
    queue_errors: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    def update_queue_size(self, total: int, pending: int, processing: int,
                         completed: int, failed: int) -> None:
        """Update queue size metrics."""
        self.total_urls = total
        self.pending_urls = pending
        self.processing_urls = processing
        self.completed_urls = completed
        self.failed_urls = failed
    
    def record_sync_operation(self, success: bool, duration: float) -> None:
        """Record a sync operation."""
        self.sync_operations += 1
        if not success:
            self.sync_failures += 1
        self.last_sync_time = time.time()
        self.sync_duration = duration
    
    def record_recovery_operation(self, success: bool, recovered_count: int) -> None:
        """Record a recovery operation."""
        self.recovery_operations += 1
        if not success:
            self.recovery_failures += 1
        else:
            self.urls_recovered += recovered_count
        self.last_recovery_time = time.time()
    
    def record_cleanup_operation(self, success: bool, cleaned_count: int) -> None:
        """Record a cleanup operation."""
        self.cleanup_operations += 1
        if not success:
            self.cleanup_failures += 1
        else:
            self.urls_cleaned += cleaned_count
        self.last_cleanup_time = time.time()
    
    def record_queue_error(self, error_type: str) -> None:
        """Record a queue-related error."""
        self.queue_errors[error_type] += 1
    
    def calculate_throughput(self, time_window: float) -> float:
        """Calculate queue throughput over a time window."""
        if time_window <= 0:
            return 0.0
        return self.urls_processed / time_window
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get queue health status."""
        total_operations = self.sync_operations + self.recovery_operations + self.cleanup_operations
        total_failures = self.sync_failures + self.recovery_failures + self.cleanup_failures
        
        failure_rate = (total_failures / total_operations * 100) if total_operations > 0 else 0
        
        # Determine health status
        if failure_rate > 10:
            health = "critical"
        elif failure_rate > 5:
            health = "warning"
        elif self.processing_urls > self.pending_urls * 2:  # Too many stuck processing
            health = "warning"
        else:
            health = "healthy"
        
        return {
            'status': health,
            'failure_rate': failure_rate,
            'total_operations': total_operations,
            'total_failures': total_failures,
            'queue_size': self.total_urls,
            'processing_ratio': (self.processing_urls / self.total_urls * 100) if self.total_urls > 0 else 0,
            'completion_ratio': (self.completed_urls / self.total_urls * 100) if self.total_urls > 0 else 0
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert queue metrics to dictionary."""
        return {
            'session_id': self.session_id,
            'timestamp': self.timestamp,
            'queue_size': {
                'total_urls': self.total_urls,
                'pending_urls': self.pending_urls,
                'processing_urls': self.processing_urls,
                'completed_urls': self.completed_urls,
                'failed_urls': self.failed_urls
            },
            'operations': {
                'urls_added': self.urls_added,
                'urls_processed': self.urls_processed,
                'urls_recovered': self.urls_recovered,
                'urls_cleaned': self.urls_cleaned
            },
            'performance': {
                'avg_queue_wait_time': self.avg_queue_wait_time,
                'avg_processing_time': self.avg_processing_time,
                'queue_throughput': self.queue_throughput
            },
            'sync_metrics': {
                'sync_operations': self.sync_operations,
                'sync_failures': self.sync_failures,
                'last_sync_time': self.last_sync_time,
                'sync_duration': self.sync_duration
            },
            'recovery_metrics': {
                'recovery_operations': self.recovery_operations,
                'recovery_failures': self.recovery_failures,
                'last_recovery_time': self.last_recovery_time
            },
            'cleanup_metrics': {
                'cleanup_operations': self.cleanup_operations,
                'cleanup_failures': self.cleanup_failures,
                'last_cleanup_time': self.last_cleanup_time
            },
            'errors': dict(self.queue_errors),
            'health': self.get_health_status()
        }


@dataclass
class SessionMetrics:
    """Aggregated metrics for a crawl session."""
    session_id: str
    start_time: float = field(default_factory=time.time)
    
    # Volume metrics
    pages_processed: int = 0
    pages_failed: int = 0
    total_words: int = 0
    total_bytes: int = 0
    
    # Performance metrics
    total_processing_time: float = 0.0
    total_network_time: float = 0.0
    total_db_time: float = 0.0
    
    # Error tracking
    error_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # Recent performance (sliding window)
    recent_response_times: deque = field(default_factory=lambda: deque(maxlen=100))
    recent_processing_times: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def update_from_page_metrics(self, page_metrics: PageMetrics) -> None:
        """Update session metrics from page metrics."""
        self.pages_processed += 1
        
        if page_metrics.error:
            self.pages_failed += 1
            error_type = type(page_metrics.error).__name__ if isinstance(page_metrics.error, Exception) else "unknown"
            self.error_counts[error_type] += 1
        
        if page_metrics.total_words:
            self.total_words += page_metrics.total_words
        
        if page_metrics.raw_content_size:
            self.total_bytes += page_metrics.raw_content_size
        
        if page_metrics.total_processing_time:
            self.total_processing_time += page_metrics.total_processing_time
            self.recent_processing_times.append(page_metrics.total_processing_time)
        
        if page_metrics.server_response_time:
            self.total_network_time += page_metrics.server_response_time
            self.recent_response_times.append(page_metrics.server_response_time)
        
        if page_metrics.db_insert_time:
            self.total_db_time += page_metrics.db_insert_time
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        elapsed_time = time.time() - self.start_time
        
        return {
            'session_id': self.session_id,
            'elapsed_time': elapsed_time,
            'pages_processed': self.pages_processed,
            'pages_failed': self.pages_failed,
            'total_words': self.total_words,
            'total_bytes': self.total_bytes,
            'pages_per_second': self.pages_processed / elapsed_time if elapsed_time > 0 else 0,
            'words_per_second': self.total_words / elapsed_time if elapsed_time > 0 else 0,
            'avg_processing_time': self.total_processing_time / self.pages_processed if self.pages_processed > 0 else 0,
            'avg_response_time': self.total_network_time / self.pages_processed if self.pages_processed > 0 else 0,
            'error_rate': self.pages_failed / self.pages_processed * 100 if self.pages_processed > 0 else 0,
            'recent_avg_response_time': sum(self.recent_response_times) / len(self.recent_response_times) if self.recent_response_times else 0,
            'recent_avg_processing_time': sum(self.recent_processing_times) / len(self.recent_processing_times) if self.recent_processing_times else 0,
            'error_counts': dict(self.error_counts)
        }


@dataclass
class SystemMetrics:
    """System resource metrics."""
    timestamp: float = field(default_factory=time.time)
    
    # CPU metrics
    cpu_usage_percent: float = 0.0
    cpu_usage_per_core: List[float] = field(default_factory=list)
    cpu_load_1min: float = 0.0
    cpu_load_5min: float = 0.0
    cpu_load_15min: float = 0.0
    
    # Memory metrics
    total_memory_mb: int = 0
    used_memory_mb: int = 0
    available_memory_mb: int = 0
    memory_usage_percent: float = 0.0
    
    # Process-specific memory
    process_memory_rss_mb: int = 0
    process_memory_vms_mb: int = 0
    
    # Network metrics
    network_bytes_sent: int = 0
    network_bytes_received: int = 0
    active_connections: int = 0
    
    # Disk metrics
    disk_usage_percent: float = 0.0
    disk_read_mb: int = 0
    disk_write_mb: int = 0
    
    @classmethod
    def collect_current(cls) -> 'SystemMetrics':
        """Collect current system metrics."""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_per_core = psutil.cpu_percent(interval=1, percpu=True)
            load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            process = psutil.Process()
            process_memory = process.memory_info()
            
            # Network metrics
            network = psutil.net_io_counters()
            connections = len(psutil.net_connections())
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            
            return cls(
                cpu_usage_percent=cpu_percent,
                cpu_usage_per_core=cpu_per_core,
                cpu_load_1min=load_avg[0],
                cpu_load_5min=load_avg[1],
                cpu_load_15min=load_avg[2],
                total_memory_mb=memory.total // 1024 // 1024,
                used_memory_mb=memory.used // 1024 // 1024,
                available_memory_mb=memory.available // 1024 // 1024,
                memory_usage_percent=memory.percent,
                process_memory_rss_mb=process_memory.rss // 1024 // 1024,
                process_memory_vms_mb=process_memory.vms // 1024 // 1024,
                network_bytes_sent=network.bytes_sent,
                network_bytes_received=network.bytes_recv,
                active_connections=connections,
                disk_usage_percent=disk.percent,
                disk_read_mb=disk_io.read_bytes // 1024 // 1024 if disk_io else 0,
                disk_write_mb=disk_io.write_bytes // 1024 // 1024 if disk_io else 0
            )
        except Exception as e:
            raise MetricsError(f"Failed to collect system metrics: {e}")


class MetricsCollector:
    """
    Centralized metrics collection and aggregation.
    """
    
    def __init__(self, collection_interval: int = 60):
        self.collection_interval = collection_interval
        self.session_metrics: Dict[str, SessionMetrics] = {}
        self.system_metrics_history: deque = deque(maxlen=1000)
        self._collection_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start_collection(self) -> None:
        """Start continuous metrics collection."""
        if self._running:
            return
        
        self._running = True
        self._collection_task = asyncio.create_task(self._collection_loop())
    
    async def stop_collection(self) -> None:
        """Stop metrics collection."""
        self._running = False
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
    
    async def _collection_loop(self) -> None:
        """Main collection loop."""
        while self._running:
            try:
                # Collect system metrics
                system_metrics = SystemMetrics.collect_current()
                self.system_metrics_history.append(system_metrics)
                
                # Sleep until next collection
                await asyncio.sleep(self.collection_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in metrics collection: {e}")
                await asyncio.sleep(self.collection_interval)
    
    async def record_page_metrics(self, page_metrics: PageMetrics) -> None:
        """Record metrics for a processed page."""
        try:
            # This would typically store to database
            # For now, we'll just track in memory
            pass
        except Exception as e:
            raise MetricsError(f"Failed to record page metrics: {e}")
    
    async def record_error(self, url: str, error_msg: str, depth: int) -> None:
        """Record an error event."""
        try:
            # This would typically store to database
            # For now, we'll just track in memory
            pass
        except Exception as e:
            raise MetricsError(f"Failed to record error: {e}")
    
    def get_session_metrics(self, session_id: str) -> Optional[SessionMetrics]:
        """Get metrics for a specific session."""
        return self.session_metrics.get(session_id)
    
    def create_session_metrics(self, session_id: str) -> SessionMetrics:
        """Create new session metrics tracker."""
        session_metrics = SessionMetrics(session_id=session_id)
        self.session_metrics[session_id] = session_metrics
        return session_metrics
    
    def get_current_system_metrics(self) -> Optional[SystemMetrics]:
        """Get the most recent system metrics."""
        return self.system_metrics_history[-1] if self.system_metrics_history else None
    
    def get_system_metrics_history(self, limit: int = 100) -> List[SystemMetrics]:
        """Get recent system metrics history."""
        return list(self.system_metrics_history)[-limit:]
    
    def calculate_percentiles(self, values: List[float], percentiles: List[float]) -> Dict[float, float]:
        """Calculate percentiles for a list of values."""
        if not values:
            return {p: 0.0 for p in percentiles}
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        result = {}
        
        for p in percentiles:
            if p == 0:
                result[p] = sorted_values[0]
            elif p == 100:
                result[p] = sorted_values[-1]
            else:
                index = (p / 100) * (n - 1)
                lower_index = int(index)
                upper_index = min(lower_index + 1, n - 1)
                
                if lower_index == upper_index:
                    result[p] = sorted_values[lower_index]
                else:
                    # Linear interpolation
                    weight = index - lower_index
                    result[p] = (sorted_values[lower_index] * (1 - weight) + 
                               sorted_values[upper_index] * weight)
        
        return result
    
    def get_performance_summary(self, session_id: str) -> Dict[str, Any]:
        """Get comprehensive performance summary for a session."""
        session_metrics = self.session_metrics.get(session_id)
        if not session_metrics:
            return {}
        
        stats = session_metrics.get_performance_stats()
        
        # Add percentile calculations
        if session_metrics.recent_response_times:
            response_percentiles = self.calculate_percentiles(
                list(session_metrics.recent_response_times),
                [50, 95, 99]
            )
            stats['response_time_percentiles'] = response_percentiles
        
        if session_metrics.recent_processing_times:
            processing_percentiles = self.calculate_percentiles(
                list(session_metrics.recent_processing_times),
                [50, 95, 99]
            )
            stats['processing_time_percentiles'] = processing_percentiles
        
        # Add system metrics if available
        current_system = self.get_current_system_metrics()
        if current_system:
            stats['system_metrics'] = {
                'cpu_usage': current_system.cpu_usage_percent,
                'memory_usage': current_system.memory_usage_percent,
                'process_memory_mb': current_system.process_memory_rss_mb,
                'active_connections': current_system.active_connections
            }
        
        return stats