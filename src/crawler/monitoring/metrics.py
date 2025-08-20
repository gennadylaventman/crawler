"""
Metrics collection and monitoring for the web crawler system.
"""

import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

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


