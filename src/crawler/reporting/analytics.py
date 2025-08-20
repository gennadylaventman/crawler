"""
Analytics engine for web crawler data analysis.

This module provides comprehensive analytics capabilities for analyzing
crawl results, performance metrics, and generating insights.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict, Counter
from dataclasses import dataclass
import statistics

from crawler.utils.exceptions import AnalyticsError
from crawler.monitoring.logger import get_logger


@dataclass
class CrawlAnalytics:
    """Container for crawl analytics results."""
    session_id: str
    session_name: str
    
    # Basic statistics
    total_pages: int
    successful_pages: int
    failed_pages: int
    total_words: int
    unique_words: int
    total_links: int
    unique_domains: int
    
    # Performance metrics
    average_response_time: float
    median_response_time: float
    p95_response_time: float
    pages_per_second: float
    total_duration: float
    
    # Content analysis
    top_words: List[Tuple[str, int]]
    top_domains: List[Tuple[str, int]]
    content_type_distribution: Dict[str, int]
    language_distribution: Dict[str, int]
    
    # Error analysis
    error_summary: Dict[str, int]
    error_rate: float
    
    # Quality metrics
    average_quality_score: float
    readability_distribution: Dict[str, int]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert analytics to dictionary."""
        return {
            'session_info': {
                'session_id': self.session_id,
                'session_name': self.session_name,
                'total_duration': self.total_duration
            },
            'volume_metrics': {
                'total_pages': self.total_pages,
                'successful_pages': self.successful_pages,
                'failed_pages': self.failed_pages,
                'total_words': self.total_words,
                'unique_words': self.unique_words,
                'total_links': self.total_links,
                'unique_domains': self.unique_domains
            },
            'performance_metrics': {
                'average_response_time': self.average_response_time,
                'median_response_time': self.median_response_time,
                'p95_response_time': self.p95_response_time,
                'pages_per_second': self.pages_per_second
            },
            'content_analysis': {
                'top_words': self.top_words,
                'top_domains': self.top_domains,
                'content_type_distribution': self.content_type_distribution,
                'language_distribution': self.language_distribution
            },
            'quality_metrics': {
                'average_quality_score': self.average_quality_score,
                'readability_distribution': self.readability_distribution
            },
            'error_analysis': {
                'error_summary': self.error_summary,
                'error_rate': self.error_rate
            }
        }


class AnalyticsEngine:
    """
    Comprehensive analytics engine for crawler data analysis.
    """
    
    def __init__(self, database_manager=None):
        self.db = database_manager
        self.logger = get_logger('analytics')
    
    async def analyze_crawl_session(self, session_id: str) -> CrawlAnalytics:
        """
        Perform comprehensive analysis of a crawl session.
        
        Args:
            session_id: ID of the crawl session to analyze
            
        Returns:
            CrawlAnalytics with comprehensive analysis results
        """
        try:
            self.logger.info(f"Starting analysis for session {session_id}")
            
            # Get session information
            session_info = await self._get_session_info(session_id)
            if not session_info:
                raise AnalyticsError(f"Session {session_id} not found")
            
            # Get all pages for this session
            pages = await self._get_session_pages(session_id)
            
            # Get word frequencies
            word_frequencies = await self._get_session_word_frequencies(session_id)
            
            # Get links
            links = await self._get_session_links(session_id)
            
            # Get metrics
            metrics = await self._get_session_metrics(session_id)
            
            # Get errors
            errors = await self._get_session_errors(session_id)
            
            # Perform analysis
            analytics = await self._compute_analytics(
                session_info, pages, word_frequencies, links, metrics, errors
            )
            
            self.logger.info(f"Completed analysis for session {session_id}")
            return analytics
            
        except Exception as e:
            self.logger.error(f"Failed to analyze session {session_id}: {e}")
            raise AnalyticsError(f"Analysis failed: {e}")
    
    async def _get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session information."""
        # This would query the database for session info
        # For now, return mock data
        return {
            'session_id': session_id,
            'name': 'Sample Crawl',
            'start_time': datetime.now() - timedelta(hours=1),
            'end_time': datetime.now(),
            'status': 'completed'
        }
    
    async def _get_session_pages(self, session_id: str) -> List[Dict[str, Any]]:
        """Get all pages for a session."""
        # Mock data - in real implementation, this would query the database
        return [
            {
                'url': 'https://example.com/page1',
                'status_code': 200,
                'word_count': 500,
                'processing_successful': True,
                'response_time': 1.2,
                'content_type': 'text/html',
                'language': 'en',
                'quality_score': 0.8,
                'readability_score': 65.0
            },
            {
                'url': 'https://example.com/page2',
                'status_code': 200,
                'word_count': 750,
                'processing_successful': True,
                'response_time': 0.9,
                'content_type': 'text/html',
                'language': 'en',
                'quality_score': 0.7,
                'readability_score': 58.0
            },
            {
                'url': 'https://example.com/page3',
                'status_code': 404,
                'word_count': 0,
                'processing_successful': False,
                'response_time': 0.5,
                'content_type': 'text/html',
                'language': None,
                'quality_score': 0.0,
                'readability_score': None
            }
        ]
    
    async def _get_session_word_frequencies(self, session_id: str) -> List[Dict[str, Any]]:
        """Get word frequencies for a session."""
        # Mock data
        return [
            {'word': 'web', 'frequency': 45},
            {'word': 'crawler', 'frequency': 32},
            {'word': 'data', 'frequency': 28},
            {'word': 'analysis', 'frequency': 25},
            {'word': 'content', 'frequency': 22},
            {'word': 'page', 'frequency': 20},
            {'word': 'system', 'frequency': 18},
            {'word': 'information', 'frequency': 15},
            {'word': 'search', 'frequency': 12},
            {'word': 'technology', 'frequency': 10}
        ]
    
    async def _get_session_links(self, session_id: str) -> List[Dict[str, Any]]:
        """Get links for a session."""
        # Mock data
        return [
            {'target_url': 'https://example.com/page1', 'is_internal': True},
            {'target_url': 'https://example.com/page2', 'is_internal': True},
            {'target_url': 'https://external.com/page1', 'is_internal': False},
            {'target_url': 'https://another.com/page1', 'is_internal': False}
        ]
    
    async def _get_session_metrics(self, session_id: str) -> List[Dict[str, Any]]:
        """Get performance metrics for a session."""
        # Mock data
        return [
            {'total_time': 1200, 'server_response_time': 800, 'processing_time': 400},
            {'total_time': 900, 'server_response_time': 600, 'processing_time': 300},
            {'total_time': 500, 'server_response_time': 300, 'processing_time': 200}
        ]
    
    async def _get_session_errors(self, session_id: str) -> List[Dict[str, Any]]:
        """Get errors for a session."""
        # Mock data
        return [
            {'error_type': 'network_error', 'error_message': 'Connection timeout'},
            {'error_type': 'content_error', 'error_message': 'Invalid content type'},
            {'error_type': 'network_error', 'error_message': 'DNS resolution failed'}
        ]
    
    async def _compute_analytics(
        self,
        session_info: Dict[str, Any],
        pages: List[Dict[str, Any]],
        word_frequencies: List[Dict[str, Any]],
        links: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        errors: List[Dict[str, Any]]
    ) -> CrawlAnalytics:
        """Compute comprehensive analytics from raw data."""
        
        # Basic statistics
        total_pages = len(pages)
        successful_pages = sum(1 for p in pages if p['processing_successful'])
        failed_pages = total_pages - successful_pages
        total_words = sum(p['word_count'] for p in pages)
        unique_words = len(word_frequencies)
        total_links = len(links)
        
        # Domain analysis
        domains = set()
        for page in pages:
            try:
                from urllib.parse import urlparse
                domain = urlparse(page['url']).netloc
                domains.add(domain)
            except:
                pass
        unique_domains = len(domains)
        
        # Performance metrics
        response_times = [p['response_time'] for p in pages if p['response_time']]
        if response_times:
            average_response_time = statistics.mean(response_times)
            median_response_time = statistics.median(response_times)
            p95_response_time = self._calculate_percentile(response_times, 95)
        else:
            average_response_time = median_response_time = p95_response_time = 0.0
        
        # Calculate duration and pages per second
        start_time = session_info.get('start_time', datetime.now())
        end_time = session_info.get('end_time', datetime.now())
        total_duration = (end_time - start_time).total_seconds()
        pages_per_second = successful_pages / total_duration if total_duration > 0 else 0
        
        # Content analysis
        top_words = [(wf['word'], wf['frequency']) for wf in word_frequencies[:10]]
        
        # Domain distribution
        domain_counter = Counter()
        for page in pages:
            try:
                from urllib.parse import urlparse
                domain = urlparse(page['url']).netloc
                domain_counter[domain] += 1
            except:
                pass
        top_domains = domain_counter.most_common(10)
        
        # Content type distribution
        content_type_dist = Counter(p.get('content_type', 'unknown') for p in pages)
        
        # Language distribution
        language_dist = Counter(p.get('language', 'unknown') for p in pages if p.get('language'))
        
        # Error analysis
        error_summary = Counter(e['error_type'] for e in errors)
        error_rate = failed_pages / total_pages if total_pages > 0 else 0
        
        # Quality metrics
        quality_scores = [p['quality_score'] for p in pages if p.get('quality_score') is not None]
        average_quality_score = statistics.mean(quality_scores) if quality_scores else 0.0
        
        # Readability distribution
        readability_scores = [p['readability_score'] for p in pages if p.get('readability_score') is not None]
        readability_dist = self._categorize_readability(readability_scores)
        
        return CrawlAnalytics(
            session_id=session_info['session_id'],
            session_name=session_info['name'],
            total_pages=total_pages,
            successful_pages=successful_pages,
            failed_pages=failed_pages,
            total_words=total_words,
            unique_words=unique_words,
            total_links=total_links,
            unique_domains=unique_domains,
            average_response_time=average_response_time,
            median_response_time=median_response_time,
            p95_response_time=p95_response_time,
            pages_per_second=pages_per_second,
            total_duration=total_duration,
            top_words=top_words,
            top_domains=top_domains,
            content_type_distribution=dict(content_type_dist),
            language_distribution=dict(language_dist),
            error_summary=dict(error_summary),
            error_rate=error_rate,
            average_quality_score=average_quality_score,
            readability_distribution=readability_dist
        )
    
    def _calculate_percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile value."""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = (percentile / 100) * (len(sorted_values) - 1)
        
        if index.is_integer():
            return sorted_values[int(index)]
        else:
            lower_index = int(index)
            upper_index = lower_index + 1
            weight = index - lower_index
            
            if upper_index >= len(sorted_values):
                return sorted_values[lower_index]
            
            return sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight
    
    def _categorize_readability(self, scores: List[float]) -> Dict[str, int]:
        """Categorize readability scores."""
        categories = {
            'Very Easy': 0,      # 90-100
            'Easy': 0,           # 80-89
            'Fairly Easy': 0,    # 70-79
            'Standard': 0,       # 60-69
            'Fairly Difficult': 0, # 50-59
            'Difficult': 0,      # 30-49
            'Very Difficult': 0  # 0-29
        }
        
        for score in scores:
            if score >= 90:
                categories['Very Easy'] += 1
            elif score >= 80:
                categories['Easy'] += 1
            elif score >= 70:
                categories['Fairly Easy'] += 1
            elif score >= 60:
                categories['Standard'] += 1
            elif score >= 50:
                categories['Fairly Difficult'] += 1
            elif score >= 30:
                categories['Difficult'] += 1
            else:
                categories['Very Difficult'] += 1
        
        return categories
    
    async def compare_sessions(self, session_ids: List[str]) -> Dict[str, Any]:
        """
        Compare multiple crawl sessions.
        
        Args:
            session_ids: List of session IDs to compare
            
        Returns:
            Comparison analysis results
        """
        try:
            analytics_list = []
            for session_id in session_ids:
                analytics = await self.analyze_crawl_session(session_id)
                analytics_list.append(analytics)
            
            comparison = {
                'sessions': [a.to_dict() for a in analytics_list],
                'comparison_metrics': {
                    'total_pages': [a.total_pages for a in analytics_list],
                    'success_rates': [(a.successful_pages / a.total_pages) * 100 for a in analytics_list],
                    'average_response_times': [a.average_response_time for a in analytics_list],
                    'pages_per_second': [a.pages_per_second for a in analytics_list],
                    'error_rates': [a.error_rate * 100 for a in analytics_list]
                },
                'best_performing': {
                    'fastest_session': min(analytics_list, key=lambda a: a.average_response_time).session_id,
                    'most_pages': max(analytics_list, key=lambda a: a.total_pages).session_id,
                    'highest_success_rate': max(analytics_list, key=lambda a: a.successful_pages / a.total_pages).session_id,
                    'best_quality': max(analytics_list, key=lambda a: a.average_quality_score).session_id
                }
            }
            
            return comparison
            
        except Exception as e:
            self.logger.error(f"Failed to compare sessions: {e}")
            raise AnalyticsError(f"Session comparison failed: {e}")
    
    async def get_trending_words(self, session_ids: List[str], limit: int = 20) -> List[Tuple[str, int]]:
        """
        Get trending words across multiple sessions.
        
        Args:
            session_ids: List of session IDs to analyze
            limit: Maximum number of words to return
            
        Returns:
            List of (word, total_frequency) tuples
        """
        try:
            word_counter = Counter()
            
            for session_id in session_ids:
                word_frequencies = await self._get_session_word_frequencies(session_id)
                for wf in word_frequencies:
                    word_counter[wf['word']] += wf['frequency']
            
            return word_counter.most_common(limit)
            
        except Exception as e:
            self.logger.error(f"Failed to get trending words: {e}")
            raise AnalyticsError(f"Trending words analysis failed: {e}")
    
    async def analyze_performance_trends(self, session_id: str) -> Dict[str, Any]:
        """
        Analyze performance trends within a session.
        
        Args:
            session_id: Session ID to analyze
            
        Returns:
            Performance trend analysis
        """
        try:
            metrics = await self._get_session_metrics(session_id)
            
            if not metrics:
                return {'error': 'No metrics data available'}
            
            # Calculate trends
            response_times = [m['server_response_time'] for m in metrics]
            processing_times = [m['processing_time'] for m in metrics]
            total_times = [m['total_time'] for m in metrics]
            
            trends = {
                'response_time_trend': self._calculate_trend(response_times),
                'processing_time_trend': self._calculate_trend(processing_times),
                'total_time_trend': self._calculate_trend(total_times),
                'performance_summary': {
                    'avg_response_time': statistics.mean(response_times),
                    'avg_processing_time': statistics.mean(processing_times),
                    'avg_total_time': statistics.mean(total_times),
                    'response_time_variance': statistics.variance(response_times) if len(response_times) > 1 else 0,
                    'processing_time_variance': statistics.variance(processing_times) if len(processing_times) > 1 else 0
                }
            }
            
            return trends
            
        except Exception as e:
            self.logger.error(f"Failed to analyze performance trends: {e}")
            raise AnalyticsError(f"Performance trend analysis failed: {e}")
    
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction from a list of values."""
        if len(values) < 2:
            return 'insufficient_data'
        
        # Simple linear trend calculation
        n = len(values)
        x_sum = sum(range(n))
        y_sum = sum(values)
        xy_sum = sum(i * values[i] for i in range(n))
        x_squared_sum = sum(i * i for i in range(n))
        
        slope = (n * xy_sum - x_sum * y_sum) / (n * x_squared_sum - x_sum * x_sum)
        
        if slope > 0.1:
            return 'increasing'
        elif slope < -0.1:
            return 'decreasing'
        else:
            return 'stable'