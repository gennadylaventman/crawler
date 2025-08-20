"""
Analytics engine for web crawler data analysis.

This module provides comprehensive analytics capabilities for analyzing
crawl results, performance metrics, and generating insights.
"""

import asyncio
import uuid
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
    
    def __init__(self, database_manager):
        """Initialize AnalyticsEngine with a required database manager."""
        if database_manager is None:
            raise AnalyticsError("AnalyticsEngine requires a valid database_manager")
        
        if not hasattr(database_manager, '_initialized') or not database_manager._initialized:
            raise AnalyticsError("Database manager must be initialized before use")
            
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
            
            # Use database statistics directly to match CLI analyze results
            stats = await self.db.get_session_statistics(session_id)
            if not stats:
                raise AnalyticsError(f"Session {session_id} not found")
            
            session_info = stats.get('session_info', {})
            page_stats = stats.get('page_statistics', {})
            top_words = stats.get('top_words', [])
            
            # Get additional data for comprehensive analysis
            pages = await self._get_session_pages(session_id)
            links = await self._get_session_links(session_id)
            errors = await self._get_session_errors(session_id)
            
            # Build analytics using database statistics (to match CLI analyze)
            analytics = await self._build_analytics_from_stats(
                session_info, page_stats, top_words, pages, links, errors
            )
            
            self.logger.info(f"Completed analysis for session {session_id}")
            return analytics
            
        except Exception as e:
            self.logger.error(f"Failed to analyze session {session_id}: {e}")
            raise AnalyticsError(f"Analysis failed: {e}")
    
    async def _get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session information from database."""
        try:
            result = await self.db.get_crawl_session(session_id)
            if not result:
                raise AnalyticsError(f"Session {session_id} not found in database")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get session info for {session_id}: {e}")
            raise AnalyticsError(f"Failed to get session info: {e}")
    
    async def _get_session_pages(self, session_id: str) -> List[Dict[str, Any]]:
        """Get all pages for a session from database."""
        try:
            result = await self.db.get_session_pages(session_id)
            return result
        except Exception as e:
            self.logger.error(f"Failed to get pages for session {session_id}: {e}")
            raise AnalyticsError(f"Failed to get session pages: {e}")
    
    async def _get_session_word_frequencies(self, session_id: str) -> List[Dict[str, Any]]:
        """Get word frequencies for a session from database."""
        try:
            result = await self.db.get_word_frequency_analysis(session_id, limit=20)
            if result and 'top_words' in result:
                word_frequencies = [
                    {'word': word_data['word'], 'frequency': word_data['total_frequency']}
                    for word_data in result['top_words']
                ]
                return word_frequencies
            else:
                return []
        except Exception as e:
            self.logger.error(f"Failed to get word frequencies for session {session_id}: {e}")
            raise AnalyticsError(f"Failed to get word frequencies: {e}")
    
    async def _get_session_links(self, session_id: str) -> List[Dict[str, Any]]:
        """Get links for a session from database."""
        try:
            result = await self.db.get_session_links(session_id)
            return result
        except Exception as e:
            self.logger.error(f"Failed to get links for session {session_id}: {e}")
            raise AnalyticsError(f"Failed to get session links: {e}")
    
    async def _get_session_metrics(self, session_id: str) -> List[Dict[str, Any]]:
        """Get performance metrics for a session from database."""
        try:
            result = await self.db.get_session_metrics_simple(session_id)
            return result
        except Exception as e:
            self.logger.error(f"Failed to get metrics for session {session_id}: {e}")
            raise AnalyticsError(f"Failed to get session metrics: {e}")
    
    async def _get_session_errors(self, session_id: str) -> List[Dict[str, Any]]:
        """Get errors for a session from database."""
        try:
            result = await self.db.get_session_errors(session_id)
            return result
        except Exception as e:
            self.logger.error(f"Failed to get errors for session {session_id}: {e}")
            raise AnalyticsError(f"Failed to get session errors: {e}")
    
    async def _build_analytics_from_stats(
        self, session_info: Dict, page_stats: Dict, top_words: List[Dict],
        pages: List[Dict], links: List[Dict], errors: List[Dict]
    ) -> CrawlAnalytics:
        """
        Build analytics using database statistics (matches CLI analyze results).
        """
        try:
            # Convert session_info fields - use started_at and completed_at
            session_start = session_info.get('started_at') or session_info.get('start_time')
            session_end = session_info.get('completed_at') or session_info.get('end_time')
            
            if isinstance(session_start, str):
                session_start = datetime.fromisoformat(session_start.replace('Z', '+00:00'))
            if isinstance(session_end, str):
                session_end = datetime.fromisoformat(session_end.replace('Z', '+00:00'))
            
            # Calculate duration
            duration = 0
            if session_start and session_end:
                duration = (session_end - session_start).total_seconds()
            
            # Use database statistics directly (matches CLI analyze)
            error_rate = float(page_stats.get('error_rate', 0))
            avg_response_time = float(page_stats.get('avg_response_time', 0))
            pages_processed = int(page_stats.get('pages_processed', 0))
            error_pages = int(page_stats.get('error_pages', 0))
            
            # Build word frequency list from database top_words
            word_freq_list = []
            for word_data in top_words:
                word_freq_list.append({
                    'word': word_data.get('word', ''),
                    'frequency': int(word_data.get('frequency', 0))
                })
            
            # Build page analysis
            page_analysis = []
            for page in pages:
                response_time = page.get('response_time')
                if response_time is not None:
                    response_time = float(response_time) * 1000  # Convert to ms
                
                page_analysis.append({
                    'url': page.get('url', ''),
                    'status_code': page.get('status_code', 0),
                    'response_time': response_time,
                    'content_length': page.get('content_length', 0),
                    'word_count': page.get('word_count', 0),
                    'link_count': page.get('link_count', 0)
                })
            
            # Build link analysis
            link_analysis = []
            for link in links:
                link_analysis.append({
                    'source_url': link.get('source_url', ''),
                    'target_url': link.get('target_url', ''),
                    'link_text': link.get('link_text', ''),
                    'is_external': bool(link.get('is_external', False))
                })
            
            # Build error analysis
            error_analysis = []
            for error in errors:
                error_analysis.append({
                    'url': error.get('url', ''),
                    'error_type': error.get('error_type', ''),
                    'error_message': error.get('error_message', ''),
                    'status_code': error.get('status_code', 0)
                })
            
            # Calculate additional metrics needed for CrawlAnalytics
            from urllib.parse import urlparse
            
            # Domain analysis
            domains = set()
            for page in pages:
                try:
                    domain = urlparse(page.get('url', '')).netloc
                    if domain:
                        domains.add(domain)
                except:
                    pass
            unique_domains = len(domains)
            
            # Domain distribution for top_domains
            domain_counter = Counter()
            for page in pages:
                try:
                    domain = urlparse(page.get('url', '')).netloc
                    if domain:
                        domain_counter[domain] += 1
                except:
                    pass
            top_domains = domain_counter.most_common(10)
            
            # Convert word frequencies to tuples for top_words
            top_words_tuples = [(word_data.get('word', ''), int(word_data.get('frequency', 0)))
                               for word_data in top_words[:10]]
            
            # Calculate total words
            total_words = sum(int(p.get('word_count', 0)) for p in pages)
            
            # Content type and language distributions
            content_type_dist = Counter(p.get('content_type', 'unknown') for p in pages)
            language_dist = Counter(p.get('language', 'unknown') for p in pages if p.get('language'))
            
            # Error summary
            error_summary = Counter(e.get('error_type', 'unknown') for e in errors)
            
            # Performance metrics (use database values or calculate from pages)
            response_times = [float(p.get('response_time', 0)) for p in pages if p.get('response_time')]
            if response_times:
                median_response_time = statistics.median(response_times) * 1000  # Convert to ms
                p95_response_time = self._calculate_percentile(response_times, 95) * 1000  # Convert to ms
            else:
                median_response_time = avg_response_time
                p95_response_time = avg_response_time
            
            # Pages per second
            pages_per_second = pages_processed / duration if duration > 0 else 0
            
            # Quality metrics (defaults for now)
            average_quality_score = 0.0
            readability_distribution = {'Standard': pages_processed}
            
            return CrawlAnalytics(
                session_id=session_info.get('session_id', ''),
                session_name=session_info.get('name', 'Unknown Session'),
                total_pages=pages_processed,
                successful_pages=pages_processed - error_pages,
                failed_pages=error_pages,
                total_words=total_words,
                unique_words=len(word_freq_list),
                total_links=len(links),
                unique_domains=unique_domains,
                average_response_time=avg_response_time,  # Already in ms from database
                median_response_time=median_response_time,
                p95_response_time=p95_response_time,
                pages_per_second=pages_per_second,
                total_duration=duration,
                top_words=top_words_tuples,
                top_domains=top_domains,
                content_type_distribution=dict(content_type_dist),
                language_distribution=dict(language_dist),
                error_summary=dict(error_summary),
                error_rate=error_rate,
                average_quality_score=average_quality_score,
                readability_distribution=readability_distribution
            )
            
        except Exception as e:
            self.logger.error(f"Error building analytics from stats: {e}")
            raise AnalyticsError(f"Failed to build analytics: {e}")

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
        
        # Helper function to convert Decimal to float safely
        def to_float(value):
            if value is None:
                return 0.0
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0
        
        def to_int(value):
            if value is None:
                return 0
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
        
        # Basic statistics with type conversion
        total_pages = len(pages)
        successful_pages = sum(1 for p in pages if p.get('processing_successful', False))
        failed_pages = total_pages - successful_pages
        total_words = sum(to_int(p.get('word_count', 0)) for p in pages)
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
        
        # Performance metrics with type conversion
        response_times = [to_float(p.get('response_time')) for p in pages if p.get('response_time') is not None]
        response_times = [rt for rt in response_times if rt > 0]  # Filter out zero/invalid values
        
        if response_times:
            average_response_time = statistics.mean(response_times)
            median_response_time = statistics.median(response_times)
            p95_response_time = self._calculate_percentile(response_times, 95)
        else:
            average_response_time = median_response_time = p95_response_time = 0.0
        
        # Calculate duration and pages per second with None handling
        start_time = session_info.get('start_time') or session_info.get('started_at') or datetime.now()
        end_time = session_info.get('end_time') or session_info.get('completed_at') or datetime.now()
        
        # Handle None values
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.now()
            
        try:
            total_duration = (end_time - start_time).total_seconds()
        except (TypeError, AttributeError):
            total_duration = 3600.0  # Default to 1 hour
            
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
        # Calculate error rate like CLI analyze: pages with status_code >= 400 OR error_message
        error_pages = sum(1 for p in pages if (p.get('status_code', 0) >= 400) or p.get('error_message'))
        error_rate = (error_pages / total_pages) if total_pages > 0 else 0
        
        # Quality metrics with type conversion
        quality_scores = [to_float(p.get('quality_score')) for p in pages if p.get('quality_score') is not None]
        quality_scores = [qs for qs in quality_scores if qs > 0]  # Filter out zero/invalid values
        average_quality_score = statistics.mean(quality_scores) if quality_scores else 0.0
        
        # Readability distribution with type conversion
        readability_scores = [to_float(p.get('readability_score')) for p in pages if p.get('readability_score') is not None]
        readability_scores = [rs for rs in readability_scores if rs > 0]  # Filter out zero/invalid values
        readability_dist = self._categorize_readability(readability_scores)
        
        return CrawlAnalytics(
            session_id=session_info.get('session_id') or session_info.get('id', 'unknown'),
            session_name=session_info.get('name', 'Unknown Session'),
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