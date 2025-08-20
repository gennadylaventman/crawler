"""
Data visualization and chart generation for crawler analytics.

This module provides comprehensive visualization capabilities for crawler data,
including charts, graphs, and interactive visualizations.
"""

import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
import base64
from io import BytesIO

from crawler.reporting.analytics import CrawlAnalytics
from crawler.utils.exceptions import AnalyticsError
from crawler.monitoring.logger import get_logger


class ChartGenerator:
    """
    Chart generation utilities for crawler data visualization.
    """
    
    def __init__(self):
        self.logger = get_logger('chart_generator')
    
    def generate_word_cloud_data(self, word_frequencies: List[Tuple[str, int]], max_words: int = 100) -> Dict[str, Any]:
        """
        Generate word cloud data structure.
        
        Args:
            word_frequencies: List of (word, frequency) tuples
            max_words: Maximum number of words to include
            
        Returns:
            Word cloud data structure
        """
        try:
            # Limit to max_words and normalize frequencies
            limited_words = word_frequencies[:max_words]
            
            if not limited_words:
                return {'words': [], 'max_frequency': 0}
            
            max_freq = max(freq for _, freq in limited_words)
            
            word_cloud_data = {
                'words': [
                    {
                        'text': word,
                        'frequency': freq,
                        'size': int((freq / max_freq) * 100) + 10  # Scale to 10-110
                    }
                    for word, freq in limited_words
                ],
                'max_frequency': max_freq,
                'total_words': len(limited_words)
            }
            
            return word_cloud_data
            
        except Exception as e:
            self.logger.error(f"Failed to generate word cloud data: {e}")
            return {'words': [], 'max_frequency': 0}
    
    def generate_performance_chart_data(self, analytics: CrawlAnalytics) -> Dict[str, Any]:
        """
        Generate performance chart data.
        
        Args:
            analytics: Crawl analytics data
            
        Returns:
            Performance chart data structure
        """
        try:
            chart_data = {
                'response_times': {
                    'average': analytics.average_response_time,
                    'median': analytics.median_response_time,
                    'p95': analytics.p95_response_time,
                    'labels': ['Average', 'Median', '95th Percentile'],
                    'values': [
                        analytics.average_response_time,
                        analytics.median_response_time,
                        analytics.p95_response_time
                    ]
                },
                'success_metrics': {
                    'successful': analytics.successful_pages,
                    'failed': analytics.failed_pages,
                    'total': analytics.total_pages,
                    'success_rate': (analytics.successful_pages / analytics.total_pages * 100) if analytics.total_pages > 0 else 0,
                    'labels': ['Successful', 'Failed'],
                    'values': [analytics.successful_pages, analytics.failed_pages],
                    'colors': ['#28a745', '#dc3545']
                },
                'throughput': {
                    'pages_per_second': analytics.pages_per_second,
                    'total_duration': analytics.total_duration,
                    'total_pages': analytics.total_pages
                }
            }
            
            return chart_data
            
        except Exception as e:
            self.logger.error(f"Failed to generate performance chart data: {e}")
            return {}
    
    def generate_content_distribution_data(self, analytics: CrawlAnalytics) -> Dict[str, Any]:
        """
        Generate content distribution chart data.
        
        Args:
            analytics: Crawl analytics data
            
        Returns:
            Content distribution chart data
        """
        try:
            chart_data = {
                'content_types': {
                    'labels': list(analytics.content_type_distribution.keys()),
                    'values': list(analytics.content_type_distribution.values()),
                    'colors': self._generate_colors(len(analytics.content_type_distribution))
                },
                'languages': {
                    'labels': list(analytics.language_distribution.keys()),
                    'values': list(analytics.language_distribution.values()),
                    'colors': self._generate_colors(len(analytics.language_distribution))
                },
                'readability': {
                    'labels': list(analytics.readability_distribution.keys()),
                    'values': list(analytics.readability_distribution.values()),
                    'colors': ['#28a745', '#6f42c1', '#17a2b8', '#ffc107', '#fd7e14', '#dc3545', '#6c757d']
                }
            }
            
            return chart_data
            
        except Exception as e:
            self.logger.error(f"Failed to generate content distribution data: {e}")
            return {}
    
    def generate_domain_analysis_data(self, analytics: CrawlAnalytics) -> Dict[str, Any]:
        """
        Generate domain analysis chart data.
        
        Args:
            analytics: Crawl analytics data
            
        Returns:
            Domain analysis chart data
        """
        try:
            chart_data = {
                'top_domains': {
                    'labels': [domain for domain, _ in analytics.top_domains],
                    'values': [count for _, count in analytics.top_domains],
                    'colors': self._generate_colors(len(analytics.top_domains))
                },
                'domain_summary': {
                    'unique_domains': analytics.unique_domains,
                    'total_pages': analytics.total_pages,
                    'pages_per_domain': analytics.total_pages / analytics.unique_domains if analytics.unique_domains > 0 else 0
                }
            }
            
            return chart_data
            
        except Exception as e:
            self.logger.error(f"Failed to generate domain analysis data: {e}")
            return {}
    
    def generate_error_analysis_data(self, analytics: CrawlAnalytics) -> Dict[str, Any]:
        """
        Generate error analysis chart data.
        
        Args:
            analytics: Crawl analytics data
            
        Returns:
            Error analysis chart data
        """
        try:
            chart_data = {
                'error_types': {
                    'labels': list(analytics.error_summary.keys()),
                    'values': list(analytics.error_summary.values()),
                    'colors': ['#dc3545', '#fd7e14', '#ffc107', '#6c757d']
                },
                'error_metrics': {
                    'error_rate': analytics.error_rate * 100,
                    'total_errors': sum(analytics.error_summary.values()),
                    'successful_pages': analytics.successful_pages,
                    'failed_pages': analytics.failed_pages
                }
            }
            
            return chart_data
            
        except Exception as e:
            self.logger.error(f"Failed to generate error analysis data: {e}")
            return {}
    
    def _generate_colors(self, count: int) -> List[str]:
        """Generate a list of colors for charts."""
        base_colors = [
            '#007bff', '#28a745', '#ffc107', '#dc3545', '#6f42c1',
            '#17a2b8', '#fd7e14', '#e83e8c', '#6c757d', '#343a40'
        ]
        
        if count <= len(base_colors):
            return base_colors[:count]
        
        # Generate additional colors if needed
        colors = base_colors.copy()
        for i in range(count - len(base_colors)):
            # Generate colors by varying hue
            hue = (i * 137.5) % 360  # Golden angle approximation
            colors.append(f'hsl({hue}, 70%, 50%)')
        
        return colors[:count]


class DataVisualizer:
    """
    Main data visualization class for crawler analytics.
    """
    
    def __init__(self):
        self.chart_generator = ChartGenerator()
        self.logger = get_logger('data_visualizer')
    
    def create_dashboard_data(self, analytics: CrawlAnalytics) -> Dict[str, Any]:
        """
        Create comprehensive dashboard data for visualization.
        
        Args:
            analytics: Crawl analytics data
            
        Returns:
            Complete dashboard data structure
        """
        try:
            self.logger.info(f"Creating dashboard data for session {analytics.session_id}")
            
            dashboard_data = {
                'session_info': {
                    'session_id': analytics.session_id,
                    'session_name': analytics.session_name,
                    'generated_at': datetime.now().isoformat(),
                    'total_duration': analytics.total_duration
                },
                'summary_metrics': {
                    'total_pages': analytics.total_pages,
                    'successful_pages': analytics.successful_pages,
                    'failed_pages': analytics.failed_pages,
                    'success_rate': (analytics.successful_pages / analytics.total_pages * 100) if analytics.total_pages > 0 else 0,
                    'total_words': analytics.total_words,
                    'unique_words': analytics.unique_words,
                    'unique_domains': analytics.unique_domains,
                    'pages_per_second': analytics.pages_per_second,
                    'average_response_time': analytics.average_response_time,
                    'error_rate': analytics.error_rate * 100
                },
                'charts': {
                    'word_cloud': self.chart_generator.generate_word_cloud_data(analytics.top_words),
                    'performance': self.chart_generator.generate_performance_chart_data(analytics),
                    'content_distribution': self.chart_generator.generate_content_distribution_data(analytics),
                    'domain_analysis': self.chart_generator.generate_domain_analysis_data(analytics),
                    'error_analysis': self.chart_generator.generate_error_analysis_data(analytics)
                },
                'tables': {
                    'top_words': [
                        {'word': word, 'frequency': freq, 'percentage': (freq / analytics.total_words * 100) if analytics.total_words > 0 else 0}
                        for word, freq in analytics.top_words[:20]
                    ],
                    'top_domains': [
                        {'domain': domain, 'pages': count, 'percentage': (count / analytics.total_pages * 100) if analytics.total_pages > 0 else 0}
                        for domain, count in analytics.top_domains[:10]
                    ],
                    'error_breakdown': [
                        {'error_type': error_type, 'count': count, 'percentage': (count / sum(analytics.error_summary.values()) * 100) if analytics.error_summary else 0}
                        for error_type, count in analytics.error_summary.items()
                    ]
                }
            }
            
            self.logger.info(f"Dashboard data created successfully for session {analytics.session_id}")
            return dashboard_data
            
        except Exception as e:
            self.logger.error(f"Failed to create dashboard data: {e}")
            raise AnalyticsError(f"Dashboard creation failed: {e}")
    
    def create_comparison_dashboard(self, analytics_list: List[CrawlAnalytics]) -> Dict[str, Any]:
        """
        Create comparison dashboard for multiple crawl sessions.
        
        Args:
            analytics_list: List of crawl analytics to compare
            
        Returns:
            Comparison dashboard data structure
        """
        try:
            self.logger.info(f"Creating comparison dashboard for {len(analytics_list)} sessions")
            
            if not analytics_list:
                raise AnalyticsError("No analytics data provided for comparison")
            
            # Prepare comparison data
            session_names = [a.session_name for a in analytics_list]
            
            comparison_data = {
                'session_info': {
                    'session_count': len(analytics_list),
                    'session_names': session_names,
                    'generated_at': datetime.now().isoformat()
                },
                'comparison_charts': {
                    'pages_comparison': {
                        'labels': session_names,
                        'datasets': [
                            {
                                'label': 'Total Pages',
                                'data': [a.total_pages for a in analytics_list],
                                'backgroundColor': '#007bff'
                            },
                            {
                                'label': 'Successful Pages',
                                'data': [a.successful_pages for a in analytics_list],
                                'backgroundColor': '#28a745'
                            },
                            {
                                'label': 'Failed Pages',
                                'data': [a.failed_pages for a in analytics_list],
                                'backgroundColor': '#dc3545'
                            }
                        ]
                    },
                    'performance_comparison': {
                        'labels': session_names,
                        'datasets': [
                            {
                                'label': 'Average Response Time (ms)',
                                'data': [a.average_response_time for a in analytics_list],
                                'backgroundColor': '#17a2b8'
                            },
                            {
                                'label': 'Pages per Second',
                                'data': [a.pages_per_second for a in analytics_list],
                                'backgroundColor': '#ffc107'
                            }
                        ]
                    },
                    'quality_comparison': {
                        'labels': session_names,
                        'datasets': [
                            {
                                'label': 'Success Rate (%)',
                                'data': [(a.successful_pages / a.total_pages * 100) if a.total_pages > 0 else 0 for a in analytics_list],
                                'backgroundColor': '#28a745'
                            },
                            {
                                'label': 'Error Rate (%)',
                                'data': [a.error_rate * 100 for a in analytics_list],
                                'backgroundColor': '#dc3545'
                            },
                            {
                                'label': 'Quality Score',
                                'data': [a.average_quality_score * 100 for a in analytics_list],
                                'backgroundColor': '#6f42c1'
                            }
                        ]
                    }
                },
                'comparison_table': [
                    {
                        'session_name': a.session_name,
                        'total_pages': a.total_pages,
                        'success_rate': f"{(a.successful_pages / a.total_pages * 100):.1f}%" if a.total_pages > 0 else "0%",
                        'avg_response_time': f"{a.average_response_time:.2f}ms",
                        'pages_per_second': f"{a.pages_per_second:.2f}",
                        'total_words': a.total_words,
                        'unique_domains': a.unique_domains,
                        'error_rate': f"{a.error_rate * 100:.1f}%"
                    }
                    for a in analytics_list
                ],
                'best_performers': {
                    'fastest_session': min(analytics_list, key=lambda a: a.average_response_time).session_name,
                    'most_pages': max(analytics_list, key=lambda a: a.total_pages).session_name,
                    'highest_success_rate': max(analytics_list, key=lambda a: a.successful_pages / a.total_pages if a.total_pages > 0 else 0).session_name,
                    'best_quality': max(analytics_list, key=lambda a: a.average_quality_score).session_name
                }
            }
            
            self.logger.info(f"Comparison dashboard created successfully")
            return comparison_data
            
        except Exception as e:
            self.logger.error(f"Failed to create comparison dashboard: {e}")
            raise AnalyticsError(f"Comparison dashboard creation failed: {e}")
    
    def export_chart_data(self, chart_data: Dict[str, Any], format: str = 'json') -> str:
        """
        Export chart data in specified format.
        
        Args:
            chart_data: Chart data to export
            format: Export format ('json', 'csv')
            
        Returns:
            Exported data as string
        """
        try:
            if format.lower() == 'json':
                return json.dumps(chart_data, indent=2, default=str)
            elif format.lower() == 'csv':
                return self._convert_to_csv(chart_data)
            else:
                raise AnalyticsError(f"Unsupported export format: {format}")
                
        except Exception as e:
            self.logger.error(f"Failed to export chart data: {e}")
            raise AnalyticsError(f"Chart data export failed: {e}")
    
    def _convert_to_csv(self, data: Dict[str, Any]) -> str:
        """Convert chart data to CSV format."""
        try:
            import csv
            from io import StringIO
            
            output = StringIO()
            
            # Handle different data structures
            if 'summary_metrics' in data:
                # Dashboard data
                writer = csv.writer(output)
                writer.writerow(['Metric', 'Value'])
                
                for key, value in data['summary_metrics'].items():
                    writer.writerow([key.replace('_', ' ').title(), value])
                
                # Add top words if available
                if 'tables' in data and 'top_words' in data['tables']:
                    writer.writerow([])  # Empty row
                    writer.writerow(['Top Words'])
                    writer.writerow(['Word', 'Frequency', 'Percentage'])
                    
                    for word_data in data['tables']['top_words']:
                        writer.writerow([word_data['word'], word_data['frequency'], f"{word_data['percentage']:.2f}%"])
            
            return output.getvalue()
            
        except Exception as e:
            self.logger.error(f"Failed to convert to CSV: {e}")
            return ""
    
    def create_html_visualization(self, dashboard_data: Dict[str, Any]) -> str:
        """
        Create HTML visualization from dashboard data.
        
        Args:
            dashboard_data: Dashboard data structure
            
        Returns:
            HTML string with embedded visualizations
        """
        try:
            html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crawler Analytics Dashboard - {session_name}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 30px; padding: 20px; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .metric-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }}
        .metric-value {{ font-size: 2em; font-weight: bold; color: #007bff; }}
        .metric-label {{ color: #6c757d; margin-top: 5px; }}
        .chart-container {{ background: white; padding: 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .chart-title {{ font-size: 1.2em; font-weight: bold; margin-bottom: 15px; color: #343a40; }}
        .chart-wrapper {{ position: relative; height: 400px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }}
        th {{ background-color: #f8f9fa; font-weight: bold; }}
        .success {{ color: #28a745; }}
        .error {{ color: #dc3545; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Crawler Analytics Dashboard</h1>
            <h2>{session_name}</h2>
            <p>Generated on {generated_at}</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{total_pages}</div>
                <div class="metric-label">Total Pages</div>
            </div>
            <div class="metric-card">
                <div class="metric-value success">{success_rate:.1f}%</div>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{pages_per_second:.2f}</div>
                <div class="metric-label">Pages/Second</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{average_response_time:.0f}ms</div>
                <div class="metric-label">Avg Response Time</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{total_words:,}</div>
                <div class="metric-label">Total Words</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{unique_domains}</div>
                <div class="metric-label">Unique Domains</div>
            </div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Performance Metrics</div>
            <div class="chart-wrapper">
                <canvas id="performanceChart"></canvas>
            </div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Top Words</div>
            <table>
                <thead>
                    <tr><th>Word</th><th>Frequency</th><th>Percentage</th></tr>
                </thead>
                <tbody>
                    {top_words_rows}
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        // Performance Chart
        const performanceCtx = document.getElementById('performanceChart').getContext('2d');
        new Chart(performanceCtx, {{
            type: 'bar',
            data: {{
                labels: {performance_labels},
                datasets: [{{
                    label: 'Response Time (ms)',
                    data: {performance_values},
                    backgroundColor: ['#007bff', '#28a745', '#ffc107']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
            """
            
            # Prepare data for template
            session_info = dashboard_data.get('session_info', {})
            summary = dashboard_data.get('summary_metrics', {})
            charts = dashboard_data.get('charts', {})
            tables = dashboard_data.get('tables', {})
            
            # Generate top words table rows
            top_words_rows = ""
            for word_data in tables.get('top_words', [])[:10]:
                top_words_rows += f"<tr><td>{word_data['word']}</td><td>{word_data['frequency']}</td><td>{word_data['percentage']:.1f}%</td></tr>"
            
            # Performance chart data
            performance_data = charts.get('performance', {}).get('response_times', {})
            performance_labels = json.dumps(performance_data.get('labels', []))
            performance_values = json.dumps(performance_data.get('values', []))
            
            # Format the HTML
            html_content = html_template.format(
                session_name=session_info.get('session_name', 'Unknown'),
                generated_at=session_info.get('generated_at', datetime.now().isoformat()),
                total_pages=summary.get('total_pages', 0),
                success_rate=summary.get('success_rate', 0),
                pages_per_second=summary.get('pages_per_second', 0),
                average_response_time=summary.get('average_response_time', 0),
                total_words=summary.get('total_words', 0),
                unique_domains=summary.get('unique_domains', 0),
                top_words_rows=top_words_rows,
                performance_labels=performance_labels,
                performance_values=performance_values
            )
            
            return html_content
            
        except Exception as e:
            self.logger.error(f"Failed to create HTML visualization: {e}")
            raise AnalyticsError(f"HTML visualization creation failed: {e}")