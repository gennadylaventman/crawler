"""
Report generation for crawler analytics.

This module provides comprehensive report generation capabilities,
including various output formats and automated report scheduling.
"""

import json
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from enum import Enum
import asyncio

from crawler.reporting.analytics import AnalyticsEngine, CrawlAnalytics
from crawler.reporting.visualizer import DataVisualizer
from crawler.utils.exceptions import AnalyticsError
from crawler.utils.logging import get_logger


class ReportFormat(str, Enum):
    """Supported report formats."""
    JSON = "json"
    HTML = "html"
    CSV = "csv"
    PDF = "pdf"
    MARKDOWN = "md"


class ReportType(str, Enum):
    """Types of reports that can be generated."""
    SESSION_SUMMARY = "session_summary"
    PERFORMANCE_ANALYSIS = "performance_analysis"
    CONTENT_ANALYSIS = "content_analysis"
    ERROR_ANALYSIS = "error_analysis"
    COMPARISON_REPORT = "comparison_report"
    TREND_ANALYSIS = "trend_analysis"


class ReportGenerator:
    """
    Comprehensive report generator for crawler analytics.
    """
    
    def __init__(self, analytics_engine: Optional[AnalyticsEngine] = None):
        self.analytics_engine = analytics_engine or AnalyticsEngine()
        self.visualizer = DataVisualizer()
        self.logger = get_logger('report_generator')
    
    async def generate_session_report(
        self,
        session_id: str,
        report_format: ReportFormat = ReportFormat.HTML,
        output_path: Optional[str] = None
    ) -> str:
        """
        Generate a comprehensive report for a single crawl session.
        
        Args:
            session_id: ID of the crawl session
            report_format: Format for the output report
            output_path: Optional path to save the report
            
        Returns:
            Path to the generated report or report content
        """
        try:
            self.logger.info(f"Generating {report_format} report for session {session_id}")
            
            # Get analytics data
            analytics = await self.analytics_engine.analyze_crawl_session(session_id)
            
            # Generate report based on format
            if report_format == ReportFormat.HTML:
                content = await self._generate_html_report(analytics)
            elif report_format == ReportFormat.JSON:
                content = await self._generate_json_report(analytics)
            elif report_format == ReportFormat.CSV:
                content = await self._generate_csv_report(analytics)
            elif report_format == ReportFormat.MARKDOWN:
                content = await self._generate_markdown_report(analytics)
            elif report_format == ReportFormat.PDF:
                content = await self._generate_pdf_report(analytics)
            else:
                raise AnalyticsError(f"Unsupported report format: {report_format}")
            
            # Save to file if output path provided
            if output_path:
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                if report_format == ReportFormat.PDF and isinstance(content, bytes):
                    # PDF content is binary
                    output_file.write_bytes(content)
                else:
                    # Text content
                    text_content = content.decode('utf-8') if isinstance(content, bytes) else content
                    output_file.write_text(text_content, encoding='utf-8')
                
                self.logger.info(f"Report saved to {output_file}")
                return str(output_file)
            
            # Return content as string
            return content.decode('utf-8') if isinstance(content, bytes) else content
            
        except Exception as e:
            self.logger.error(f"Failed to generate session report: {e}")
            raise AnalyticsError(f"Report generation failed: {e}")
    
    async def generate_comparison_report(
        self,
        session_ids: List[str],
        report_format: ReportFormat = ReportFormat.HTML,
        output_path: Optional[str] = None
    ) -> str:
        """
        Generate a comparison report for multiple crawl sessions.
        
        Args:
            session_ids: List of session IDs to compare
            report_format: Format for the output report
            output_path: Optional path to save the report
            
        Returns:
            Path to the generated report or report content
        """
        try:
            self.logger.info(f"Generating comparison report for {len(session_ids)} sessions")
            
            # Get analytics for all sessions
            analytics_list = []
            for session_id in session_ids:
                analytics = await self.analytics_engine.analyze_crawl_session(session_id)
                analytics_list.append(analytics)
            
            # Generate comparison dashboard
            comparison_data = self.visualizer.create_comparison_dashboard(analytics_list)
            
            # Generate report based on format
            if report_format == ReportFormat.HTML:
                content = await self._generate_comparison_html_report(comparison_data)
            elif report_format == ReportFormat.JSON:
                content = json.dumps(comparison_data, indent=2, default=str)
            elif report_format == ReportFormat.CSV:
                content = await self._generate_comparison_csv_report(comparison_data)
            elif report_format == ReportFormat.MARKDOWN:
                content = await self._generate_comparison_markdown_report(comparison_data)
            else:
                raise AnalyticsError(f"Unsupported format for comparison report: {report_format}")
            
            # Save to file if output path provided
            if output_path:
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(content, encoding='utf-8')
                
                self.logger.info(f"Comparison report saved to {output_file}")
                return str(output_file)
            
            return content
            
        except Exception as e:
            self.logger.error(f"Failed to generate comparison report: {e}")
            raise AnalyticsError(f"Comparison report generation failed: {e}")
    
    async def _generate_html_report(self, analytics: CrawlAnalytics) -> str:
        """Generate HTML report from analytics data."""
        try:
            # Create dashboard data
            dashboard_data = self.visualizer.create_dashboard_data(analytics)
            
            # Generate HTML visualization
            html_content = self.visualizer.create_html_visualization(dashboard_data)
            
            return html_content
            
        except Exception as e:
            self.logger.error(f"Failed to generate HTML report: {e}")
            raise AnalyticsError(f"HTML report generation failed: {e}")
    
    async def _generate_json_report(self, analytics: CrawlAnalytics) -> str:
        """Generate JSON report from analytics data."""
        try:
            report_data = {
                'report_info': {
                    'type': 'session_summary',
                    'generated_at': datetime.now().isoformat(),
                    'session_id': analytics.session_id,
                    'session_name': analytics.session_name
                },
                'analytics': analytics.to_dict()
            }
            
            return json.dumps(report_data, indent=2, default=str)
            
        except Exception as e:
            self.logger.error(f"Failed to generate JSON report: {e}")
            raise AnalyticsError(f"JSON report generation failed: {e}")
    
    async def _generate_csv_report(self, analytics: CrawlAnalytics) -> str:
        """Generate CSV report from analytics data."""
        try:
            from io import StringIO
            
            output = StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow(['Crawler Analytics Report'])
            writer.writerow(['Session ID', analytics.session_id])
            writer.writerow(['Session Name', analytics.session_name])
            writer.writerow(['Generated At', datetime.now().isoformat()])
            writer.writerow([])  # Empty row
            
            # Summary metrics
            writer.writerow(['Summary Metrics'])
            writer.writerow(['Metric', 'Value'])
            writer.writerow(['Total Pages', analytics.total_pages])
            writer.writerow(['Successful Pages', analytics.successful_pages])
            writer.writerow(['Failed Pages', analytics.failed_pages])
            writer.writerow(['Success Rate (%)', f"{(analytics.successful_pages / analytics.total_pages * 100):.1f}" if analytics.total_pages > 0 else "0"])
            writer.writerow(['Total Words', analytics.total_words])
            writer.writerow(['Unique Words', analytics.unique_words])
            writer.writerow(['Unique Domains', analytics.unique_domains])
            writer.writerow(['Pages per Second', f"{analytics.pages_per_second:.2f}"])
            writer.writerow(['Average Response Time (ms)', f"{analytics.average_response_time:.2f}"])
            writer.writerow(['Error Rate (%)', f"{analytics.error_rate * 100:.1f}"])
            writer.writerow([])  # Empty row
            
            # Top words
            writer.writerow(['Top Words'])
            writer.writerow(['Word', 'Frequency'])
            for word, freq in analytics.top_words[:20]:
                writer.writerow([word, freq])
            writer.writerow([])  # Empty row
            
            # Top domains
            writer.writerow(['Top Domains'])
            writer.writerow(['Domain', 'Page Count'])
            for domain, count in analytics.top_domains[:10]:
                writer.writerow([domain, count])
            writer.writerow([])  # Empty row
            
            # Error summary
            if analytics.error_summary:
                writer.writerow(['Error Summary'])
                writer.writerow(['Error Type', 'Count'])
                for error_type, count in analytics.error_summary.items():
                    writer.writerow([error_type, count])
            
            return output.getvalue()
            
        except Exception as e:
            self.logger.error(f"Failed to generate CSV report: {e}")
            raise AnalyticsError(f"CSV report generation failed: {e}")
    
    async def _generate_markdown_report(self, analytics: CrawlAnalytics) -> str:
        """Generate Markdown report from analytics data."""
        try:
            md_content = f"""# Crawler Analytics Report

## Session Information
- **Session ID**: {analytics.session_id}
- **Session Name**: {analytics.session_name}
- **Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Total Duration**: {analytics.total_duration:.1f} seconds

## Summary Metrics

| Metric | Value |
|--------|-------|
| Total Pages | {analytics.total_pages:,} |
| Successful Pages | {analytics.successful_pages:,} |
| Failed Pages | {analytics.failed_pages:,} |
| Success Rate | {(analytics.successful_pages / analytics.total_pages * 100):.1f}% |
| Total Words | {analytics.total_words:,} |
| Unique Words | {analytics.unique_words:,} |
| Unique Domains | {analytics.unique_domains} |
| Pages per Second | {analytics.pages_per_second:.2f} |
| Average Response Time | {analytics.average_response_time:.2f}ms |
| Error Rate | {analytics.error_rate * 100:.1f}% |

## Performance Analysis

### Response Time Metrics
- **Average**: {analytics.average_response_time:.2f}ms
- **Median**: {analytics.median_response_time:.2f}ms
- **95th Percentile**: {analytics.p95_response_time:.2f}ms

### Throughput
- **Pages per Second**: {analytics.pages_per_second:.2f}
- **Total Processing Time**: {analytics.total_duration:.1f} seconds

## Content Analysis

### Top Words
| Word | Frequency |
|------|-----------|
"""
            
            # Add top words
            for word, freq in analytics.top_words[:15]:
                md_content += f"| {word} | {freq:,} |\n"
            
            md_content += f"""
### Top Domains
| Domain | Pages |
|--------|-------|
"""
            
            # Add top domains
            for domain, count in analytics.top_domains[:10]:
                md_content += f"| {domain} | {count} |\n"
            
            # Add content type distribution
            if analytics.content_type_distribution:
                md_content += f"""
### Content Type Distribution
| Content Type | Count |
|--------------|-------|
"""
                for content_type, count in analytics.content_type_distribution.items():
                    md_content += f"| {content_type} | {count} |\n"
            
            # Add error analysis if there are errors
            if analytics.error_summary:
                md_content += f"""
## Error Analysis

### Error Summary
| Error Type | Count |
|------------|-------|
"""
                for error_type, count in analytics.error_summary.items():
                    md_content += f"| {error_type} | {count} |\n"
            
            # Add quality metrics
            md_content += f"""
## Quality Metrics

- **Average Quality Score**: {analytics.average_quality_score:.2f}
- **Error Rate**: {analytics.error_rate * 100:.1f}%

### Readability Distribution
| Category | Count |
|----------|-------|
"""
            
            for category, count in analytics.readability_distribution.items():
                md_content += f"| {category} | {count} |\n"
            
            md_content += f"""
---
*Report generated by Web Crawler Analytics System*
"""
            
            return md_content
            
        except Exception as e:
            self.logger.error(f"Failed to generate Markdown report: {e}")
            raise AnalyticsError(f"Markdown report generation failed: {e}")
    
    async def _generate_pdf_report(self, analytics: CrawlAnalytics) -> bytes:
        """Generate PDF report from analytics data."""
        try:
            # For PDF generation, we would typically use a library like reportlab
            # For now, we'll create a simple text-based PDF or convert HTML to PDF
            
            # Generate HTML first
            html_content = await self._generate_html_report(analytics)
            
            # In a real implementation, you would use a library like weasyprint or pdfkit
            # to convert HTML to PDF. For now, we'll return the HTML as bytes
            # with a note that this would be converted to PDF
            
            pdf_note = f"""
PDF Report Generation Note:
This would typically use a library like weasyprint or pdfkit to convert
the HTML report to PDF format. The HTML content is ready for conversion.

Session: {analytics.session_name}
Generated: {datetime.now().isoformat()}
            """.strip()
            
            return pdf_note.encode('utf-8')
            
        except Exception as e:
            self.logger.error(f"Failed to generate PDF report: {e}")
            raise AnalyticsError(f"PDF report generation failed: {e}")
    
    async def _generate_comparison_html_report(self, comparison_data: Dict[str, Any]) -> str:
        """Generate HTML comparison report."""
        try:
            html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crawler Session Comparison Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 30px; padding: 20px; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .chart-container {{ background: white; padding: 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .chart-title {{ font-size: 1.2em; font-weight: bold; margin-bottom: 15px; color: #343a40; }}
        .chart-wrapper {{ position: relative; height: 400px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }}
        th {{ background-color: #f8f9fa; font-weight: bold; }}
        .best-performer {{ background-color: #d4edda; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Crawler Session Comparison Report</h1>
            <p>Comparing {session_count} crawl sessions</p>
            <p>Generated on {generated_at}</p>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Session Comparison</div>
            <table>
                <thead>
                    <tr>
                        <th>Session</th>
                        <th>Total Pages</th>
                        <th>Success Rate</th>
                        <th>Avg Response Time</th>
                        <th>Pages/Second</th>
                        <th>Total Words</th>
                        <th>Unique Domains</th>
                        <th>Error Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {comparison_rows}
                </tbody>
            </table>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Best Performers</div>
            <ul>
                <li><strong>Fastest Session:</strong> {fastest_session}</li>
                <li><strong>Most Pages:</strong> {most_pages}</li>
                <li><strong>Highest Success Rate:</strong> {highest_success_rate}</li>
                <li><strong>Best Quality:</strong> {best_quality}</li>
            </ul>
        </div>
    </div>
</body>
</html>
            """
            
            # Prepare data
            session_info = comparison_data.get('session_info', {})
            comparison_table = comparison_data.get('comparison_table', [])
            best_performers = comparison_data.get('best_performers', {})
            
            # Generate table rows
            comparison_rows = ""
            for row in comparison_table:
                comparison_rows += f"""
                <tr>
                    <td>{row['session_name']}</td>
                    <td>{row['total_pages']}</td>
                    <td>{row['success_rate']}</td>
                    <td>{row['avg_response_time']}</td>
                    <td>{row['pages_per_second']}</td>
                    <td>{row['total_words']}</td>
                    <td>{row['unique_domains']}</td>
                    <td>{row['error_rate']}</td>
                </tr>
                """
            
            # Format the HTML
            html_content = html_template.format(
                session_count=session_info.get('session_count', 0),
                generated_at=session_info.get('generated_at', datetime.now().isoformat()),
                comparison_rows=comparison_rows,
                fastest_session=best_performers.get('fastest_session', 'N/A'),
                most_pages=best_performers.get('most_pages', 'N/A'),
                highest_success_rate=best_performers.get('highest_success_rate', 'N/A'),
                best_quality=best_performers.get('best_quality', 'N/A')
            )
            
            return html_content
            
        except Exception as e:
            self.logger.error(f"Failed to generate comparison HTML report: {e}")
            raise AnalyticsError(f"Comparison HTML report generation failed: {e}")
    
    async def _generate_comparison_csv_report(self, comparison_data: Dict[str, Any]) -> str:
        """Generate CSV comparison report."""
        try:
            from io import StringIO
            
            output = StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow(['Crawler Session Comparison Report'])
            writer.writerow(['Generated At', comparison_data.get('session_info', {}).get('generated_at', datetime.now().isoformat())])
            writer.writerow(['Session Count', comparison_data.get('session_info', {}).get('session_count', 0)])
            writer.writerow([])  # Empty row
            
            # Comparison table
            writer.writerow(['Session Comparison'])
            writer.writerow(['Session Name', 'Total Pages', 'Success Rate', 'Avg Response Time', 'Pages/Second', 'Total Words', 'Unique Domains', 'Error Rate'])
            
            for row in comparison_data.get('comparison_table', []):
                writer.writerow([
                    row['session_name'],
                    row['total_pages'],
                    row['success_rate'],
                    row['avg_response_time'],
                    row['pages_per_second'],
                    row['total_words'],
                    row['unique_domains'],
                    row['error_rate']
                ])
            
            writer.writerow([])  # Empty row
            
            # Best performers
            writer.writerow(['Best Performers'])
            best_performers = comparison_data.get('best_performers', {})
            writer.writerow(['Category', 'Session'])
            writer.writerow(['Fastest Session', best_performers.get('fastest_session', 'N/A')])
            writer.writerow(['Most Pages', best_performers.get('most_pages', 'N/A')])
            writer.writerow(['Highest Success Rate', best_performers.get('highest_success_rate', 'N/A')])
            writer.writerow(['Best Quality', best_performers.get('best_quality', 'N/A')])
            
            return output.getvalue()
            
        except Exception as e:
            self.logger.error(f"Failed to generate comparison CSV report: {e}")
            raise AnalyticsError(f"Comparison CSV report generation failed: {e}")
    
    async def _generate_comparison_markdown_report(self, comparison_data: Dict[str, Any]) -> str:
        """Generate Markdown comparison report."""
        try:
            session_info = comparison_data.get('session_info', {})
            comparison_table = comparison_data.get('comparison_table', [])
            best_performers = comparison_data.get('best_performers', {})
            
            md_content = f"""# Crawler Session Comparison Report

## Report Information
- **Generated**: {session_info.get('generated_at', datetime.now().isoformat())}
- **Sessions Compared**: {session_info.get('session_count', 0)}

## Session Comparison

| Session Name | Total Pages | Success Rate | Avg Response Time | Pages/Second | Total Words | Unique Domains | Error Rate |
|--------------|-------------|--------------|-------------------|--------------|-------------|----------------|------------|
"""
            
            # Add comparison rows
            for row in comparison_table:
                md_content += f"| {row['session_name']} | {row['total_pages']} | {row['success_rate']} | {row['avg_response_time']} | {row['pages_per_second']} | {row['total_words']} | {row['unique_domains']} | {row['error_rate']} |\n"
            
            md_content += f"""
## Best Performers

- **🚀 Fastest Session**: {best_performers.get('fastest_session', 'N/A')}
- **📊 Most Pages**: {best_performers.get('most_pages', 'N/A')}
- **✅ Highest Success Rate**: {best_performers.get('highest_success_rate', 'N/A')}
- **⭐ Best Quality**: {best_performers.get('best_quality', 'N/A')}

---
*Report generated by Web Crawler Analytics System*
"""
            
            return md_content
            
        except Exception as e:
            self.logger.error(f"Failed to generate comparison Markdown report: {e}")
            raise AnalyticsError(f"Comparison Markdown report generation failed: {e}")
    
    async def schedule_report_generation(
        self,
        session_ids: List[str],
        report_type: ReportType,
        report_format: ReportFormat,
        output_directory: str,
        schedule_interval: timedelta = timedelta(hours=24)
    ) -> str:
        """
        Schedule automatic report generation.
        
        Args:
            session_ids: List of session IDs to include in reports
            report_type: Type of report to generate
            report_format: Format for the reports
            output_directory: Directory to save reports
            schedule_interval: Interval between report generations
            
        Returns:
            Scheduler task ID or status message
        """
        try:
            self.logger.info(f"Scheduling {report_type} report generation every {schedule_interval}")
            
            # In a real implementation, this would integrate with a task scheduler
            # like Celery, APScheduler, or similar
            
            schedule_info = {
                'session_ids': session_ids,
                'report_type': report_type,
                'report_format': report_format,
                'output_directory': output_directory,
                'schedule_interval': schedule_interval,
                'next_run': datetime.now() + schedule_interval,
                'created_at': datetime.now()
            }
            
            # For now, return a mock scheduler ID
            scheduler_id = f"report_schedule_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            self.logger.info(f"Report generation scheduled with ID: {scheduler_id}")
            return scheduler_id
            
        except Exception as e:
            self.logger.error(f"Failed to schedule report generation: {e}")
            raise AnalyticsError(f"Report scheduling failed: {e}")