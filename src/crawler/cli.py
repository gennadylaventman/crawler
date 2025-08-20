"""
Command-line interface for the web crawler.
"""

import asyncio
import sys
import os
from typing import List, Optional
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from crawler.utils.config import get_config, CrawlConfig
from crawler.core.engine import CrawlerEngine
from crawler.storage.database import DatabaseManager
from crawler.utils.exceptions import CrawlerError, DatabaseError

console = Console()


@click.group()
def cli():
    """Web Crawler - A comprehensive web crawling system with analytics."""
    pass


@cli.command()
@click.option('--url', '-u', multiple=True, required=True, help='URLs to crawl')
@click.option('--depth', '-d', default=3, help='Maximum crawl depth')
@click.option('--pages', '-p', default=100, help='Maximum pages to crawl')
@click.option('--workers', '-w', default=10, help='Number of concurrent workers')
@click.option('--session-name', '-s', default='cli_crawl', help='Session name')
@click.option('--config', '-c', help='Configuration file path')
def crawl(url: tuple, depth: int, pages: int, workers: int, session_name: str, config: Optional[str]):
    """Start a web crawling session."""
    asyncio.run(_run_crawl(list(url), depth, pages, workers, session_name, config))


async def _run_crawl(urls: List[str], depth: int, pages: int, workers: int, session_name: str, config_path: Optional[str]):
    """Run the crawling session."""
    try:
        # Load configuration
        if config_path:
            os.environ['CRAWLER_CONFIG_PATH'] = config_path
        
        # Use development environment for better debugging
        config = get_config("development")
        
        # Override with CLI parameters
        config.crawler.max_depth = depth
        config.crawler.max_pages = pages
        config.crawler.concurrent_workers = workers
        config.start_urls = urls
        config.session_name = session_name
        
        console.print(f"[bold green]Starting crawl session: {session_name}[/bold green]")
        console.print(f"URLs: {', '.join(urls)}")
        console.print(f"Max depth: {depth}, Max pages: {pages}, Workers: {workers}")
        
        # Initialize and run crawler
        async with CrawlerEngine(config) as crawler:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task("Crawling...", total=None)
                
                session_id = await crawler.start_crawl(urls, session_name)
                
                progress.update(task, description="Crawl completed!")
        
        console.print(f"[bold green]✓ Crawl completed! Session ID: {session_id}[/bold green]")
        
    except Exception as e:
        console.print(f"[bold red]✗ Crawl failed: {e}[/bold red]")
        sys.exit(1)


@cli.command()
@click.option('--session-id', '-s', help='Session ID to analyze')
@click.option('--limit', '-l', default=20, help='Number of top words to show')
def analyze(session_id: Optional[str], limit: int):
    """Analyze crawl results."""
    asyncio.run(_run_analysis(session_id, limit))


async def _run_analysis(session_id: Optional[str], limit: int):
    """Run analysis on crawl results."""
    try:
        config = get_config()
        db_manager = DatabaseManager(config.database)
        await db_manager.initialize(auto_migrate=False)
        
        if not session_id:
            # Show available sessions
            console.print("[bold blue]Available crawl sessions:[/bold blue]")
            # This would require a method to list sessions
            console.print("Please specify a session ID with --session-id")
            return
        
        # Get session statistics
        stats = await db_manager.get_session_statistics(session_id)
        
        if not stats:
            console.print(f"[bold red]✗ Session {session_id} not found[/bold red]")
            return
        
        # Display session info
        session_info = stats.get('session_info', {})
        page_stats = stats.get('page_statistics', {})
        timing_breakdown = stats.get('timing_breakdown', {})
        top_words = stats.get('top_words', [])
        
        console.print(f"[bold green]Session Analysis: {session_info.get('name', 'Unknown')}[/bold green]")
        
        # Session overview table
        overview_table = Table(title="Session Overview")
        overview_table.add_column("Metric", style="cyan")
        overview_table.add_column("Value", style="magenta")
        
        overview_table.add_row("Status", session_info.get('status', 'Unknown'))
        overview_table.add_row("Pages Processed", str(page_stats.get('pages_processed', 0)))
        overview_table.add_row("Total Words", str(page_stats.get('total_words', 0)))
        overview_table.add_row("Error Rate", f"{page_stats.get('error_rate', 0) or 0:.2f}%")
        overview_table.add_row("Avg Response Time", f"{page_stats.get('avg_response_time', 0) or 0:.2f}ms")
        
        console.print(overview_table)
        
        # Performance breakdown table
        if timing_breakdown:
            perf_table = Table(title="Performance Breakdown")
            perf_table.add_column("Operation", style="cyan")
            perf_table.add_column("Avg Time (ms)", style="magenta")
            
            perf_table.add_row("DNS Lookup", f"{timing_breakdown.get('avg_dns_time', 0) or 0:.2f}")
            perf_table.add_row("Server Response", f"{timing_breakdown.get('avg_server_time', 0) or 0:.2f}")
            perf_table.add_row("HTML Parsing", f"{timing_breakdown.get('avg_parse_time', 0) or 0:.2f}")
            perf_table.add_row("Text Extraction", f"{timing_breakdown.get('avg_extraction_time', 0) or 0:.2f}")
            perf_table.add_row("Word Counting", f"{timing_breakdown.get('avg_counting_time', 0) or 0:.2f}")
            perf_table.add_row("Database Insert", f"{timing_breakdown.get('avg_db_time', 0) or 0:.2f}")
            
            console.print(perf_table)
        
        # Top words table
        if top_words:
            words_table = Table(title=f"Top {min(limit, len(top_words))} Words")
            words_table.add_column("Rank", style="cyan")
            words_table.add_column("Word", style="green")
            words_table.add_column("Frequency", style="magenta")
            words_table.add_column("Pages", style="yellow")
            
            for i, word_data in enumerate(top_words[:limit], 1):
                words_table.add_row(
                    str(i),
                    word_data['word'],
                    str(word_data['frequency']),
                    str(word_data.get('pages', 'N/A'))
                )
            
            console.print(words_table)
        
        await db_manager.close()
        
    except Exception as e:
        console.print(f"[bold red]✗ Analysis failed: {e}[/bold red]")
        sys.exit(1)


@cli.command()
@click.option('--recreate', is_flag=True, help='Recreate schema (destroys all data)')
def migrate(recreate: bool):
    """Run database migrations."""
    asyncio.run(_run_migrations(recreate))


async def _run_migrations(recreate: bool):
    """Run database migrations."""
    try:
        config = get_config()
        db_manager = DatabaseManager(config.database)
        await db_manager.initialize(auto_migrate=False)
        
        if recreate:
            console.print("[bold yellow]⚠️  Recreating database schema (this will destroy all data!)[/bold yellow]")
            if click.confirm("Are you sure you want to continue?"):
                success = await db_manager.recreate_schema()
                if success:
                    console.print("[bold green]✓ Schema recreated successfully[/bold green]")
                else:
                    console.print("[bold red]✗ Schema recreation failed[/bold red]")
            else:
                console.print("Operation cancelled")
        else:
            console.print("[bold blue]Running database migrations...[/bold blue]")
            success = await db_manager.migrate_to_latest()
            if success:
                console.print("[bold green]✓ Migrations completed successfully[/bold green]")
            else:
                console.print("[bold red]✗ Migrations failed[/bold red]")
        
        # Show migration status
        status = await db_manager.get_migration_status()
        console.print(f"Current version: {status.get('current_version', 'None')}")
        console.print(f"Applied migrations: {status.get('applied_count', 0)}")
        console.print(f"Pending migrations: {status.get('pending_count', 0)}")
        
        await db_manager.close()
        
    except Exception as e:
        console.print(f"[bold red]✗ Migration failed: {e}[/bold red]")
        sys.exit(1)


@cli.command()
def status():
    """Show system status and configuration."""
    asyncio.run(_show_status())


async def _show_status():
    """Show system status."""
    try:
        config = get_config()
        
        # Configuration table
        config_table = Table(title="Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="magenta")
        
        config_table.add_row("Database Host", config.database.host)
        config_table.add_row("Database Port", str(config.database.port))
        config_table.add_row("Database Name", config.database.database)
        config_table.add_row("Max Depth", str(config.crawler.max_depth))
        config_table.add_row("Max Pages", str(config.crawler.max_pages))
        config_table.add_row("Concurrent Workers", str(config.crawler.concurrent_workers))
        config_table.add_row("Rate Limit", f"{config.crawler.rate_limit_delay}s")
        
        console.print(config_table)
        
        # Test database connection
        try:
            db_manager = DatabaseManager(config.database)
            await db_manager.initialize(auto_migrate=False)
            
            status = await db_manager.get_migration_status()
            
            db_table = Table(title="Database Status")
            db_table.add_column("Metric", style="cyan")
            db_table.add_column("Value", style="green")
            
            db_table.add_row("Connection", "✓ Connected")
            db_table.add_row("Current Version", status.get('current_version', 'None'))
            db_table.add_row("Up to Date", "✓ Yes" if status.get('is_up_to_date') else "✗ No")
            
            console.print(db_table)
            
            await db_manager.close()
            
        except Exception as e:
            console.print(f"[bold red]✗ Database connection failed: {e}[/bold red]")
        
    except Exception as e:
        console.print(f"[bold red]✗ Status check failed: {e}[/bold red]")
        sys.exit(1)


if __name__ == '__main__':
    cli()