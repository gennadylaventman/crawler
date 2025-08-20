# Web Crawler System

A comprehensive web crawler with advanced analytics capabilities, designed for large-scale operations with detailed performance monitoring and word frequency analysis.

## Features

- **Asynchronous Crawling**: High-performance concurrent processing with configurable worker pools
- **Advanced Analytics**: Comprehensive word frequency analysis and content insights
- **Database Integration**: PostgreSQL with optimized schema and migration system
- **Performance Monitoring**: Detailed timing metrics and system resource tracking
- **Configurable Limits**: Depth control, rate limiting, and content filtering
- **Rich CLI Interface**: Easy-to-use command-line interface with progress tracking
- **Scalable Architecture**: Designed for crawling 1000+ pages with comprehensive metrics

## Architecture

The system consists of several key components:

- **Core Engine**: Asyncio-based crawler with WorkerPool architecture for concurrent processing
- **WorkerPool System**: Individual worker instances handling URL processing with performance profiling
- **Content Processor**: Text extraction, cleaning, and word frequency analysis
- **Database Layer**: PostgreSQL with migration system and optimized indexing
- **URL Management**: Priority-based queue with bloom filter deduplication and domain-based rate limiting
- **Monitoring System**: Real-time metrics collection and performance tracking
- **Configuration Management**: YAML-based configuration with environment overrides

### Detailed Architecture Documentation

- **[WorkerPool Architecture](worker_pool_architecture.md)** - Comprehensive documentation of the concurrent processing system
- **[Implementation Plan](implementation_plan.md)** - Updated technical specifications and API documentation
- **[Architecture Design](architecture_design.md)** - System architecture overview with updated flow diagrams
- **[Performance Analysis](performance_analysis.md)** - Performance monitoring and optimization strategies

## Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 12+
- Required Python packages (see requirements.txt)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd crawler
```

2. Create and activate virtual environment:
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. Set up PostgreSQL database:
```bash
# Create database and user
createdb webcrawler
createuser crawler
```

5. Configure database connection:
```bash
export DB_PASSWORD=your_password
```

6. Run database migrations:
```bash
python -m crawler.cli migrate
```

**Note**: Always activate the virtual environment before running any commands:
```bash
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### Basic Usage

**Important**: Make sure your virtual environment is activated before running any commands:
```bash
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows
```

1. **Start a crawl session**:
```bash
python -m crawler.cli crawl -u https://example.com -d 2 -p 50 -s my_crawl
```

2. **Check system status**:
```bash
python -m crawler.cli status
```

3. **Analyze results**:
```bash
python -m crawler.cli analyze -s <session-id> -l 20
```

## Configuration

The system uses YAML configuration files with environment variable overrides:

### Default Configuration (`config/default.yaml`)

```yaml
database:
  host: localhost
  port: 5432
  database: webcrawler
  username: crawler
  password: ${DB_PASSWORD}

crawler:
  max_depth: 3
  max_pages: 1000
  concurrent_workers: 10
  rate_limit_delay: 1.0
  request_timeout: 30

content:
  max_page_size: 10485760  # 10MB
  remove_scripts: true
  remove_styles: true
  min_text_length: 100

monitoring:
  enable_metrics: true
  metrics_interval: 60
  log_level: INFO
```

### Environment Variables

- `DB_PASSWORD`: Database password
- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `CRAWLER_MAX_DEPTH`: Maximum crawl depth
- `CRAWLER_MAX_PAGES`: Maximum pages to crawl
- `CRAWLER_WORKERS`: Number of concurrent workers
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

## CLI Commands

### Crawl Command
```bash
# Make sure venv is activated first
source venv/bin/activate

python -m crawler.cli crawl [OPTIONS]

Options:
  -u, --url TEXT          URLs to crawl (multiple allowed) [required]
  -d, --depth INTEGER     Maximum crawl depth [default: 3]
  -p, --pages INTEGER     Maximum pages to crawl [default: 100]
  -w, --workers INTEGER   Number of concurrent workers [default: 10]
  -s, --session-name TEXT Session name [default: cli_crawl]
  -c, --config TEXT       Configuration file path
```

### Analyze Command
```bash
# Make sure venv is activated first
source venv/bin/activate

python -m crawler.cli analyze [OPTIONS]

Options:
  -s, --session-id TEXT   Session ID to analyze
  -l, --limit INTEGER     Number of top words to show [default: 20]
```

### Migrate Command
```bash
# Make sure venv is activated first
source venv/bin/activate

python -m crawler.cli migrate [OPTIONS]

Options:
  --recreate              Recreate schema (destroys all data)
```

### Status Command
```bash
# Make sure venv is activated first
source venv/bin/activate

python -m crawler.cli status
```

## Database Schema

The system uses a comprehensive PostgreSQL schema with the following main tables:

- **crawl_sessions**: Crawl session metadata and configuration
- **pages**: Individual page data with comprehensive metrics
- **word_frequencies**: Word frequency analysis results
- **links**: Discovered links and relationships
- **session_metrics_timeseries**: Time-series performance data
- **error_events**: Error tracking and analysis

### Migration System

The system includes an Alembic-style migration system:

- **Versioned migrations**: Each migration has a version number and dependencies
- **Up/down migrations**: Support for both applying and rolling back changes
- **Schema recreation**: Ability to recreate the entire schema from scratch
- **Migration tracking**: Database table tracks applied migrations

## Performance Metrics

The system collects detailed performance metrics:

### Page-Level Metrics
- DNS lookup time
- TCP connection time
- Server response time
- HTML parsing time
- Text extraction time
- Word counting time
- Database insertion time
- Total processing time

### Session-Level Metrics
- Pages per second
- Words per second
- Average response times
- Error rates
- Resource utilization

### System-Level Metrics
- CPU usage
- Memory usage
- Network I/O
- Disk I/O
- Active connections

## Content Analysis

The system provides comprehensive content analysis:

### Text Processing
- HTML parsing and cleaning
- Script and style removal
- Text normalization
- Language detection
- Readability scoring

### Word Analysis
- Word frequency counting
- Stop word filtering
- Word length analysis
- Unique word tracking
- Top words identification

### Link Analysis
- Internal vs external link classification
- Link discovery and tracking
- Broken link detection
- Link relationship mapping

## API Usage

You can also use the crawler programmatically:

```python
import asyncio
from crawler.utils.config import get_config
from crawler.core.engine import CrawlerEngine

async def main():
    config = get_config()
    config.start_urls = ['https://example.com']
    config.crawler.max_pages = 50
    
    async with CrawlerEngine(config) as crawler:
        session_id = await crawler.start_crawl(
            ['https://example.com'], 
            'my_session'
        )
        
        stats = await crawler.get_crawl_statistics()
        print(f"Crawled {stats['pages_crawled']} pages")

asyncio.run(main())
```

## Performance Tuning

### Recommended Settings

For **small-scale crawling** (< 100 pages):
```yaml
crawler:
  concurrent_workers: 5
  rate_limit_delay: 2.0
  max_pages: 100
```

For **medium-scale crawling** (100-1000 pages):
```yaml
crawler:
  concurrent_workers: 10
  rate_limit_delay: 1.0
  max_pages: 1000
```

For **large-scale crawling** (1000+ pages):
```yaml
crawler:
  concurrent_workers: 20
  rate_limit_delay: 0.5
  max_pages: 10000
```

### Hardware Recommendations

- **CPU**: 4+ cores for concurrent processing
- **RAM**: 8GB+ for large-scale operations
- **Storage**: SSD for database performance
- **Network**: High-bandwidth connection for concurrent requests

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   - Check PostgreSQL is running
   - Verify database credentials
   - Ensure database exists

2. **Memory Issues**:
   - Reduce concurrent workers
   - Lower max_pages limit
   - Enable content size limits

3. **Rate Limiting**:
   - Increase rate_limit_delay
   - Reduce concurrent workers
   - Check robots.txt compliance

4. **Import Errors**:
   - Install all dependencies
   - Check Python path
   - Verify module structure

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python -m crawler.cli crawl -u https://example.com
```

## Development

### Project Structure
```
crawler/
├── src/crawler/           # Main package
│   ├── core/             # Core crawler engine with WorkerPool
│   │   ├── engine.py     # Main crawler engine
│   │   ├── worker.py     # WorkerPool and CrawlerWorker classes
│   │   └── session.py    # Crawl session management
│   ├── url_management/   # URL queue and management
│   │   ├── queue.py      # Priority-based URL queue with bloom filters
│   │   ├── validator.py  # URL validation and normalization
│   │   └── robots.py     # Robots.txt compliance
│   ├── content/          # Content processing pipeline
│   │   ├── extractor.py  # HTML content extraction
│   │   ├── processor.py  # Text processing and cleaning
│   │   └── analyzer.py   # Word frequency analysis
│   ├── storage/          # Database and storage
│   ├── monitoring/       # Metrics and performance profiling
│   └── utils/            # Utilities and config
├── config/               # Configuration files
├── tests/                # Test suite
├── worker_pool_architecture.md  # WorkerPool documentation
└── docs/                 # Additional documentation
```

### Key Implementation Notes

- **URL Queue Storage**: Currently uses in-memory `asyncio.PriorityQueue` with bloom filter deduplication (not PostgreSQL as originally planned)
- **WorkerPool Pattern**: Implements sophisticated concurrent processing with individual worker instances
- **Performance Profiling**: Built-in profiling system with detailed timing metrics for all operations
- **API Signatures**: Updated to match actual implementation (see `implementation_plan.md` for details)

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
black src/
flake8 src/
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run code quality checks
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue on GitHub
- Check the troubleshooting section
- Review the configuration documentation