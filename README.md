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
- **[Architecture Design](architecture_design.md)** - System architecture overview with updated flow diagrams

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

**Option A: Using Docker Compose (Recommended for Development)**
```bash
# Start PostgreSQL database in a container
docker-compose -f docker-compose.debug.yml up -d postgres

# Check if the database is running
docker-compose -f docker-compose.debug.yml ps

# View database logs (optional)
docker-compose -f docker-compose.debug.yml logs postgres
```

The Docker Compose setup provides:
- PostgreSQL 15 with Alpine Linux
- Pre-configured database (`webcrawler`) and user (`crawler`)
- Data persistence with named volumes
- Health checks for reliable startup
- Default password: `crawler_password`

**Managing the Docker Database:**
```bash
# Stop the database container
docker-compose -f docker-compose.debug.yml down

# Stop and remove all data (WARNING: destroys all data)
docker-compose -f docker-compose.debug.yml down -v

# Restart the database
docker-compose -f docker-compose.debug.yml restart postgres

# Access the database directly
docker-compose -f docker-compose.debug.yml exec postgres psql -U crawler -d webcrawler
```

**Option B: Manual PostgreSQL Installation**
```bash
# Create database and user manually
createdb webcrawler
createuser crawler
```

5. Configure database connection:
```bash
# For Docker Compose setup (default password)
export DB_PASSWORD=crawler_password

# For manual setup (your custom password)
export DB_PASSWORD=your_password
```

6. Set Python path:
   ```bash
   export PYTHONPATH=src
   ```

7. Run database migrations:
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

#### 1. `crawl` - Start Web Crawling

Start a web crawling session with specified URLs and parameters.

```bash
python src/crawler/cli.py crawl [OPTIONS]
```

**Options:**
- `--url, -u` (required, multiple): URLs to crawl
- `--depth, -d` (default: 3): Maximum crawl depth
- `--pages, -p` (default: 100): Maximum pages to crawl
- `--workers, -w` (default: 10): Number of concurrent workers
- `--session-name, -s` (default: 'cli_crawl'): Session name for identification
- `--config, -c`: Custom configuration file path

**Examples:**
```bash
# Basic crawl
python src/crawler/cli.py crawl -u https://example.com

# Advanced crawl with multiple URLs
python src/crawler/cli.py crawl \
  -u https://example.com \
  -u https://another-site.com \
  -d 5 \
  -p 500 \
  -w 20 \
  -s "my_crawl_session"

# Using custom config
python src/crawler/cli.py crawl \
  -u https://example.com \
  -c /path/to/custom/config.yaml
```

#### 2. `analyze` - Analyze Crawl Results

Analyze and display statistics for a completed crawl session.

```bash
python src/crawler/cli.py analyze [OPTIONS]
```

**Options:**
- `--session-id, -s`: Session ID to analyze
- `--limit, -l` (default: 20): Number of top words to show

**Examples:**
```bash
# Analyze specific session
python src/crawler/cli.py analyze -s "session-uuid-here"

# Show top 50 words
python src/crawler/cli.py analyze -s "session-uuid-here" -l 50
```

#### 3. `migrate` - Database Migrations

Run database migrations or recreate the schema.

```bash
python src/crawler/cli.py migrate [OPTIONS]
```

**Options:**
- `--recreate`: Recreate schema (destroys all data)

**Examples:**
```bash
# Run migrations
python src/crawler/cli.py migrate

# Recreate schema (WARNING: destroys all data)
python src/crawler/cli.py migrate --recreate
```

#### 4. `status` - System Status

Show system status and configuration information.

```bash
python src/crawler/cli.py status
```

#### 5. `report` - Generate Reports

Generate comprehensive reports for crawl sessions.

```bash
python src/crawler/cli.py report [OPTIONS]
```

**Options:**
- `--session-id, -s` (required): Session ID to generate report for
- `--format, -f`: Report format (html, json, csv, markdown, pdf)
- `--output, -o`: Output file path (optional)

**Examples:**
```bash
# Generate HTML report
python src/crawler/cli.py report -s "session-uuid-here" -f html

# Save to specific file
python src/crawler/cli.py report \
  -s "session-uuid-here" \
  -f pdf \
  -o "/path/to/report.pdf"
```

#### 6. `analytics` - Detailed Analytics

Perform detailed analytics analysis on crawl sessions.

```bash
python src/crawler/cli.py analytics [OPTIONS]
```

**Options:**
- `--session-id, -s` (required): Session ID to analyze
- `--trends`: Include performance trend analysis
- `--output, -o`: Output file path for detailed results (JSON)

**Examples:**
```bash
# Basic analytics
python src/crawler/cli.py analytics -s "session-uuid-here"

# With trends and output file
python src/crawler/cli.py analytics \
  -s "session-uuid-here" \
  --trends \
  -o "analytics_results.json"
```


### Configuration File Structure

The crawler uses a YAML configuration file with environment-specific sections. The default configuration is located at `config/default.yaml`.

#### File Structure Overview

```yaml
# Multi-environment configuration file
default:
  # Base configuration used by all environments
  database: { ... }
  crawler: { ... }
  content: { ... }
  session_name: "debug_crawl"
  start_urls: []
  allowed_domains: null
  blocked_domains: null

development:
  # Development-specific overrides and additional settings
  database: { ... }
  crawler: { ... }
  content: { ... }
  session_name: "development_crawl"
  start_urls: []
  allowed_domains: null
  blocked_domains: null
```

#### Configuration Sections

##### Database Configuration
```yaml
database:
  host: localhost
  port: 5432
  database: webcrawler
  username: crawler
  password: ${DB_PASSWORD}  # Environment variable substitution
  pool_size: 20
  max_overflow: 10
  pool_timeout: 30
```

##### Crawler Configuration
```yaml
crawler:
  max_depth: 3
  max_pages: 1000
  concurrent_workers: 10
  rate_limit_delay: 1.0
  request_timeout: 30
  max_retries: 3
  user_agent: "WebCrawler/1.0 (+https://example.com/bot)"
  max_connections: 100
  max_connections_per_host: 20
  dns_cache_ttl: 300
  keepalive_timeout: 30
```

##### Content Processing Configuration
```yaml
content:
  max_page_size: 10485760  # 10MB
  allowed_content_types:
    - "text/html"
    - "application/xhtml+xml"
    - "text/xml"
    - "application/xml"
  remove_scripts: true
  remove_styles: true
  min_text_length: 100
  max_words_per_page: 50000
  chunk_size: 8192
```

#### Environment Variable Overrides

The following environment variables can override configuration settings:

| Environment Variable | Configuration Path | Description |
|---------------------|-------------------|-------------|
| `DB_HOST` | `database.host` | Database host |
| `DB_PORT` | `database.port` | Database port |
| `DB_NAME` | `database.database` | Database name |
| `DB_USER` | `database.username` | Database username |
| `DB_PASSWORD` | `database.password` | Database password |
| `CRAWLER_MAX_DEPTH` | `crawler.max_depth` | Maximum crawl depth |
| `CRAWLER_MAX_PAGES` | `crawler.max_pages` | Maximum pages to crawl |
| `CRAWLER_WORKERS` | `crawler.concurrent_workers` | Number of workers |
| `CRAWLER_RATE_LIMIT` | `crawler.rate_limit_delay` | Rate limit delay |
| `LOG_LEVEL` | `monitoring.log_level` | Logging level |

## CLI Commands

### 1. `crawl` - Start Web Crawling

Start a web crawling session with specified URLs and parameters.

```bash
python src/crawler/cli.py crawl [OPTIONS]
```

**Options:**
- `--url, -u` (required, multiple): URLs to crawl
- `--depth, -d` (default: 3): Maximum crawl depth
- `--pages, -p` (default: 100): Maximum pages to crawl
- `--workers, -w` (default: 10): Number of concurrent workers
- `--session-name, -s` (default: 'cli_crawl'): Session name for identification
- `--config, -c`: Custom configuration file path

**Examples:**
```bash
# Basic crawl
python src/crawler/cli.py crawl -u https://example.com

# Advanced crawl with multiple URLs
python src/crawler/cli.py crawl \
  -u https://example.com \
  -u https://another-site.com \
  -d 5 \
  -p 500 \
  -w 20 \
  -s "my_crawl_session"

# Using custom config
python src/crawler/cli.py crawl \
  -u https://example.com \
  -c /path/to/custom/config.yaml
```

### 2. `analyze` - Analyze Crawl Results

Analyze and display statistics for a completed crawl session.

```bash
python src/crawler/cli.py analyze [OPTIONS]
```

**Options:**
- `--session-id, -s`: Session ID to analyze
- `--limit, -l` (default: 20): Number of top words to show

**Examples:**
```bash
# Analyze specific session
python src/crawler/cli.py analyze -s "session-uuid-here"

# Show top 50 words
python src/crawler/cli.py analyze -s "session-uuid-here" -l 50
```

### 3. `migrate` - Database Migrations

Run database migrations or recreate the schema.

```bash
python src/crawler/cli.py migrate [OPTIONS]
```

**Options:**
- `--recreate`: Recreate schema (destroys all data)

**Examples:**
```bash
# Run migrations
python src/crawler/cli.py migrate

# Recreate schema (WARNING: destroys all data)
python src/crawler/cli.py migrate --recreate
```

### 4. `status` - System Status

Show system status and configuration information.

```bash
python src/crawler/cli.py status
```

### 5. `report` - Generate Reports

Generate comprehensive reports for crawl sessions.

```bash
python src/crawler/cli.py report [OPTIONS]
```

**Options:**
- `--session-id, -s` (required): Session ID to generate report for
- `--format, -f`: Report format (html, json, csv, markdown, pdf)
- `--output, -o`: Output file path (optional)

**Examples:**
```bash
# Generate HTML report
python src/crawler/cli.py report -s "session-uuid-here" -f html

# Save to specific file
python src/crawler/cli.py report \
  -s "session-uuid-here" \
  -f pdf \
  -o "/path/to/report.pdf"
```

### 6. `analytics` - Detailed Analytics

Perform detailed analytics analysis on crawl sessions.

```bash
python src/crawler/cli.py analytics [OPTIONS]
```

**Options:**
- `--session-id, -s` (required): Session ID to analyze
- `--trends`: Include performance trend analysis
- `--output, -o`: Output file path for detailed results (JSON)

**Examples:**
```bash
# Basic analytics
python src/crawler/cli.py analytics -s "session-uuid-here"

# With trends and output file
python src/crawler/cli.py analytics \
  -s "session-uuid-here" \
  --trends \
  -o "analytics_results.json"
```

## Database Schema

The system uses a comprehensive PostgreSQL schema with the following main tables:

- **crawl_sessions**: Crawl session metadata and configuration
- **pages**: Individual page data with comprehensive metrics
- **word_frequencies**: Word frequency analysis results
- **links**: Discovered links and relationships
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

## Content Analysis

The system provides comprehensive content analysis:

### Text Processing
- HTML parsing and cleaning
- Script and style removal

### Word Analysis
- Word frequency counting
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


## Development

### Project Structure
```
crawler/
├── src/crawler/           # Main package
│   ├── cli.py            # Command-line interface
│   ├── core/             # Core crawler engine with WorkerPool
│   │   ├── engine.py     # Main crawler engine
│   │   ├── worker.py     # WorkerPool and CrawlerWorker classes
│   │   ├── session.py    # Crawl session management
│   │   └── queue_factory.py  # Queue factory for URL management
│   ├── url_management/   # URL queue and management
│   │   ├── queue.py      # Priority-based URL queue with bloom filters
│   │   ├── validator.py  # URL validation and normalization
│   │   └── robots.py     # Robots.txt compliance
│   ├── content/          # Content processing pipeline
│   │   ├── extractor.py  # HTML content extraction
│   │   ├── processor.py  # Text processing and cleaning
│   │   └── analyzer.py   # Word frequency analysis
│   ├── storage/          # Database and storage
│   │   ├── database.py   # Database manager and operations
│   │   ├── migrations.py # Database migration system
│   │   └── persistent_queue.py  # Persistent queue implementation
│   ├── monitoring/       # Metrics and performance profiling
│   │   ├── logger.py     # Logging configuration and utilities
│   │   ├── metrics.py    # Metrics collection and reporting
│   │   └── profiler.py   # Performance profiling system
│   ├── reporting/        # Report generation and analytics
│   │   ├── generator.py  # Report generation engine
│   │   ├── analytics.py  # Advanced analytics and insights
│   │   └── visualizer.py # Data visualization components
│   └── utils/            # Utilities and configuration
│       ├── config.py     # Configuration management
│       ├── exceptions.py # Custom exception classes
│       └── helpers.py    # Helper functions and utilities
└── config/               # Configuration files
```