# Web Crawler - Usage Guide

This document provides comprehensive instructions for running the web crawler system, including all CLI options, configuration details, and database setup.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Database Password Configuration](#database-password-configuration)
4. [CLI Commands](#cli-commands)
5. [Configuration File Structure](#configuration-file-structure)
6. [Examples](#examples)
7. [Troubleshooting](#troubleshooting)

## Prerequisites

- Python 3.8+
- PostgreSQL database
- Virtual environment (recommended)

## Environment Setup

1. **Activate virtual environment:**
   ```bash
   source venv/bin/activate
   ```

2. **Set Python path:**
   ```bash
   export PYTHONPATH=src
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Database Password Configuration

The crawler supports multiple ways to configure the database password:

### Method 1: Environment Variable (Recommended)
```bash
export DB_PASSWORD="your_secure_password"
```

### Method 2: Configuration File
Edit `config/default.yaml` and set the password directly:
```yaml
development:
  database:
    password: "your_secure_password"
```

### Method 3: Environment Variable in Config
Use environment variable substitution in the config file:
```yaml
development:
  database:
    password: ${DB_PASSWORD}
```

### Other Database Environment Variables
```bash
export DB_HOST="localhost"          # Database host
export DB_PORT="5432"              # Database port
export DB_NAME="webcrawler"        # Database name
export DB_USER="crawler"           # Database username
```

## CLI Commands

The crawler provides several commands for different operations:

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

## Configuration File Structure

The crawler uses a YAML configuration file with environment-specific sections. The default configuration is located at `config/default.yaml`.

### File Structure Overview

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

### Configuration Sections

#### Database Configuration
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

#### Crawler Configuration
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

#### Content Processing Configuration
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

### Environment Variable Overrides

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

## Examples

### Complete Crawling Workflow

1. **Set up environment:**
   ```bash
   source venv/bin/activate
   export PYTHONPATH=src
   export DB_PASSWORD="your_secure_password"
   ```

2. **Check system status:**
   ```bash
   python src/crawler/cli.py status
   ```

3. **Run database migrations:**
   ```bash
   python src/crawler/cli.py migrate
   ```

4. **Start crawling:**
   ```bash
   python src/crawler/cli.py crawl \
     -u https://example.com \
     -d 3 \
     -p 100 \
     -w 5 \
     -s "example_crawl"
   ```

5. **Analyze results:**
   ```bash
   python src/crawler/cli.py analyze -s "session-id-from-step-4"
   ```

6. **Generate report:**
   ```bash
   python src/crawler/cli.py report \
     -s "session-id-from-step-4" \
     -f html \
     -o "crawl_report.html"
   ```

### Custom Configuration Example

Create a custom configuration file `my-config.yaml`:

```yaml
default:
  database:
    host: localhost
    port: 5432
    database: my_crawler_db
    username: my_user
    password: ${MY_DB_PASSWORD}
  
  crawler:
    max_depth: 5
    max_pages: 1000
    concurrent_workers: 15
    rate_limit_delay: 0.5

development:
  crawler:
    max_depth: 2
    max_pages: 50
    concurrent_workers: 3
```

Use it with:
```bash
export MY_DB_PASSWORD="my_password"
python src/crawler/cli.py crawl \
  -u https://example.com \
  -c my-config.yaml
```

## Troubleshooting

### Common Issues

1. **KeyError: 'default'**
   - Ensure your configuration file has both `default` and environment sections
   - Check that the YAML structure is correct with proper indentation

2. **Database Connection Failed**
   - Verify database credentials and connection details
   - Ensure PostgreSQL is running and accessible
   - Check that the database exists

3. **Import Errors**
   - Make sure `PYTHONPATH=src` is set
   - Verify all dependencies are installed: `pip install -r requirements.txt`

4. **Permission Errors**
   - Ensure the crawler has write permissions for log and output directories
   - Check database user permissions

### Debug Mode

For detailed debugging, set the log level to DEBUG:
```bash
export LOG_LEVEL="DEBUG"
python src/crawler/cli.py crawl -u https://example.com
```

### Configuration Validation

To validate your configuration without running a crawl:
```bash
python src/crawler/cli.py status
```

This will show your current configuration and test the database connection.