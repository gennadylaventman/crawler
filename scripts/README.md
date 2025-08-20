# Scripts Directory

This directory contains utility scripts for setting up, running, and deploying the web crawler.

## Scripts Overview

### 🔧 setup.sh
**Purpose**: Initial setup and environment preparation

**Usage**:
```bash
./scripts/setup.sh
```

**What it does**:
- Checks Python 3.9+ installation
- Checks PostgreSQL availability
- Creates virtual environment
- Installs Python dependencies
- Sets up database
- Creates necessary directories
- Downloads NLTK data
- Runs basic tests to verify setup

**Requirements**:
- Python 3.9+
- PostgreSQL (optional but recommended)
- Internet connection for package downloads

### 🚀 run_crawler.sh
**Purpose**: Convenient interface for running crawler operations

**Usage**:
```bash
./scripts/run_crawler.sh [COMMAND] [OPTIONS]
```

**Commands**:
- `crawl` - Start crawling with specified URL
- `test` - Run tests (unit, integration, performance)
- `migrate` - Run database migrations
- `report` - Generate reports
- `monitor` - Start monitoring dashboard
- `profile` - Run performance profiling
- `clean` - Clean up temporary files
- `help` - Show help message

**Examples**:
```bash
# Start crawling
./scripts/run_crawler.sh crawl -u https://example.com -d 2 -w 3

# Run unit tests with coverage
./scripts/run_crawler.sh test --unit --coverage

# Generate HTML reports
./scripts/run_crawler.sh report --format html --output ./reports

# Clean up temporary files
./scripts/run_crawler.sh clean
```

### 🚢 deploy.sh
**Purpose**: Deployment to different environments

**Usage**:
```bash
./scripts/deploy.sh [OPTIONS]
```

**Options**:
- `-e, --env ENV` - Target environment (development, staging, production)
- `-v, --version VERSION` - Version tag for deployment
- `--docker` - Build Docker image
- `--push` - Push Docker image to registry
- `--registry REGISTRY` - Docker registry URL
- `--no-tests` - Skip running tests

**Examples**:
```bash
# Deploy to development
./scripts/deploy.sh --env development

# Deploy to production with Docker
./scripts/deploy.sh --env production --version v1.0.0 --docker --push --registry myregistry.com

# Deploy to staging
./scripts/deploy.sh --env staging --docker
```

## Quick Start

1. **Initial Setup**:
   ```bash
   ./scripts/setup.sh
   ```

2. **Activate Environment**:
   ```bash
   source venv/bin/activate
   ```

3. **Run Tests**:
   ```bash
   ./scripts/run_crawler.sh test --unit
   ```

4. **Start Crawling**:
   ```bash
   ./scripts/run_crawler.sh crawl -u https://example.com
   ```

## Environment Variables

The scripts use the following environment variables:

- `DB_PASSWORD` - Database password (set automatically by setup.sh)
- `CRAWLER_ENV` - Environment name (development, staging, production)
- `DOCKER_REGISTRY` - Docker registry URL for image pushing

## Directory Structure Created

The scripts will create the following directories:

```
├── venv/           # Python virtual environment
├── logs/           # Application logs
├── data/           # Crawled data storage
├── reports/        # Generated reports
├── profiles/       # Performance profiles
└── config/         # Environment-specific configurations
```

## Troubleshooting

### Common Issues

1. **Permission Denied**:
   ```bash
   chmod +x scripts/*.sh
   ```

2. **Python Version Issues**:
   - Ensure Python 3.9+ is installed
   - Check with: `python3 --version`

3. **PostgreSQL Not Running**:
   ```bash
   # Ubuntu/Debian
   sudo systemctl start postgresql
   
   # macOS
   brew services start postgresql
   ```

4. **Virtual Environment Issues**:
   ```bash
   # Remove and recreate
   rm -rf venv
   ./scripts/setup.sh
   ```

5. **Database Connection Issues**:
   - Check PostgreSQL is running
   - Verify DB_PASSWORD environment variable
   - Check database exists: `psql -l`

### Script Dependencies

- **setup.sh**: Requires Python 3.9+, optionally PostgreSQL
- **run_crawler.sh**: Requires virtual environment (created by setup.sh)
- **deploy.sh**: Requires git (for version generation), optionally Docker

### Logs and Debugging

- Application logs: `logs/crawler.log`
- Test coverage reports: `htmlcov/index.html`
- Performance profiles: `profiles/*.prof`

## Development Workflow

1. **Setup Development Environment**:
   ```bash
   ./scripts/setup.sh
   source venv/bin/activate
   ```

2. **Run Tests During Development**:
   ```bash
   ./scripts/run_crawler.sh test --unit
   ```

3. **Test Crawling Locally**:
   ```bash
   ./scripts/run_crawler.sh crawl -u https://httpbin.org -d 1 -w 2
   ```

4. **Generate Reports**:
   ```bash
   ./scripts/run_crawler.sh report --format html
   ```

5. **Deploy to Staging**:
   ```bash
   ./scripts/deploy.sh --env staging --docker
   ```

## Production Deployment

For production deployment:

1. **Prepare Production Config**:
   - Create `config/production.yaml`
   - Set production database credentials
   - Configure monitoring settings

2. **Deploy with Version Tag**:
   ```bash
   ./scripts/deploy.sh --env production --version v1.0.0 --docker --push
   ```

3. **Install Systemd Service**:
   ```bash
   sudo cp webcrawler-production.service /etc/systemd/system/
   sudo systemctl enable webcrawler-production
   sudo systemctl start webcrawler-production
   ```

4. **Monitor Service**:
   ```bash
   sudo systemctl status webcrawler-production
   journalctl -u webcrawler-production -f
   ```

## Security Considerations

- Store sensitive credentials in environment variables
- Use secure database connections in production
- Regularly update dependencies
- Monitor logs for security issues
- Use HTTPS for all external communications

## Performance Tips

- Use appropriate number of workers based on system resources
- Monitor memory usage during large crawls
- Use database connection pooling
- Enable caching for frequently accessed data
- Profile performance regularly using the profile command