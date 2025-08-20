# Configuration Directory

This directory contains environment-specific configuration files for the web crawler application.

## Configuration Files

### 📄 default.yaml
**Purpose**: Base configuration that other environments inherit from

**Usage**: Automatically loaded as the base configuration. Other environment configs override these settings.

**Contains**:
- Default database settings
- Basic crawler parameters
- Standard logging configuration
- Default monitoring settings
- Base security headers

### 🔧 development.yaml
**Purpose**: Development environment configuration

**Optimized for**:
- Local development and debugging
- Verbose logging (DEBUG level)
- Lower resource limits
- Development-friendly settings

**Key Features**:
- Small worker pool (3 workers)
- Low crawl depth (2 levels)
- Detailed logging and debugging
- Local database connection
- Hot reloading support
- Test data integration

### 🧪 staging.yaml
**Purpose**: Staging/testing environment configuration

**Optimized for**:
- Integration testing
- Performance testing
- Production-like environment with relaxed limits

**Key Features**:
- Moderate worker pool (5 workers)
- Medium crawl depth (3 levels)
- Structured JSON logging
- Load testing capabilities
- Integration with external services

### 🚀 production.yaml
**Purpose**: Production environment configuration

**Optimized for**:
- High performance and reliability
- Security and monitoring
- Scalability and resource efficiency

**Key Features**:
- Large worker pool (10 workers)
- Deep crawl capability (5 levels)
- Comprehensive monitoring and alerting
- SSL/TLS security
- Backup and retention policies
- Integration with monitoring services (Datadog, New Relic, Sentry)

## Environment Variables

The configuration files support environment variable substitution using the `${VAR_NAME:default_value}` syntax.

### Required Environment Variables

| Variable | Description | Default | Required In |
|----------|-------------|---------|-------------|
| `DB_PASSWORD` | Database password | - | All environments |
| `DB_HOST` | Database host | localhost | Staging, Production |
| `DB_PORT` | Database port | 5432 | Staging, Production |
| `DB_NAME` | Database name | varies | Staging, Production |
| `DB_USER` | Database user | crawler | Staging, Production |

### Optional Environment Variables

| Variable | Description | Default | Used In |
|----------|-------------|---------|---------|
| `SSL_CERT_PATH` | SSL certificate path | - | Production |
| `SSL_KEY_PATH` | SSL private key path | - | Production |
| `SSL_CA_PATH` | SSL CA certificate path | - | Production |
| `ALERT_WEBHOOK_URL` | Webhook URL for alerts | - | Production |
| `DATADOG_API_KEY` | Datadog API key | - | Production |
| `NEWRELIC_LICENSE_KEY` | New Relic license key | - | Production |
| `SENTRY_DSN` | Sentry DSN for error tracking | - | Production |
| `HTTP_PROXY` | HTTP proxy URL | - | Production |
| `HTTPS_PROXY` | HTTPS proxy URL | - | Production |

## Configuration Loading

The application loads configuration in the following order:

1. **default.yaml** - Base configuration
2. **{environment}.yaml** - Environment-specific overrides
3. **Environment variables** - Runtime overrides

### Environment Detection

The environment is determined by the `CRAWLER_ENV` environment variable:

```bash
export CRAWLER_ENV=development  # Loads development.yaml
export CRAWLER_ENV=staging      # Loads staging.yaml
export CRAWLER_ENV=production   # Loads production.yaml
```

If `CRAWLER_ENV` is not set, the application defaults to `development`.

## Configuration Sections

### Database Configuration
```yaml
database:
  host: localhost
  port: 5432
  name: webcrawler
  user: crawler
  password: ${DB_PASSWORD}
  pool_size: 5
  max_overflow: 10
```

### Crawler Configuration
```yaml
crawler:
  max_workers: 5
  max_depth: 3
  delay_between_requests: 1.0
  respect_robots_txt: true
  user_agent: "WebCrawler/1.0"
```

### Logging Configuration
```yaml
logging:
  level: INFO
  file:
    enabled: true
    path: "logs/crawler.log"
  console:
    enabled: true
```

### Monitoring Configuration
```yaml
monitoring:
  enabled: true
  metrics:
    enabled: true
    port: 8001
  health_check:
    enabled: true
    port: 8002
```

## Usage Examples

### Setting Environment Variables

**Development**:
```bash
export CRAWLER_ENV=development
export DB_PASSWORD=dev_password
```

**Production**:
```bash
export CRAWLER_ENV=production
export DB_PASSWORD=secure_prod_password
export DB_HOST=prod-db.company.com
export SENTRY_DSN=https://your-sentry-dsn@sentry.io/project
```

### Loading Configuration in Code

```python
from crawler.utils.config import load_config

# Load configuration for current environment
config = load_config()

# Load specific environment configuration
config = load_config(environment='production')

# Access configuration values
db_host = config.database.host
max_workers = config.crawler.max_workers
```

## Configuration Validation

All configuration files are validated using Pydantic models to ensure:

- Required fields are present
- Data types are correct
- Values are within acceptable ranges
- Dependencies are satisfied

### Validation Examples

```python
from crawler.utils.config import validate_config

# Validate current configuration
errors = validate_config()
if errors:
    print("Configuration errors:", errors)
```

## Best Practices

### 1. Environment-Specific Settings

- **Development**: Use verbose logging, small limits, local services
- **Staging**: Use production-like settings with relaxed limits
- **Production**: Use optimized settings, security features, monitoring

### 2. Security

- Never commit passwords or API keys to version control
- Use environment variables for sensitive data
- Enable SSL/TLS in production
- Use secure headers and proxy settings

### 3. Performance

- Adjust worker counts based on available resources
- Set appropriate timeouts and retry limits
- Enable compression and caching in production
- Monitor resource usage and adjust limits

### 4. Monitoring

- Enable comprehensive logging in production
- Set up alerts for critical metrics
- Use structured logging (JSON) for log aggregation
- Monitor database connections and performance

## Troubleshooting

### Common Issues

1. **Configuration Not Loading**:
   - Check `CRAWLER_ENV` environment variable
   - Verify configuration file exists
   - Check file permissions

2. **Database Connection Errors**:
   - Verify `DB_PASSWORD` is set
   - Check database host and port
   - Ensure database exists and user has permissions

3. **Validation Errors**:
   - Check configuration syntax (YAML format)
   - Verify required fields are present
   - Check data types and value ranges

4. **Environment Variable Substitution**:
   - Use `${VAR_NAME}` syntax for required variables
   - Use `${VAR_NAME:default}` for optional variables
   - Check environment variables are exported

### Debugging Configuration

```bash
# Check current environment
echo $CRAWLER_ENV

# Validate configuration
python -c "from crawler.utils.config import load_config; print(load_config())"

# Check environment variables
env | grep -E "(DB_|CRAWLER_)"
```

## Configuration Schema

For detailed information about all available configuration options, see the Pydantic models in:
- `src/crawler/utils/config.py`
- `src/crawler/storage/models.py`

## Migration Guide

When upgrading between versions, check for:
- New configuration options
- Deprecated settings
- Changed default values
- New environment variables

The application will log warnings for deprecated configuration options and provide migration guidance.