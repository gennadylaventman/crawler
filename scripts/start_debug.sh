#!/bin/bash

# Crawler Debug Startup Script
echo "🐛 Starting Crawler Debug Environment"
echo "======================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please create one first:"
    echo "   python -m venv venv"
    exit 1
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Load environment variables
if [ -f ".env" ]; then
    echo "📝 Loading environment variables from .env..."
    export $(cat .env | xargs)
else
    echo "⚠️  .env file not found, using defaults..."
    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_NAME=webcrawler
    export DB_USER=crawler
    export DB_PASSWORD=crawler_password
    export REDIS_HOST=localhost
    export REDIS_PORT=6379
    export REDIS_PASSWORD=redis_password
    export CRAWLER_ENV=development
    export LOG_LEVEL=DEBUG
fi

# Check Docker containers
echo "🐳 Checking Docker containers..."
if ! docker ps | grep -q "webcrawler-postgres"; then
    echo "❌ PostgreSQL container not running. Starting it..."
    docker run -d --name webcrawler-postgres \
        -e POSTGRES_DB=webcrawler \
        -e POSTGRES_USER=crawler \
        -e POSTGRES_PASSWORD=crawler_password \
        -p 5432:5432 \
        -v $(pwd)/database_schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro \
        postgres:15-alpine
    echo "⏳ Waiting for PostgreSQL to start..."
    sleep 10
fi

if ! docker ps | grep -q "webcrawler-redis"; then
    echo "❌ Redis container not running. Starting it..."
    docker run -d --name webcrawler-redis \
        -p 6379:6379 \
        redis:7-alpine redis-server --appendonly yes --requirepass redis_password
    echo "⏳ Waiting for Redis to start..."
    sleep 5
fi

# Test connections
echo "🔍 Testing database connections..."
python -c "
import psycopg2
import redis
import sys

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='webcrawler',
        user='crawler',
        password='crawler_password'
    )
    conn.close()
    print('✅ PostgreSQL connection successful')
except Exception as e:
    print(f'❌ PostgreSQL connection failed: {e}')
    sys.exit(1)

try:
    r = redis.Redis(host='localhost', port=6379, password='redis_password')
    r.ping()
    print('✅ Redis connection successful')
except Exception as e:
    print(f'❌ Redis connection failed: {e}')
    sys.exit(1)
"

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Debug environment ready!"
    echo ""
    echo "Database connections:"
    echo "  🐘 PostgreSQL: localhost:5432 (database: webcrawler)"
    echo "  🔴 Redis: localhost:6379"
    echo ""
    echo "To debug your crawler:"
    echo "  1. Set breakpoints in your IDE"
    echo "  2. Run: python src/crawler/cli.py --config config/development.yaml"
    echo "  3. Or use your IDE's debug configuration"
    echo ""
    echo "To run tests:"
    echo "  python -m pytest tests/unit/ -v"
    echo ""
    echo "To stop containers when done:"
    echo "  docker stop webcrawler-postgres webcrawler-redis"
    echo ""
else
    echo "❌ Setup failed. Check the error messages above."
    exit 1
fi