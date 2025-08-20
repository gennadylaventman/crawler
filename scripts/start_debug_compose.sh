#!/bin/bash

# Crawler Debug Startup Script with Docker Compose
echo "🐛 Starting Crawler Debug Environment with Docker Compose"
echo "========================================================="

# Stop any existing services first
echo "🧹 Stopping any existing services..."
docker-compose -f docker-compose.debug.yml down 2>/dev/null || true

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
    set -a && source .env && set +a
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

# Start Docker Compose services
echo "🐳 Starting Docker Compose services..."
docker-compose -f docker-compose.debug.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check service health
echo "🔍 Checking service health..."
if docker-compose -f docker-compose.debug.yml ps | grep -q "healthy"; then
    echo "✅ Services are healthy"
else
    echo "⚠️  Services may still be starting up..."
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
    echo "Docker Compose services:"
    echo "  🐘 PostgreSQL: localhost:5432 (database: webcrawler)"
    echo "  🔴 Redis: localhost:6379"
    echo ""
    echo "To debug your crawler:"
    echo "  1. Set breakpoints in your IDE"
    echo "  2. Run: python src/crawler/cli.py crawl -u https://httpbin.org/html -d 1 -p 5"
    echo "  3. Or use your IDE's debug configuration"
    echo ""
    echo "To run tests:"
    echo "  python -m pytest tests/unit/ -v"
    echo ""
    echo "To view logs:"
    echo "  docker-compose -f docker-compose.debug.yml logs -f"
    echo ""
    echo "To stop services when done:"
    echo "  docker-compose -f docker-compose.debug.yml down"
    echo ""
else
    echo "❌ Setup failed. Check the error messages above."
    exit 1
fi