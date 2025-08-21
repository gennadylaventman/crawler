#!/bin/bash

# Web Crawler with Automatic Report Generation
# Usage: ./run_crawl_and_report.sh [URL] [DEPTH] [WORKERS] [PAGES] [REPORT_FORMAT] [OUTPUT_FILE]

# Ensure DB running and setting config
source venv/bin/activate

docker-compose -f docker-compose.debug.yml down

docker-compose -f docker-compose.debug.yml up -d postgres

docker-compose -f docker-compose.debug.yml ps

export DB_PASSWORD=crawler_password
export CRAWLER_ENABLE_PERSISTENT_QUEUE=True

# Default values
URL=${1:-"https://www.example.com"}
DEPTH=${2:-2}
WORKERS=${3:-5}
PAGES=${4:-1000}
REPORT_FORMAT=${5:-"html"}
OUTPUT_FILE=${6:-"report.html"}

echo "🚀 Starting web crawl..."
echo "URL: $URL"
echo "Depth: $DEPTH"
echo "Workers: $WORKERS"
echo "Max Pages: $PAGES"
echo "Report Format: $REPORT_FORMAT"
echo "Output File: $OUTPUT_FILE"
echo "----------------------------------------"

# Run the crawler and capture output
CRAWL_OUTPUT=$(python src/crawler/cli.py crawl --url "$URL" --depth "$DEPTH" --workers "$WORKERS" --pages "$PAGES" 2>&1)

# Display the crawl output
echo "$CRAWL_OUTPUT"

# Extract session ID using multiple methods (fallback approach)
SESSION_ID=""

# Method 1: Using awk
if [ -z "$SESSION_ID" ]; then
    SESSION_ID=$(echo "$CRAWL_OUTPUT" | grep "Session ID:" | awk '{print $NF}')
fi

# Method 2: Using sed as fallback
if [ -z "$SESSION_ID" ]; then
    SESSION_ID=$(echo "$CRAWL_OUTPUT" | sed -n 's/.*Session ID: \([a-f0-9]*\).*/\1/p')
fi

# Method 3: Using grep with Perl regex as second fallback
if [ -z "$SESSION_ID" ] && command -v grep >/dev/null 2>&1; then
    SESSION_ID=$(echo "$CRAWL_OUTPUT" | grep -oP 'Session ID: \K[a-f0-9]+' 2>/dev/null || true)
fi

# Check if session ID was extracted successfully
if [ -z "$SESSION_ID" ]; then
    echo "❌ Error: Could not extract session ID from crawler output"
    echo "Please check the crawler output above for errors"
    exit 1
fi

echo "----------------------------------------"
echo "📊 Generating report..."
echo "Session ID: $SESSION_ID"
echo "Format: $REPORT_FORMAT"
echo "Output: $OUTPUT_FILE"

# Generate the report
if python src/crawler/cli.py report -s "$SESSION_ID" -f "$REPORT_FORMAT" -o "$OUTPUT_FILE"; then
    echo "✅ Report generated successfully: $OUTPUT_FILE"
    
    # Show file info
    if [ -f "$OUTPUT_FILE" ]; then
        FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
        echo "📄 Report file size: $FILE_SIZE"
    fi
else
    echo "❌ Error: Failed to generate report"
    exit 1
fi

echo "🎉 Process completed successfully!"