#!/bin/bash

# Web Crawler Run Script
# This script provides convenient commands to run the web crawler

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if virtual environment exists and activate it
activate_venv() {
    if [ -d "venv" ]; then
        source venv/bin/activate
        print_status "Virtual environment activated"
    else
        print_error "Virtual environment not found. Run setup.sh first."
        exit 1
    fi
}

# Check if database is running
check_database() {
    if ! pgrep -x "postgres" > /dev/null; then
        print_error "PostgreSQL is not running. Please start PostgreSQL service."
        print_status "On Ubuntu/Debian: sudo systemctl start postgresql"
        print_status "On macOS: brew services start postgresql"
        exit 1
    fi
}

# Show usage information
show_usage() {
    echo "🕷️  Web Crawler Run Script"
    echo "=========================="
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  crawl       Start crawling with specified URL"
    echo "  test        Run tests"
    echo "  migrate     Run database migrations"
    echo "  report      Generate reports"
    echo "  monitor     Start monitoring dashboard"
    echo "  profile     Run performance profiling"
    echo "  clean       Clean up temporary files"
    echo "  help        Show this help message"
    echo ""
    echo "Crawl Options:"
    echo "  -u, --url URL           Starting URL to crawl"
    echo "  -d, --depth DEPTH       Maximum crawl depth (default: 3)"
    echo "  -w, --workers WORKERS   Number of worker threads (default: 5)"
    echo "  -c, --config CONFIG     Configuration file path"
    echo "  --max-pages PAGES       Maximum pages to crawl (default: 1000)"
    echo "  --delay SECONDS         Delay between requests (default: 1)"
    echo "  --output DIR            Output directory for results"
    echo ""
    echo "Test Options:"
    echo "  --unit                  Run unit tests only"
    echo "  --integration          Run integration tests only"
    echo "  --performance          Run performance tests only"
    echo "  --coverage             Run tests with coverage report"
    echo ""
    echo "Examples:"
    echo "  $0 crawl -u https://example.com -d 2 -w 3"
    echo "  $0 test --unit --coverage"
    echo "  $0 report --format html --output ./reports"
    echo "  $0 profile --url https://example.com --duration 60"
}

# Run crawler with specified options
run_crawl() {
    local url=""
    local depth=3
    local workers=5
    local config=""
    local max_pages=1000
    local delay=1
    local output="./data"
    
    # Parse crawl options
    while [[ $# -gt 0 ]]; do
        case $1 in
            -u|--url)
                url="$2"
                shift 2
                ;;
            -d|--depth)
                depth="$2"
                shift 2
                ;;
            -w|--workers)
                workers="$2"
                shift 2
                ;;
            -c|--config)
                config="$2"
                shift 2
                ;;
            --max-pages)
                max_pages="$2"
                shift 2
                ;;
            --delay)
                delay="$2"
                shift 2
                ;;
            --output)
                output="$2"
                shift 2
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    if [ -z "$url" ]; then
        print_error "URL is required for crawling"
        echo "Usage: $0 crawl -u <URL> [OPTIONS]"
        exit 1
    fi
    
    print_status "Starting crawler with URL: $url"
    print_status "Depth: $depth, Workers: $workers, Max pages: $max_pages"
    
    # Build command
    local cmd="python -m crawler.cli crawl --url '$url' --depth $depth --workers $workers --max-pages $max_pages --delay $delay --output '$output'"
    
    if [ -n "$config" ]; then
        cmd="$cmd --config '$config'"
    fi
    
    print_status "Running: $cmd"
    eval $cmd
}

# Run tests with specified options
run_tests() {
    local test_type="all"
    local coverage=false
    
    # Parse test options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --unit)
                test_type="unit"
                shift
                ;;
            --integration)
                test_type="integration"
                shift
                ;;
            --performance)
                test_type="performance"
                shift
                ;;
            --coverage)
                coverage=true
                shift
                ;;
            *)
                print_error "Unknown test option: $1"
                exit 1
                ;;
        esac
    done
    
    print_status "Running $test_type tests..."
    
    local cmd="python -m pytest"
    
    case $test_type in
        unit)
            cmd="$cmd tests/unit/"
            ;;
        integration)
            cmd="$cmd tests/integration/"
            ;;
        performance)
            cmd="$cmd tests/performance/"
            ;;
        all)
            cmd="$cmd tests/"
            ;;
    esac
    
    if [ "$coverage" = true ]; then
        cmd="$cmd --cov=src/crawler --cov-report=html --cov-report=term"
        print_status "Coverage report will be generated in htmlcov/"
    fi
    
    cmd="$cmd -v"
    
    print_status "Running: $cmd"
    eval $cmd
}

# Run database migrations
run_migrate() {
    print_status "Running database migrations..."
    python -m crawler.cli migrate
    print_success "Database migrations completed"
}

# Generate reports
run_report() {
    local format="html"
    local output="./reports"
    
    # Parse report options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --format)
                format="$2"
                shift 2
                ;;
            --output)
                output="$2"
                shift 2
                ;;
            *)
                print_error "Unknown report option: $1"
                exit 1
                ;;
        esac
    done
    
    print_status "Generating $format reports in $output..."
    python -m crawler.cli report --format "$format" --output "$output"
    print_success "Reports generated successfully"
}

# Start monitoring dashboard
run_monitor() {
    print_status "Starting monitoring dashboard..."
    python -m crawler.cli monitor
}

# Run performance profiling
run_profile() {
    local url=""
    local duration=60
    local output="./profiles"
    
    # Parse profile options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --url)
                url="$2"
                shift 2
                ;;
            --duration)
                duration="$2"
                shift 2
                ;;
            --output)
                output="$2"
                shift 2
                ;;
            *)
                print_error "Unknown profile option: $1"
                exit 1
                ;;
        esac
    done
    
    if [ -z "$url" ]; then
        print_error "URL is required for profiling"
        exit 1
    fi
    
    print_status "Running performance profiling for $duration seconds..."
    python -m crawler.cli profile --url "$url" --duration "$duration" --output "$output"
    print_success "Profiling completed. Results saved to $output"
}

# Clean up temporary files
run_clean() {
    print_status "Cleaning up temporary files..."
    
    # Remove Python cache files
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -name "*.pyc" -delete 2>/dev/null || true
    find . -name "*.pyo" -delete 2>/dev/null || true
    
    # Remove test artifacts
    rm -rf .pytest_cache/ 2>/dev/null || true
    rm -rf htmlcov/ 2>/dev/null || true
    rm -f .coverage 2>/dev/null || true
    
    # Remove temporary logs (keep recent ones)
    find logs/ -name "*.log" -mtime +7 -delete 2>/dev/null || true
    
    # Remove old profile files
    find profiles/ -name "*.prof" -mtime +7 -delete 2>/dev/null || true
    
    print_success "Cleanup completed"
}

# Main script logic
main() {
    if [ $# -eq 0 ]; then
        show_usage
        exit 0
    fi
    
    local command="$1"
    shift
    
    case $command in
        crawl)
            activate_venv
            check_database
            run_crawl "$@"
            ;;
        test)
            activate_venv
            run_tests "$@"
            ;;
        migrate)
            activate_venv
            check_database
            run_migrate "$@"
            ;;
        report)
            activate_venv
            run_report "$@"
            ;;
        monitor)
            activate_venv
            check_database
            run_monitor "$@"
            ;;
        profile)
            activate_venv
            run_profile "$@"
            ;;
        clean)
            run_clean "$@"
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            print_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"