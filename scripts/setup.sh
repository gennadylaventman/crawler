#!/bin/bash

# Web Crawler Setup Script
# This script sets up the development environment for the web crawler

set -e  # Exit on any error

echo "🚀 Setting up Web Crawler development environment..."

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

# Check if Python 3.9+ is installed
check_python() {
    print_status "Checking Python version..."
    
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
        PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d'.' -f1)
        PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d'.' -f2)
        
        if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 9 ]; then
            print_success "Python $PYTHON_VERSION found"
            PYTHON_CMD="python3"
        else
            print_error "Python 3.9+ required, found $PYTHON_VERSION"
            exit 1
        fi
    else
        print_error "Python 3 not found. Please install Python 3.9+"
        exit 1
    fi
}

# Check if PostgreSQL is installed
check_postgresql() {
    print_status "Checking PostgreSQL..."
    
    if command -v psql &> /dev/null; then
        PG_VERSION=$(psql --version | head -n1 | awk '{print $3}')
        print_success "PostgreSQL $PG_VERSION found"
    else
        print_warning "PostgreSQL not found. Please install PostgreSQL 12+"
        print_status "On Ubuntu/Debian: sudo apt-get install postgresql postgresql-contrib"
        print_status "On macOS: brew install postgresql"
        print_status "On CentOS/RHEL: sudo yum install postgresql-server postgresql-contrib"
    fi
}

# Create virtual environment
create_venv() {
    print_status "Creating virtual environment..."
    
    if [ ! -d "venv" ]; then
        $PYTHON_CMD -m venv venv
        print_success "Virtual environment created"
    else
        print_warning "Virtual environment already exists"
    fi
}

# Activate virtual environment and install dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        print_success "Dependencies installed from requirements.txt"
    else
        print_warning "requirements.txt not found, installing basic dependencies..."
        pip install aiohttp beautifulsoup4 asyncpg pydantic pyyaml psutil nltk langdetect
    fi
    
    # Install development dependencies
    pip install pytest pytest-asyncio pytest-cov black flake8 mypy
    print_success "Development dependencies installed"
}

# Setup database
setup_database() {
    print_status "Setting up database..."
    
    # Check if PostgreSQL is running
    if ! pgrep -x "postgres" > /dev/null; then
        print_warning "PostgreSQL is not running. Please start PostgreSQL service."
        print_status "On Ubuntu/Debian: sudo systemctl start postgresql"
        print_status "On macOS: brew services start postgresql"
        return
    fi
    
    # Create database and user (if they don't exist)
    print_status "Creating database and user..."
    
    # Try to create database
    createdb webcrawler 2>/dev/null || print_warning "Database 'webcrawler' may already exist"
    
    # Try to create user
    createuser crawler 2>/dev/null || print_warning "User 'crawler' may already exist"
    
    print_success "Database setup completed"
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p logs
    mkdir -p data
    mkdir -p reports
    mkdir -p profiles
    
    print_success "Directories created"
}

# Setup configuration files
setup_config() {
    print_status "Setting up configuration files..."
    
    # Create logs directory
    mkdir -p logs
    
    # Set default database password if not set
    if [ -z "$DB_PASSWORD" ]; then
        export DB_PASSWORD="crawler_password"
        echo "export DB_PASSWORD=crawler_password" >> ~/.bashrc
        print_warning "DB_PASSWORD not set, using default: crawler_password"
        print_status "Add 'export DB_PASSWORD=your_password' to your ~/.bashrc"
    fi
    
    print_success "Configuration setup completed"
}

# Download NLTK data
setup_nltk() {
    print_status "Setting up NLTK data..."
    
    source venv/bin/activate
    python3 -c "
import nltk
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    print('NLTK data downloaded successfully')
except Exception as e:
    print(f'Warning: Failed to download NLTK data: {e}')
"
    
    print_success "NLTK setup completed"
}

# Run tests to verify setup
run_tests() {
    print_status "Running tests to verify setup..."
    
    source venv/bin/activate
    
    # Run unit tests only (skip integration tests that require database)
    if python3 -m pytest tests/unit/ -v --tb=short; then
        print_success "Unit tests passed"
    else
        print_warning "Some tests failed, but setup is complete"
    fi
}

# Main setup process
main() {
    echo "🕷️  Web Crawler Setup Script"
    echo "================================"
    
    check_python
    check_postgresql
    create_venv
    install_dependencies
    create_directories
    setup_config
    setup_nltk
    setup_database
    
    echo ""
    echo "🎉 Setup completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Activate the virtual environment: source venv/bin/activate"
    echo "2. Set your database password: export DB_PASSWORD=your_password"
    echo "3. Run database migrations: python -m crawler.cli migrate"
    echo "4. Start crawling: python -m crawler.cli crawl -u https://example.com"
    echo ""
    echo "For more information, see README.md"
}

# Run main function
main "$@"