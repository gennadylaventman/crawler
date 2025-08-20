#!/bin/bash

# Web Crawler Deployment Script
# This script handles deployment to different environments

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

# Default values
ENVIRONMENT="development"
BUILD_DOCKER=false
PUSH_DOCKER=false
RUN_TESTS=true
DOCKER_REGISTRY=""
VERSION=""

# Show usage information
show_usage() {
    echo "🚀 Web Crawler Deployment Script"
    echo "================================"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --env ENV           Target environment (development, staging, production)"
    echo "  -v, --version VERSION   Version tag for deployment"
    echo "  --docker               Build Docker image"
    echo "  --push                 Push Docker image to registry"
    echo "  --registry REGISTRY    Docker registry URL"
    echo "  --no-tests             Skip running tests"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --env production --version v1.0.0 --docker --push"
    echo "  $0 --env staging --docker"
    echo "  $0 --env development"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            --docker)
                BUILD_DOCKER=true
                shift
                ;;
            --push)
                PUSH_DOCKER=true
                shift
                ;;
            --registry)
                DOCKER_REGISTRY="$2"
                shift 2
                ;;
            --no-tests)
                RUN_TESTS=false
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Validate environment
validate_environment() {
    case $ENVIRONMENT in
        development|staging|production)
            print_status "Deploying to $ENVIRONMENT environment"
            ;;
        *)
            print_error "Invalid environment: $ENVIRONMENT"
            print_error "Valid environments: development, staging, production"
            exit 1
            ;;
    esac
}

# Generate version if not provided
generate_version() {
    if [ -z "$VERSION" ]; then
        if command -v git &> /dev/null && git rev-parse --git-dir > /dev/null 2>&1; then
            VERSION=$(git describe --tags --always --dirty)
            print_status "Generated version from git: $VERSION"
        else
            VERSION=$(date +"%Y%m%d-%H%M%S")
            print_status "Generated version from timestamp: $VERSION"
        fi
    fi
}

# Run tests before deployment
run_tests() {
    if [ "$RUN_TESTS" = true ]; then
        print_status "Running tests before deployment..."
        
        if [ -f "scripts/run_crawler.sh" ]; then
            bash scripts/run_crawler.sh test --unit
        else
            print_warning "Test script not found, skipping tests"
        fi
        
        print_success "Tests completed"
    else
        print_warning "Skipping tests as requested"
    fi
}

# Build Docker image
build_docker_image() {
    if [ "$BUILD_DOCKER" = true ]; then
        print_status "Building Docker image..."
        
        local image_name="webcrawler"
        local full_tag="${image_name}:${VERSION}"
        
        if [ -n "$DOCKER_REGISTRY" ]; then
            full_tag="${DOCKER_REGISTRY}/${full_tag}"
        fi
        
        if [ -f "Dockerfile" ]; then
            docker build -t "$full_tag" .
            docker tag "$full_tag" "${image_name}:latest"
            
            if [ -n "$DOCKER_REGISTRY" ]; then
                docker tag "$full_tag" "${DOCKER_REGISTRY}/${image_name}:latest"
            fi
            
            print_success "Docker image built: $full_tag"
        else
            print_error "Dockerfile not found"
            exit 1
        fi
    fi
}

# Push Docker image to registry
push_docker_image() {
    if [ "$PUSH_DOCKER" = true ]; then
        if [ "$BUILD_DOCKER" = false ]; then
            print_error "Cannot push without building. Use --docker flag."
            exit 1
        fi
        
        if [ -z "$DOCKER_REGISTRY" ]; then
            print_error "Docker registry not specified. Use --registry flag."
            exit 1
        fi
        
        print_status "Pushing Docker image to registry..."
        
        local image_name="webcrawler"
        local full_tag="${DOCKER_REGISTRY}/${image_name}:${VERSION}"
        local latest_tag="${DOCKER_REGISTRY}/${image_name}:latest"
        
        docker push "$full_tag"
        docker push "$latest_tag"
        
        print_success "Docker image pushed to registry"
    fi
}

# Deploy to development environment
deploy_development() {
    print_status "Deploying to development environment..."
    
    # Create development configuration if it doesn't exist
    if [ ! -f "config/development.yaml" ]; then
        print_warning "Development config not found, creating default..."
        mkdir -p config
        cat > config/development.yaml << EOF
database:
  host: localhost
  port: 5432
  name: webcrawler_dev
  user: crawler
  password: \${DB_PASSWORD}

crawler:
  max_workers: 3
  max_depth: 2
  delay_between_requests: 1.0
  max_pages_per_domain: 100

logging:
  level: DEBUG
  file: logs/crawler_dev.log

monitoring:
  enabled: true
  metrics_port: 8001
EOF
        print_success "Development config created"
    fi
    
    # Ensure development database exists
    createdb webcrawler_dev 2>/dev/null || print_warning "Development database may already exist"
    
    print_success "Development deployment completed"
}

# Deploy to staging environment
deploy_staging() {
    print_status "Deploying to staging environment..."
    
    # Check if staging config exists
    if [ ! -f "config/staging.yaml" ]; then
        print_error "Staging configuration not found: config/staging.yaml"
        exit 1
    fi
    
    # Create staging database if needed
    createdb webcrawler_staging 2>/dev/null || print_warning "Staging database may already exist"
    
    # Run database migrations
    export CRAWLER_ENV=staging
    python -m crawler.cli migrate
    
    print_success "Staging deployment completed"
}

# Deploy to production environment
deploy_production() {
    print_status "Deploying to production environment..."
    
    # Extra validation for production
    if [ -z "$VERSION" ]; then
        print_error "Version is required for production deployment"
        exit 1
    fi
    
    # Check if production config exists
    if [ ! -f "config/production.yaml" ]; then
        print_error "Production configuration not found: config/production.yaml"
        exit 1
    fi
    
    # Confirm production deployment
    echo -n "Are you sure you want to deploy to PRODUCTION? (yes/no): "
    read -r confirmation
    if [ "$confirmation" != "yes" ]; then
        print_error "Production deployment cancelled"
        exit 1
    fi
    
    # Run database migrations
    export CRAWLER_ENV=production
    python -m crawler.cli migrate
    
    # Create deployment record
    echo "$(date): Deployed version $VERSION to production" >> deployment.log
    
    print_success "Production deployment completed"
}

# Create systemd service file
create_systemd_service() {
    if [ "$ENVIRONMENT" = "production" ] || [ "$ENVIRONMENT" = "staging" ]; then
        print_status "Creating systemd service file..."
        
        local service_name="webcrawler-${ENVIRONMENT}"
        local working_dir=$(pwd)
        local user=$(whoami)
        
        cat > "${service_name}.service" << EOF
[Unit]
Description=Web Crawler - ${ENVIRONMENT}
After=network.target postgresql.service

[Service]
Type=simple
User=${user}
WorkingDirectory=${working_dir}
Environment=CRAWLER_ENV=${ENVIRONMENT}
ExecStart=${working_dir}/venv/bin/python -m crawler.cli run
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
        
        print_success "Systemd service file created: ${service_name}.service"
        print_status "To install: sudo cp ${service_name}.service /etc/systemd/system/"
        print_status "To enable: sudo systemctl enable ${service_name}"
        print_status "To start: sudo systemctl start ${service_name}"
    fi
}

# Main deployment function
deploy() {
    case $ENVIRONMENT in
        development)
            deploy_development
            ;;
        staging)
            deploy_staging
            ;;
        production)
            deploy_production
            ;;
    esac
    
    create_systemd_service
}

# Cleanup function
cleanup() {
    print_status "Cleaning up temporary files..."
    # Add any cleanup logic here
    print_success "Cleanup completed"
}

# Main script execution
main() {
    echo "🚀 Web Crawler Deployment"
    echo "========================"
    
    parse_args "$@"
    validate_environment
    generate_version
    
    print_status "Environment: $ENVIRONMENT"
    print_status "Version: $VERSION"
    
    run_tests
    build_docker_image
    push_docker_image
    deploy
    cleanup
    
    echo ""
    print_success "🎉 Deployment completed successfully!"
    echo ""
    print_status "Environment: $ENVIRONMENT"
    print_status "Version: $VERSION"
    
    if [ "$BUILD_DOCKER" = true ]; then
        print_status "Docker image: webcrawler:$VERSION"
    fi
    
    echo ""
    print_status "Next steps:"
    case $ENVIRONMENT in
        development)
            echo "  1. Activate virtual environment: source venv/bin/activate"
            echo "  2. Start crawler: python -m crawler.cli run"
            ;;
        staging|production)
            echo "  1. Install systemd service if needed"
            echo "  2. Start service: sudo systemctl start webcrawler-${ENVIRONMENT}"
            echo "  3. Monitor logs: journalctl -u webcrawler-${ENVIRONMENT} -f"
            ;;
    esac
}

# Run main function
main "$@"