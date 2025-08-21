#!/bin/bash

# Visual Tests Script for dataframe-ui-x
# This script manages Playwright visual tests using Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if services are running
check_services() {
    log_info "Checking if required services are running..."
    
    if ! curl -s http://localhost:5001 > /dev/null 2>&1; then
        log_error "dataframe-ui-x service is not running on port 5001"
        log_info "Please start services with: make up"
        return 1
    fi
    
    if ! curl -s http://localhost:4999/api/dataframes > /dev/null 2>&1; then
        log_warning "dataframe-api service may not be running on port 4999"
    fi
    
    log_success "Services are accessible"
    return 0
}

# Function to setup playwright environment
setup_playwright() {
    log_info "Setting up Playwright test environment..."
    
    cd "$PROJECT_ROOT/playwright-tests"
    
    # Start playwright container if not running
    if ! docker ps | grep -q playwright-tests; then
        log_info "Starting Playwright container..."
        cd "$PROJECT_ROOT"
        docker compose --profile testing up -d playwright
        sleep 5
    fi
    
    # Install dependencies
    log_info "Installing Playwright dependencies..."
    docker compose exec playwright npm install
    
    # Install browsers
    log_info "Installing Playwright browsers..."
    docker compose exec playwright npx playwright install
    
    log_success "Playwright environment ready"
}

# Function to run visual tests
run_tests() {
    local test_args="$1"
    
    log_info "Running Playwright visual tests..."
    
    cd "$PROJECT_ROOT"
    
    # Run tests in container
    if [ -z "$test_args" ]; then
        docker compose exec playwright npm test
    else
        docker compose exec playwright npx playwright test $test_args
    fi
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_success "All tests passed!"
    else
        log_error "Some tests failed (exit code: $exit_code)"
    fi
    
    return $exit_code
}

# Function to generate test report
generate_report() {
    log_info "Generating test report..."
    
    cd "$PROJECT_ROOT"
    docker compose exec playwright npm run test:report
    
    log_success "Test report generated at playwright-report/index.html"
}

# Function to clean up
cleanup() {
    log_info "Cleaning up test environment..."
    
    cd "$PROJECT_ROOT"
    
    # Stop playwright container
    docker compose --profile testing down
    
    log_success "Cleanup complete"
}

# Function to show help
show_help() {
    cat << EOF
Visual Tests Script for dataframe-ui-x

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    setup           Setup Playwright test environment
    test [args]     Run visual tests (optional: pass playwright test arguments)
    test-headed     Run tests in headed mode (visible browser)
    test-debug      Run tests in debug mode
    report          Generate and show test report
    cleanup         Stop test containers and cleanup
    help            Show this help message

Examples:
    $0 setup                           # Setup test environment
    $0 test                           # Run all tests
    $0 test tests/auth.spec.js        # Run specific test file
    $0 test --grep "login"            # Run tests matching pattern
    $0 test-headed                    # Run tests with visible browser
    $0 test-debug                     # Run tests in debug mode
    $0 report                         # Generate test report
    $0 cleanup                        # Clean up

Prerequisites:
    - Docker and docker-compose installed
    - Services running (make up)
    - Playwright profile enabled in docker-compose.yml

EOF
}

# Main script logic
main() {
    case "${1:-help}" in
        setup)
            check_services && setup_playwright
            ;;
        test)
            shift
            check_services && run_tests "$*"
            ;;
        test-headed)
            check_services && run_tests "--headed"
            ;;
        test-debug)
            check_services && run_tests "--debug"
            ;;
        report)
            generate_report
            ;;
        cleanup)
            cleanup
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"