#!/bin/bash
set -euo pipefail

# Docker-Only Test Runner for DataFrame Processing System
# All tests run in isolated Docker containers - no host dependencies required

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
Docker-Only Test Runner for DataFrame Processing System

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    api              Run API tests in Docker
    all              Run all tests in Docker (default)
    clean            Clean up test containers and volumes

Options:
    -h, --help       Show this help message
    -v, --verbose    Verbose output

Examples:
    $0                    # Run all tests (default)
    $0 api               # Run API tests only
    $0 all -v            # Run all tests with verbose output
    $0 clean             # Clean up after tests

Note: All tests run in Docker containers. No npm, Node.js, or Playwright 
      installation required on host machine.

EOF
}

check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        print_error "Docker is required but not found"
        print_info "Please install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # Check for docker compose (prefer plugin over standalone)
    if docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker-compose"
    else
        print_error "Docker Compose is required but not found"
        print_info "Please install Docker Compose"
        exit 1
    fi
    
    print_info "Using: $DOCKER_COMPOSE"
}

run_api_tests() {
    print_info "Running API tests in Docker..."
    cd "$PROJECT_ROOT"
    
    $DOCKER_COMPOSE -f tests/docker/docker-compose.simple.yml --profile api-tests up --build --abort-on-container-exit api-tests
    local exit_code=$?
    
    # Cleanup
    $DOCKER_COMPOSE -f tests/docker/docker-compose.simple.yml down >/dev/null 2>&1
    
    if [ $exit_code -eq 0 ]; then
        print_success "API tests completed successfully"
    else
        print_error "API tests failed"
        return $exit_code
    fi
}

run_all_tests() {
    print_info "Running all tests in Docker..."
    
    # Run API tests
    run_api_tests || return 1
    
    print_success "All tests completed successfully"
}

clean_tests() {
    print_info "Cleaning up test containers and volumes..."
    cd "$PROJECT_ROOT"
    
    # Stop and remove containers from both compose files
    $DOCKER_COMPOSE -f tests/docker/docker-compose.simple.yml down >/dev/null 2>&1
    $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml down --volumes >/dev/null 2>&1
    
    # Remove any test images
    docker images | grep "spark-test-visualizer.*test" | awk '{print $3}' | xargs -r docker rmi >/dev/null 2>&1 || true
    
    print_success "Cleanup completed"
}

# Parse command line arguments
COMMAND=""
VERBOSE="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        api|all|clean)
            COMMAND="$1"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            VERBOSE="true"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Set default command if none provided
if [[ -z "$COMMAND" ]]; then
    COMMAND="all"
fi

# Check Docker availability
check_docker

# Set verbose output for docker-compose if requested
if [[ "$VERBOSE" == "true" ]]; then
    export COMPOSE_LOG_LEVEL=DEBUG
fi

# Execute command
case "$COMMAND" in
    api)
        run_api_tests
        ;;
    all)
        run_all_tests
        ;;
    clean)
        clean_tests
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac