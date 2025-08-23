#!/bin/bash
set -euo pipefail

# Test execution script for modern testing infrastructure
# Provides unified interface for running different types of tests

# Default configuration
API_BASE="${API_BASE:-http://localhost:4999}"
UI_BASE="${UI_BASE:-http://localhost:5001}"
TEST_ENV="${TEST_ENV:-local}"
PARALLEL_WORKERS="${PARALLEL_WORKERS:-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TESTS_DIR="$PROJECT_ROOT/tests"

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

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
Test Runner for DataFrame Processing System

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    infrastructure    Run infrastructure/setup tests only
    api              Run API endpoint tests
    visual           Run visual regression tests (Docker-based, no host npm required)
    auth             Run authentication tests
    core             Run core operation tests (select, filter, etc.)
    all              Run all available tests
    docker           Run tests in Docker environment
    
    wait-api         Wait for API to be ready
    wait-ui          Wait for UI to be ready
    setup            Setup test environment and dependencies
    clean            Clean test artifacts and temporary files

Options:
    -h, --help       Show this help message
    -v, --verbose    Verbose output
    -p, --parallel   Run tests in parallel (default: $PARALLEL_WORKERS workers)
    -f, --failfast   Stop on first failure
    --coverage       Generate coverage report
    --html           Generate HTML test report
    --api-base URL   API base URL (default: $API_BASE)
    --ui-base URL    UI base URL (default: $UI_BASE)
    --env ENV        Test environment: local, ci, docker (default: $TEST_ENV)

Examples:
    $0 infrastructure              # Test basic infrastructure
    $0 api --verbose               # Run API tests with verbose output
    $0 visual                      # Run visual tests in Docker (no npm needed)
    $0 all --coverage --html       # Run all tests with reports
    $0 core --parallel 4           # Run core tests with 4 workers
    $0 docker                      # Run complete test suite in Docker
    $0 wait-api                    # Wait for API to be ready

Environment Variables:
    API_BASE         API base URL
    UI_BASE          UI base URL  
    TEST_ENV         Test environment (local, ci, docker)
    PARALLEL_WORKERS Number of parallel workers

EOF
}

wait_for_api() {
    print_info "Waiting for API at $API_BASE to be ready..."
    for i in {1..60}; do
        if curl -fsS "$API_BASE/api/stats" >/dev/null 2>&1; then
            print_success "API is ready"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    print_error "API not ready after timeout"
    return 1
}

wait_for_ui() {
    print_info "Waiting for UI at $UI_BASE to be ready..."
    for i in {1..60}; do
        if curl -fsS "$UI_BASE" >/dev/null 2>&1; then
            print_success "UI is ready"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    print_error "UI not ready after timeout"
    return 1
}

setup_test_environment() {
    print_info "Setting up test environment..."
    
    # Install test dependencies if not already installed (for API tests only)
    if ! python -c "import pytest" >/dev/null 2>&1; then
        print_info "Installing Python test dependencies..."
        pip install -r "$TESTS_DIR/requirements.txt"
    fi
    
    # Create temp directories
    mkdir -p "$TESTS_DIR/reports/api_coverage"
    mkdir -p "$TESTS_DIR/reports/visual_reports"
    mkdir -p "/tmp/test_data"
    
    # Check Docker availability for visual tests
    if command -v docker >/dev/null 2>&1; then
        if docker compose version >/dev/null 2>&1; then
            print_success "Docker and Docker Compose detected - visual tests will run in containers"
        elif command -v docker-compose >/dev/null 2>&1; then
            print_success "Docker and docker-compose detected - visual tests will run in containers"
        else
            print_warning "Docker detected but Docker Compose not found - visual tests may not work"
            print_info "Install Docker Compose to enable visual regression testing"
        fi
    else
        print_warning "Docker not detected - visual tests will not be available"
        print_info "Install Docker to enable visual regression testing"
    fi
    
    print_success "Test environment setup complete"
}

run_infrastructure_tests() {
    print_info "Running infrastructure tests..."
    cd "$PROJECT_ROOT"
    
    local pytest_args=(
        "tests/api/test_infrastructure.py"
        "-v"
        "--rootdir=$PROJECT_ROOT"
    )
    
    if [[ "$VERBOSE" == "true" ]]; then
        pytest_args+=("-s")
    fi
    
    python -m pytest "${pytest_args[@]}"
}

run_api_tests() {
    print_info "Running API tests..."
    cd "$PROJECT_ROOT"
    
    # Wait for API to be ready
    wait_for_api || return 1
    
    local pytest_args=(
        "tests/api/test_authentication.py"
        "tests/api/test_core_operations.py"
        "-v"
    )
    
    if [[ "$VERBOSE" == "true" ]]; then
        pytest_args+=("-s")
    fi
    
    if [[ "$PARALLEL" == "true" ]]; then
        pytest_args+=("-n" "$PARALLEL_WORKERS")
    fi
    
    if [[ "$FAILFAST" == "true" ]]; then
        pytest_args+=("-x")
    fi
    
    if [[ "$COVERAGE" == "true" ]]; then
        pytest_args+=("--cov=tests" "--cov-report=html:tests/reports/api_coverage")
    fi
    
    if [[ "$HTML_REPORT" == "true" ]]; then
        pytest_args+=("--html=tests/reports/api_report.html")
    fi
    
    export API_BASE="$API_BASE"
    export TEST_ENV="$TEST_ENV"
    
    python -m pytest "${pytest_args[@]}"
}

run_core_tests() {
    print_info "Running core operation tests..."
    cd "$PROJECT_ROOT"
    
    # Wait for API to be ready
    wait_for_api || return 1
    
    local pytest_args=(
        "tests/api/test_core_operations.py"
        "-v"
        "-k" "TestSelectOperations or TestFilterOperations or TestGroupByOperations or TestMergeOperations"
    )
    
    if [[ "$VERBOSE" == "true" ]]; then
        pytest_args+=("-s")
    fi
    
    if [[ "$PARALLEL" == "true" ]]; then
        pytest_args+=("-n" "$PARALLEL_WORKERS")
    fi
    
    export API_BASE="$API_BASE"
    export TEST_ENV="$TEST_ENV"
    
    python -m pytest "${pytest_args[@]}"
}

run_auth_tests() {
    print_info "Running authentication tests..."
    cd "$PROJECT_ROOT"
    
    # Wait for API to be ready
    wait_for_api || return 1
    
    local pytest_args=(
        "tests/api/test_authentication.py"
        "-v"
    )
    
    if [[ "$VERBOSE" == "true" ]]; then
        pytest_args+=("-s")
    fi
    
    export API_BASE="$API_BASE"
    export TEST_ENV="$TEST_ENV"
    
    python -m pytest "${pytest_args[@]}"
}

run_visual_tests() {
    print_info "Running visual tests in Docker..."
    cd "$PROJECT_ROOT"
    
    # Check if docker-compose is available
    if ! command -v docker >/dev/null 2>&1; then
        print_error "Docker is required for visual tests"
        print_info "Please install Docker"
        return 1
    fi
    
    # Check for docker compose (prefer plugin over standalone)
    if docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker-compose"
    else
        print_error "Docker Compose is required for visual tests"
        print_info "Please install Docker Compose"
        return 1
    fi
    
    # Determine test mode based on environment
    if [[ "$TEST_ENV" == "local" ]]; then
        # For local testing, use host network to connect to existing services
        wait_for_ui || {
            print_warning "UI not available, visual tests require the UI to be running"
            print_info "Start the UI with: make up"
            return 1
        }
        
        print_info "Running visual tests against local services..."
        local playwright_args="npx playwright test"
        
        if [[ "$VERBOSE" == "true" ]]; then
            playwright_args="$playwright_args --reporter=list,html"
        fi
        
        if [[ "$FAILFAST" == "true" ]]; then
            playwright_args="$playwright_args --max-failures=1"
        fi
        
        # Use host network configuration for local services
        $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml --profile visual-tests-local run --rm \
            -e UI_BASE="http://host.docker.internal:5001" \
            -e API_BASE="http://host.docker.internal:4999" \
            -e TEST_ENV="local" \
            playwright-tests-local sh -c "$playwright_args"
    else
        # For Docker/CI environment, start all services
        print_info "Starting test environment with all services..."
        $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml --profile visual-tests up --build --abort-on-container-exit playwright-tests
        
        # Cleanup
        $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml down --volumes
    fi
}

run_all_tests() {
    print_info "Running all available tests..."
    
    # Run infrastructure tests first
    run_infrastructure_tests || return 1
    
    # Run API tests if API is available
    if wait_for_api; then
        run_api_tests || return 1
    else
        print_warning "API not available, skipping API tests"
    fi
    
    # Run visual tests if UI is available
    if wait_for_ui; then
        run_visual_tests || {
            print_warning "Visual tests failed or skipped"
        }
    else
        print_warning "UI not available, skipping visual tests"
    fi
    
    print_success "All tests completed"
}

clean_test_artifacts() {
    print_info "Cleaning test artifacts..."
    
    # Remove test reports
    rm -rf "$TESTS_DIR/reports/api_coverage"
    rm -rf "$TESTS_DIR/reports/visual_reports"
    rm -f "$TESTS_DIR/reports/api_report.html"
    
    # Remove pytest cache
    rm -rf "$PROJECT_ROOT/.pytest_cache"
    
    # Remove temp test data
    rm -rf "/tmp/test_data"
    
    print_success "Test artifacts cleaned"
}

# Parse command line arguments
COMMAND=""
VERBOSE="false"
PARALLEL="false"
FAILFAST="false"
COVERAGE="false"
HTML_REPORT="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        infrastructure|api|visual|auth|core|all|docker|wait-api|wait-ui|setup|clean)
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
        -p|--parallel)
            PARALLEL="true"
            shift
            ;;
        -f|--failfast)
            FAILFAST="true"
            shift
            ;;
        --coverage)
            COVERAGE="true"
            shift
            ;;
        --html)
            HTML_REPORT="true"
            shift
            ;;
        --api-base)
            API_BASE="$2"
            shift 2
            ;;
        --ui-base)
            UI_BASE="$2"
            shift 2
            ;;
        --env)
            TEST_ENV="$2"
            shift 2
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

# Export environment variables
export API_BASE="$API_BASE"
export UI_BASE="$UI_BASE"
export TEST_ENV="$TEST_ENV"
export PARALLEL_WORKERS="$PARALLEL_WORKERS"

# Execute command
case "$COMMAND" in
    infrastructure)
        run_infrastructure_tests
        ;;
    api)
        run_api_tests
        ;;
    visual)
        run_visual_tests
        ;;
    auth)
        run_auth_tests
        ;;
    core)
        run_core_tests
        ;;
    all)
        run_all_tests
        ;;
    docker)
        print_info "Running tests in Docker environment..."
        cd "$PROJECT_ROOT"
        
        # Check if docker is available
        if ! command -v docker >/dev/null 2>&1; then
            print_error "Docker is required for Docker testing"
            return 1
        fi
        
        # Check for docker compose (prefer plugin over standalone)
        if docker compose version >/dev/null 2>&1; then
            DOCKER_COMPOSE="docker compose"
        elif command -v docker-compose >/dev/null 2>&1; then
            DOCKER_COMPOSE="docker-compose"
        else
            print_error "Docker Compose is required for Docker testing"
            return 1
        fi
        
        # Set test environment
        export TEST_ENV="docker"
        
        # Run API tests in Docker
        print_info "Running API tests in Docker..."
        $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml --profile api-tests up --build --abort-on-container-exit api-tests
        
        # Run visual tests in Docker
        print_info "Running visual tests in Docker..."
        $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml --profile visual-tests up --build --abort-on-container-exit playwright-tests
        
        # Cleanup
        print_info "Cleaning up test containers..."
        $DOCKER_COMPOSE -f tests/docker/docker-compose.test.yml down --volumes
        
        print_success "Docker tests completed"
        ;;
    wait-api)
        wait_for_api
        ;;
    wait-ui)
        wait_for_ui
        ;;
    setup)
        setup_test_environment
        ;;
    clean)
        clean_test_artifacts
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac