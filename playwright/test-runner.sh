#!/bin/bash

# Playwright Test Runner Script
# This script demonstrates how to run the Playwright visual tests

set -e

echo "=== Playwright Visual Tests Setup ==="
echo ""

# Check if Playwright container is built
if ! docker images | grep -q "spark-test-visualizer-playwright-tests"; then
    echo "Building Playwright test container..."
    make test-visual-build
    echo "✓ Playwright container built successfully"
else
    echo "✓ Playwright container already exists"
fi

echo ""
echo "=== Running Basic Test Validation ==="

# Test if the container runs and shows help
echo "Testing container execution..."
docker run --rm spark-test-visualizer-playwright-tests python --version
echo "✓ Python is working in container"

docker run --rm spark-test-visualizer-playwright-tests python -c "import playwright; print('✓ Playwright imported successfully')"
echo "✓ Playwright is installed"

docker run --rm spark-test-visualizer-playwright-tests python -c "import pytest; print('✓ Pytest imported successfully')"
echo "✓ Pytest is installed"

echo ""
echo "=== Test Structure Validation ==="

# Show test structure
docker run --rm spark-test-visualizer-playwright-tests find tests/ -name "*.py" -type f
echo "✓ Test files are present"

# Show configuration
docker run --rm spark-test-visualizer-playwright-tests ls -la config/
echo "✓ Configuration files are present"

echo ""
echo "=== Test Syntax Validation ==="

# Validate Python syntax of test files
docker run --rm spark-test-visualizer-playwright-tests python -m py_compile tests/test_home_page.py
echo "✓ test_home_page.py syntax is valid"

docker run --rm spark-test-visualizer-playwright-tests python -m py_compile tests/test_dataframes_list.py
echo "✓ test_dataframes_list.py syntax is valid"

docker run --rm spark-test-visualizer-playwright-tests python -m py_compile utils/helpers.py
echo "✓ helpers.py syntax is valid"

echo ""
echo "=== Dry Run Tests ==="

# Show available tests without running them
echo "Available tests:"
docker run --rm spark-test-visualizer-playwright-tests python -m pytest --collect-only -q

echo ""
echo "=== Setup Complete ==="
echo "The Playwright visual testing infrastructure is ready!"
echo ""
echo "To run the tests when the application is running:"
echo "  make test-visual           # Run all tests (headless)"
echo "  make test-visual-dev       # Run with browser window"
echo "  make test-visual-screenshots # Capture screenshots"
echo ""
echo "Note: Make sure the application services are running before executing tests:"
echo "  docker compose up -d redis dataframe-api dataframe-ui-x"