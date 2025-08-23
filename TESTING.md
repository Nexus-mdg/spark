# Testing Infrastructure Guide

This document provides comprehensive guidance for using the modernized testing infrastructure for the DataFrame Processing System.

## Overview

The testing infrastructure consists of three main components:

1. **API Testing (pytest)** - Structured tests for REST API endpoints
2. **Visual Testing (Playwright)** - UI component and flow testing with screenshot comparison
3. **Docker Integration** - Containerized testing environment for consistency

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 16+ (for visual tests)
- Docker & Docker Compose (for containerized testing)

### Basic Usage

```bash
# Install test dependencies
pip install -r tests/requirements.txt

# Run infrastructure tests (no services required)
make test-infrastructure

# Run API tests (requires API running)
make test-api

# Run visual tests (requires UI running) 
make test-visual

# Run all available tests
make test-all
```

## Test Categories

### 1. Infrastructure Tests

These tests validate the testing framework itself and can run without any services.

```bash
# Run via Makefile
make test-infrastructure

# Run via test script
./tests/scripts/run_tests.sh infrastructure

# Run directly with pytest
python -m pytest tests/api/test_infrastructure.py -v
```

**What they test:**
- Test fixture functionality
- Sample data validation
- API client utilities
- Environment configuration

### 2. API Testing

Modern pytest-based tests replacing the shell script approach.

```bash
# Start services first
make up

# Run API tests
make test-api

# Run specific test categories
./tests/scripts/run_tests.sh auth      # Authentication tests
./tests/scripts/run_tests.sh core     # Core operations (select, filter, etc.)

# Run with coverage and HTML report
./tests/scripts/run_tests.sh api --coverage --html
```

**Test Structure:**
```
tests/api/
├── conftest.py                 # Pytest fixtures and API client
├── test_authentication.py     # API protection and auth endpoints
├── test_core_operations.py     # DataFrame operations testing
├── test_infrastructure.py     # Framework validation
├── fixtures/
│   └── sample_data.py         # Test data fixtures
└── utils/
    └── api_client.py          # API testing utilities
```

**Key Features:**
- Automatic test data setup using existing sample CSV files
- Comprehensive API client with helper methods
- Response validation and error handling
- Test isolation and cleanup

### 3. Visual Testing

Playwright-based UI testing with screenshot comparison.

```bash
# Prerequisites: Start services and install Node.js dependencies
make up
cd tests/visual && npm install && npx playwright install chromium

# Run visual tests
make test-visual

# Run with different options
./tests/scripts/run_tests.sh visual --verbose
```

**Test Structure:**
```
tests/visual/
├── playwright.config.js       # Playwright configuration
├── tests/
│   ├── auth/
│   │   ├── login.spec.js      # Login flow testing
│   │   └── logout.spec.js     # Logout flow testing
│   ├── components/
│   │   └── header.spec.js     # Component testing
│   └── pages/
│       └── home.spec.js       # Page-level testing
├── utils/
│   └── visual_helpers.js      # Screenshot utilities
└── screenshots/               # Visual baselines and results
```

**Key Features:**
- Cross-viewport testing (mobile, tablet, desktop)
- Animation and scroll handling for consistent screenshots
- Baseline vs actual comparison with configurable thresholds
- Responsive design validation

## Test Runner Script

The unified test runner script provides a consistent interface:

```bash
./tests/scripts/run_tests.sh [COMMAND] [OPTIONS]
```

### Commands

| Command | Description |
|---------|-------------|
| `infrastructure` | Run framework validation tests |
| `api` | Run API endpoint tests |
| `visual` | Run visual regression tests |
| `auth` | Run authentication-specific tests |
| `core` | Run core DataFrame operation tests |
| `all` | Run all available tests |
| `docker` | Run tests in Docker environment |
| `wait-api` | Wait for API to be ready |
| `wait-ui` | Wait for UI to be ready |
| `setup` | Setup test environment |
| `clean` | Clean test artifacts |

### Options

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Verbose output |
| `-p, --parallel` | Run tests in parallel |
| `-f, --failfast` | Stop on first failure |
| `--coverage` | Generate coverage report |
| `--html` | Generate HTML test report |
| `--api-base URL` | Override API base URL |
| `--ui-base URL` | Override UI base URL |
| `--env ENV` | Set test environment |

### Examples

```bash
# Basic usage
./tests/scripts/run_tests.sh api

# With options
./tests/scripts/run_tests.sh api --verbose --coverage --html

# Different environment
./tests/scripts/run_tests.sh all --env=ci --parallel

# Custom URLs
./tests/scripts/run_tests.sh visual --ui-base=http://localhost:3000
```

## Docker Integration

### Local Docker Testing

```bash
# Run API tests in Docker
docker-compose -f tests/docker/docker-compose.test.yml --profile api-tests up --build

# Run visual tests in Docker  
docker-compose -f tests/docker/docker-compose.test.yml --profile visual-tests up --build

# Run via test script
./tests/scripts/run_tests.sh docker
```

### Docker Test Environment

The Docker setup includes:
- Isolated PostgreSQL database for testing
- Isolated Redis instance
- Test-configured DataFrame API
- Test-configured DataFrame UI
- Playwright container with browsers pre-installed

## CI/CD Integration

### GitHub Actions

The updated workflow includes multiple test jobs:

- **test-infrastructure**: Framework validation (always runs first)
- **test-api-core**: Modern API tests with pytest
- **test-visual**: UI testing with Playwright
- **test-legacy**: Legacy shell-based tests (for compatibility)
- **test-with-spark**: Spark-enabled tests (optional)

### Test Artifacts

Test results are automatically collected:
- API test coverage reports
- Visual test screenshots and diff reports
- HTML test reports
- Test logs and artifacts

## Writing Tests

### API Tests

```python
# tests/api/test_example.py
def test_my_endpoint(api_client, setup_test_data):
    """Test custom endpoint functionality."""
    response = api_client.my_operation("test_data", {"param": "value"})
    
    # Validate response
    data = validate_dataframe_response(response)
    assert data['name'] == 'test_data'
    assert len(data['data']) > 0
```

### Visual Tests

```javascript
// tests/visual/tests/example.spec.js
import { test, expect } from '@playwright/test';

test('should display component correctly', async ({ page }) => {
  await page.goto('/');
  
  // Wait for component
  await expect(page.locator('.my-component')).toBeVisible();
  
  // Take screenshot
  await expect(page).toHaveScreenshot('my-component.png');
});
```

## Environment Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_BASE` | `http://localhost:4999` | API base URL |
| `UI_BASE` | `http://localhost:5001` | UI base URL |
| `TEST_ENV` | `local` | Test environment |
| `PARALLEL_WORKERS` | `2` | Number of parallel workers |

### Test Environments

- **local**: Development testing with live services
- **ci**: GitHub Actions environment
- **docker**: Containerized testing environment

## Troubleshooting

### Common Issues

1. **API not ready**: Wait for services to start fully
   ```bash
   ./tests/scripts/run_tests.sh wait-api
   ```

2. **Visual test differences**: Update baselines if UI changes are expected
   ```bash
   cd tests/visual
   npx playwright test --update-snapshots
   ```

3. **Docker issues**: Clean and rebuild containers
   ```bash
   docker-compose -f tests/docker/docker-compose.test.yml down --volumes
   ./tests/scripts/run_tests.sh docker
   ```

### Debugging

- Use `--verbose` flag for detailed output
- Check test reports in `tests/reports/`
- Use `./tests/scripts/run_tests.sh setup` to verify environment
- Review GitHub Actions logs for CI failures

## Migration from Legacy Tests

The new infrastructure maintains compatibility while providing modern features:

### Legacy vs Modern

| Legacy | Modern | Benefits |
|--------|--------|----------|
| `test.sh select` | `make test-core` | Better error handling, fixtures |
| Manual curl commands | API client utilities | Reusable, maintainable |
| No visual testing | Playwright integration | UI regression detection |
| Shell scripts | pytest framework | Better reporting, parallel execution |

### Gradual Migration

1. Start with infrastructure tests to validate setup
2. Migrate API tests incrementally
3. Add visual tests for critical UI components
4. Use Docker for consistent CI/CD testing

The legacy `test.sh` script remains available for compatibility during the transition period.

## Support

For issues or questions:
1. Check this documentation
2. Review test logs and reports
3. Check GitHub Actions workflow results
4. Review the test configuration files