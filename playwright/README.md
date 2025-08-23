# Playwright Visual Tests

This directory contains Playwright-based visual tests for the Spark Test Visualizer application, specifically focused on testing the dataframes list functionality on the home page.

## Overview

The visual tests are implemented using Python and Playwright to provide comprehensive end-to-end testing of the user interface, particularly the dataframes list functionality.

## Structure

```
playwright/
├── Dockerfile              # Docker configuration for Playwright container
├── requirements.txt         # Python dependencies
├── conftest.py             # Pytest configuration and fixtures
├── pytest.ini             # Pytest settings
├── config/
│   ├── __init__.py
│   └── settings.py         # Configuration settings
├── utils/
│   ├── __init__.py
│   └── helpers.py          # Test helper utilities
├── tests/
│   ├── __init__.py
│   ├── test_home_page.py   # General home page tests
│   └── test_dataframes_list.py  # Dataframes list specific tests
├── test-data/              # Sample CSV files for testing
│   ├── sample.csv
│   └── products.csv
└── screenshots/            # Screenshots on test failures (auto-created)
```

## Features Tested

### Home Page Tests (`test_home_page.py`)
- Page loading and basic structure
- Navigation elements
- Animated feature showcase
- Dark mode functionality (if available)
- Footer presence
- Refresh functionality
- Error handling
- Accessibility basics
- Loading states
- Keyboard navigation
- Layout stability

### Dataframes List Tests (`test_dataframes_list.py`)
- Table structure and headers
- Empty state display
- Upload form functionality
- Dataframe upload and display
- Pagination controls
- Filter/search functionality
- Statistics display
- Charts section
- Responsive layout
- Table sorting

## Running Tests

### Prerequisites
- Docker and Docker Compose
- The main application services running

### Quick Start

1. **Build the test container:**
   ```bash
   make test-visual-build
   ```

2. **Run all visual tests:**
   ```bash
   make test-visual
   ```

3. **Run tests in development mode (with browser window):**
   ```bash
   make test-visual-dev
   ```

4. **Run tests with screenshot capture:**
   ```bash
   make test-visual-screenshots
   ```

5. **Clean up test artifacts:**
   ```bash
   make test-visual-clean
   ```

### Manual Docker Commands

If you prefer to run Docker commands manually:

```bash
# Start application services
docker compose up -d redis dataframe-api dataframe-ui-x

# Run tests
docker compose --profile testing run --rm playwright-tests

# Run specific test file
docker compose --profile testing run --rm playwright-tests python -m pytest tests/test_dataframes_list.py -v

# Run with specific browser
docker compose --profile testing run --rm -e BROWSER=firefox playwright-tests
```

## Configuration

The tests can be configured via environment variables:

- `BASE_URL`: Application URL (default: http://localhost:5001)
- `API_BASE_URL`: API URL (default: http://localhost:4999)
- `HEADLESS`: Run in headless mode (default: true)
- `BROWSER`: Browser to use (chromium, firefox, webkit)
- `TIMEOUT`: Default timeout in milliseconds
- `SCREENSHOT_ON_FAILURE`: Capture screenshots on test failures

## Test Data

Sample CSV files are provided in the `test-data/` directory:
- `sample.csv`: Employee data with departments and salaries
- `products.csv`: Product sales data

These files are used for testing upload functionality and ensuring the dataframes list displays correctly with real data.

## Screenshots

When tests fail or when explicitly configured, screenshots are automatically captured and saved to the `screenshots/` directory. This helps with debugging visual issues.

## Key Test Scenarios

1. **Empty State**: Tests how the application behaves when no dataframes are present
2. **Data Loading**: Tests upload and display of dataframes
3. **Pagination**: Tests navigation through multiple pages of dataframes
4. **Filtering**: Tests search and filter functionality
5. **Responsive Design**: Tests layout at different screen sizes
6. **Accessibility**: Basic accessibility compliance checks
7. **User Interactions**: Tests buttons, forms, and interactive elements

## Extending Tests

To add new tests:

1. Create new test files in the `tests/` directory following the naming convention `test_*.py`
2. Use the existing fixtures from `conftest.py`
3. Import helpers from `utils.helpers`
4. Follow the existing test patterns and assertions

Example new test:
```python
from playwright.async_api import Page, expect
from utils import TestHelpers

async def test_new_functionality(page: Page):
    await TestHelpers.ensure_dataframes_list_loaded(page)
    # Your test logic here
    await expect(page.locator('selector')).to_be_visible()
```

## Troubleshooting

- **Connection Issues**: Ensure the application services are running before running tests
- **Timeout Errors**: Increase timeout values for slower environments
- **Browser Issues**: Try different browsers if tests fail on a specific one
- **Screenshot Path Issues**: Ensure the screenshots directory is writable

## CI/CD Integration

These tests can be integrated into CI/CD pipelines. The Docker-based approach ensures consistent test environments across different systems.