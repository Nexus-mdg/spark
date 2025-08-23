# DataFrame Processing System - Docker-Based Testing

This directory contains **Docker-containerized tests** for the DataFrame Processing System. All tests run in isolated Docker containers - **no npm, Node.js, or Python installation required on your host machine**.

## Quick Start

```bash
# Run all tests (Docker-only)
make test-docker

# Run API tests only  
make test-api

# Clean up
make test-clean
```

## Prerequisites

- Docker and Docker Compose
- That's it! No other dependencies needed on host.

## Test Types

### API Tests (`tests/api/`)
- **Framework**: pytest running in Python container
- **Purpose**: Test infrastructure, data fixtures, and basic functionality
- **Container**: Python 3.11 with pytest pre-installed
- **Tests**: Infrastructure validation, data consistency, environment setup

## Running Tests

### All Tests (Recommended)
```bash
make test-docker
```
This runs the complete test suite in isolated Docker containers.

### API Tests Only
```bash
make test-api
```

### Direct Script Usage
```bash
# All tests
./tests/scripts/run_tests.sh

# API tests only
./tests/scripts/run_tests.sh api

# With verbose output
./tests/scripts/run_tests.sh api -v
```

### Direct Docker Compose Usage
```bash
# API tests
docker compose -f tests/docker/docker-compose.simple.yml --profile api-tests up --build
```

## Test Architecture

```
tests/
├── README.md              # This file
├── api/                   # API tests (pytest)
│   ├── test_infrastructure.py     # Infrastructure validation
│   ├── fixtures/                  # Test data and utilities
│   └── utils/                     # Helper utilities
├── docker/                # Docker configuration
│   └── docker-compose.simple.yml # Simple test orchestration
├── scripts/               # Test execution scripts
│   └── run_tests.sh              # Unified test runner
└── requirements.txt       # Python test dependencies
```

## Test Reports

Test reports are automatically generated and available at:
- **API Reports**: `tests/reports/api_report.html` (self-contained HTML)

## How It Works

1. **Simple Containers**: Single Python container for API tests
2. **No Dependencies**: Only Docker required on host - all tools installed in container
3. **Fast Execution**: Infrastructure tests run in under 1 second
4. **Automatic Cleanup**: Containers are removed after test completion

## What Gets Tested

Currently includes **Infrastructure Tests**:
- ✅ Sample data fixtures (people.csv, purchases.csv) 
- ✅ CSV file creation and validation
- ✅ Test case structure validation
- ✅ Import verification for test utilities
- ✅ Environment variable handling
- ✅ Data consistency and format validation

## Example Output

```bash
$ make test-api
[INFO] Running API tests in Docker...
Installing test dependencies...
Running API tests...
============================= test session starts ==============================
tests/api/test_infrastructure.py::test_sample_people_data_fixture PASSED [ 11%]
tests/api/test_infrastructure.py::test_sample_purchases_data_fixture PASSED [ 22%]
tests/api/test_infrastructure.py::test_temp_csv_file_creation PASSED     [ 33%]
...
============================== 9 passed in 0.04s ===============================
[SUCCESS] API tests completed successfully
```

## Troubleshooting

### Tests Failing?
```bash
# Check if Docker is running
docker version

# Clean up and retry
make test-clean
make test-api
```

### Reports Not Generated?
Check the `tests/reports/` directory after running tests. Reports are HTML files that can be opened in any browser.

## Adding New Tests

Add new test files to `tests/api/` following the pytest convention:
```python
# tests/api/test_my_feature.py
def test_my_feature():
    # Your test here
    assert True
```

All tests automatically run in Docker containers when using the make targets or test runner script.