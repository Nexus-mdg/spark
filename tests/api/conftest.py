"""
Pytest configuration and fixtures for API testing.
Provides reusable fixtures for API client, test data, and environment setup.
"""

import pytest
import requests
import time
import os
from typing import Dict, Any, Optional


@pytest.fixture(scope="session")
def api_base_url() -> str:
    """Get the API base URL from environment or default."""
    return os.getenv("API_BASE", "http://localhost:4999")


@pytest.fixture(scope="session")
def api_client(api_base_url: str):
    """Create an API client with helper methods for testing."""
    
    class APIClient:
        def __init__(self, base_url: str):
            self.base_url = base_url
            self.session = requests.Session()
            # Set user agent to avoid API protection
            self.session.headers.update({
                'User-Agent': 'pytest-api-client',
                'Content-Type': 'application/json'
            })
        
        def wait_for_api(self, timeout: int = 60) -> bool:
            """Wait for API to be ready."""
            for _ in range(timeout):
                try:
                    response = self.session.get(f"{self.base_url}/api/stats")
                    if response.status_code == 200:
                        return True
                except requests.RequestException:
                    pass
                time.sleep(1)
            return False
        
        def upload_dataframe(self, name: str, file_path: str) -> requests.Response:
            """Upload a dataframe from file."""
            with open(file_path, 'rb') as f:
                files = {'file': f}
                data = {'name': name}
                # Remove Content-Type for multipart form data
                headers = {k: v for k, v in self.session.headers.items() if k != 'Content-Type'}
                return self.session.post(
                    f"{self.base_url}/api/dataframes/upload",
                    files=files,
                    data=data,
                    headers=headers
                )
        
        def get_dataframe(self, name: str) -> requests.Response:
            """Get dataframe info."""
            return self.session.get(f"{self.base_url}/api/dataframes/{name}")
        
        def select_operation(self, name: str, columns: str, exclude: bool = False) -> requests.Response:
            """Execute select operation."""
            params = {'name': name, 'columns': columns}
            if exclude:
                params['exclude'] = 'true'
            return self.session.get(f"{self.base_url}/api/ops/select/get", params=params)
        
        def filter_operation(self, name: str, filters: list, combine: str = "and") -> requests.Response:
            """Execute filter operation via POST."""
            data = {
                'name': name,
                'filters': filters,
                'combine': combine
            }
            return self.session.post(f"{self.base_url}/api/ops/filter", json=data)
        
        def groupby_operation(self, name: str, by: list, agg: dict) -> requests.Response:
            """Execute groupby operation."""
            params = {
                'name': name,
                'by': ','.join(by) if isinstance(by, list) else by,
                'agg': ','.join([f"{col}:{func}" for col, func in agg.items()])
            }
            return self.session.get(f"{self.base_url}/api/ops/groupby/get", params=params)
        
        def merge_operation(self, left: str, right: str, on: str, how: str = "inner") -> requests.Response:
            """Execute merge operation."""
            params = {
                'left': left,
                'right': right,
                'on': on,
                'how': how
            }
            return self.session.get(f"{self.base_url}/api/ops/merge/get", params=params)
        
        def pivot_operation(self, name: str, mode: str = "wider", **kwargs) -> requests.Response:
            """Execute pivot operation via POST."""
            data = {'name': name, 'mode': mode, **kwargs}
            return self.session.post(f"{self.base_url}/api/ops/pivot", json=data)
        
        def mutate_operation(self, name: str, target: str, mode: str, expr: str) -> requests.Response:
            """Execute mutate operation via POST."""
            data = {
                'name': name,
                'target': target,
                'mode': mode,
                'expr': expr
            }
            return self.session.post(f"{self.base_url}/api/ops/mutate", json=data)
        
        def compare_operation(self, name1: str, name2: str) -> requests.Response:
            """Execute compare operation."""
            params = {'name1': name1, 'name2': name2}
            return self.session.get(f"{self.base_url}/api/ops/compare/get", params=params)
        
        def stats(self) -> requests.Response:
            """Get API stats."""
            return self.session.get(f"{self.base_url}/api/stats")
    
    return APIClient(api_base_url)


@pytest.fixture(scope="session") 
def sample_data_dir() -> str:
    """Get the sample data directory path."""
    return os.path.join(os.path.dirname(__file__), "../../dataframe-api/data/sample")


@pytest.fixture(scope="session")
def setup_test_data(api_client, sample_data_dir):
    """Upload test data before running tests (only if API is available)."""
    import os
    
    # Wait for API to be ready
    if not api_client.wait_for_api(timeout=10):
        pytest.skip("API not available, skipping data setup")
    
    # Upload people.csv
    people_file = os.path.join(sample_data_dir, "people.csv")
    if os.path.exists(people_file):
        response = api_client.upload_dataframe("people", people_file)
        # Accept 201 (created), 200 (ok), or 409 (already exists)
        assert response.status_code in [200, 201, 409], f"Failed to upload people.csv: {response.status_code}"
    
    # Upload purchases.csv
    purchases_file = os.path.join(sample_data_dir, "purchases.csv")
    if os.path.exists(purchases_file):
        response = api_client.upload_dataframe("purchases", purchases_file)
        assert response.status_code in [200, 201, 409], f"Failed to upload purchases.csv: {response.status_code}"


@pytest.fixture
def sample_filters():
    """Sample filter configurations for testing."""
    return {
        'simple': [{'col': 'age', 'op': 'gte', 'value': 30}],
        'multiple': [
            {'col': 'age', 'op': 'gte', 'value': 25},
            {'col': 'age', 'op': 'lte', 'value': 35}
        ],
        'string': [{'col': 'city', 'op': 'eq', 'value': 'New York'}]
    }


@pytest.fixture 
def sample_aggregations():
    """Sample aggregation configurations for testing."""
    return {
        'count': {'id': 'count'},
        'sum': {'quantity': 'sum'},
        'avg': {'quantity': 'avg'},
        'multiple': {'quantity': 'sum', 'id': 'count'}
    }