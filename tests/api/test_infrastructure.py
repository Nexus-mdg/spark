"""
Basic infrastructure tests to verify the testing framework.
These tests run without requiring the API to be available.
"""

import pytest
import tempfile
import os
from tests.api.fixtures.sample_data import (
    sample_people_data,
    sample_purchases_data,
    temp_csv_file,
    sample_filter_test_cases
)


def test_sample_people_data_fixture(sample_people_data):
    """Test that sample people data fixture works correctly."""
    assert len(sample_people_data) == 10, "Should have 10 people records"
    
    # Check structure of first record
    first_record = sample_people_data[0]
    expected_keys = {"id", "name", "age", "city"}
    assert set(first_record.keys()) == expected_keys, f"Expected keys {expected_keys}"
    
    # Check data types
    assert isinstance(first_record["id"], int), "ID should be integer"
    assert isinstance(first_record["name"], str), "Name should be string"
    assert isinstance(first_record["age"], int), "Age should be integer"
    assert isinstance(first_record["city"], str), "City should be string"


def test_sample_purchases_data_fixture(sample_purchases_data):
    """Test that sample purchases data fixture works correctly."""
    assert len(sample_purchases_data) == 8, "Should have 8 purchase records"
    
    # Check structure of first record
    first_record = sample_purchases_data[0]
    expected_keys = {"id", "product", "quantity", "price", "city", "date"}
    assert set(first_record.keys()) == expected_keys, f"Expected keys {expected_keys}"
    
    # Check data types
    assert isinstance(first_record["id"], int), "ID should be integer"
    assert isinstance(first_record["product"], str), "Product should be string"
    assert isinstance(first_record["quantity"], int), "Quantity should be integer"
    assert isinstance(first_record["price"], (int, float)), "Price should be numeric"


def test_temp_csv_file_creation(temp_csv_file, sample_people_data):
    """Test that temporary CSV file creation works."""
    csv_path = temp_csv_file(sample_people_data, "test_people")
    
    # File should exist
    assert os.path.exists(csv_path), "CSV file should be created"
    
    # File should have content
    with open(csv_path, 'r') as f:
        content = f.read()
        assert "id,name,age,city" in content, "Should have CSV header"
        assert "Alice Johnson" in content, "Should have sample data"
    
    # File extension should be correct
    assert csv_path.endswith('.csv'), "Should have .csv extension"


def test_filter_test_cases_structure(sample_filter_test_cases):
    """Test that filter test cases have the expected structure."""
    assert 'age_filters' in sample_filter_test_cases, "Should have age filters"
    assert 'city_filters' in sample_filter_test_cases, "Should have city filters"
    
    # Check age filter structure
    age_filters = sample_filter_test_cases['age_filters']
    assert 'gte_30' in age_filters, "Should have gte_30 filter"
    
    gte_30_filter = age_filters['gte_30'][0]
    assert gte_30_filter['col'] == 'age', "Filter should target age column"
    assert gte_30_filter['op'] == 'gte', "Filter should use gte operator"
    assert gte_30_filter['value'] == 30, "Filter should use value 30"


def test_api_client_utility_imports():
    """Test that API client utilities can be imported correctly."""
    from tests.api.utils.api_client import (
        DataFrameAPIError,
        validate_response_structure,
        validate_dataframe_response,
        DataFrameTestHelper
    )
    
    # Should be able to create exception
    error = DataFrameAPIError("test error")
    assert str(error) == "test error", "Exception should work correctly"


def test_conftest_imports():
    """Test that conftest fixtures can be imported correctly."""
    # This will fail if there are syntax errors in conftest.py
    from tests.api.conftest import api_base_url
    
    # The fixture itself can't be tested without pytest running it,
    # but we can verify it's importable
    assert callable(api_base_url), "api_base_url should be a callable fixture"


class TestDataValidation:
    """Test data validation and structure checks."""
    
    def test_people_data_consistency(self, sample_people_data):
        """Test that people data is consistent and well-formed."""
        # All IDs should be unique
        ids = [person["id"] for person in sample_people_data]
        assert len(ids) == len(set(ids)), "All IDs should be unique"
        
        # Ages should be reasonable
        ages = [person["age"] for person in sample_people_data]
        assert all(18 <= age <= 100 for age in ages), "Ages should be reasonable"
        
        # Names should not be empty
        names = [person["name"] for person in sample_people_data]
        assert all(name.strip() for name in names), "Names should not be empty"
        
        # Cities should not be empty
        cities = [person["city"] for person in sample_people_data]
        assert all(city.strip() for city in cities), "Cities should not be empty"
    
    def test_purchases_data_consistency(self, sample_purchases_data):
        """Test that purchases data is consistent and well-formed."""
        # Prices should be positive
        prices = [purchase["price"] for purchase in sample_purchases_data]
        assert all(price > 0 for price in prices), "Prices should be positive"
        
        # Quantities should be positive integers
        quantities = [purchase["quantity"] for purchase in sample_purchases_data]
        assert all(isinstance(qty, int) and qty > 0 for qty in quantities), "Quantities should be positive integers"
        
        # Products should not be empty
        products = [purchase["product"] for purchase in sample_purchases_data]
        assert all(product.strip() for product in products), "Products should not be empty"
        
        # Check date format (basic check)
        dates = [purchase["date"] for purchase in sample_purchases_data]
        assert all("-" in date for date in dates), "Dates should contain hyphens"


def test_environment_variables():
    """Test environment variable handling."""
    import os
    
    # Test default API base URL
    default_url = os.getenv("API_BASE", "http://localhost:4999")
    assert default_url.startswith("http"), "API base URL should be a valid URL"
    
    # Should be able to override
    os.environ["API_BASE"] = "http://test:8080"
    test_url = os.getenv("API_BASE", "http://localhost:4999")
    assert test_url == "http://test:8080", "Should be able to override API_BASE"
    
    # Clean up
    if "API_BASE" in os.environ:
        del os.environ["API_BASE"]