"""
API client utilities for DataFrame operations testing.
Provides helper functions and classes for common API interactions.
"""

import requests
from typing import Dict, Any, List, Optional
import json


class DataFrameAPIError(Exception):
    """Custom exception for DataFrame API errors."""
    pass


def validate_response_structure(response: requests.Response, expected_keys: List[str] = None) -> Dict[str, Any]:
    """
    Validate response has expected structure and return parsed JSON.
    
    Args:
        response: HTTP response object
        expected_keys: List of keys that should be present in response
        
    Returns:
        Parsed JSON response
        
    Raises:
        DataFrameAPIError: If response is invalid
    """
    if not response.ok:
        raise DataFrameAPIError(f"HTTP {response.status_code}: {response.text}")
    
    try:
        data = response.json()
    except json.JSONDecodeError:
        raise DataFrameAPIError(f"Invalid JSON response: {response.text}")
    
    if expected_keys:
        missing_keys = [key for key in expected_keys if key not in data]
        if missing_keys:
            raise DataFrameAPIError(f"Missing keys in response: {missing_keys}")
    
    return data


def validate_dataframe_response(response: requests.Response) -> Dict[str, Any]:
    """
    Validate response contains DataFrame structure.
    
    Expected structure:
    {
        "data": [...],
        "columns": [...],
        "name": "...",
        "shape": [rows, cols]
    }
    """
    return validate_response_structure(response, ['data', 'columns', 'name', 'shape'])


def validate_operation_response(response: requests.Response) -> Dict[str, Any]:
    """
    Validate response from DataFrame operation.
    
    Operations may return either:
    - DataFrame result (data, columns, name, shape)
    - Operation result (result, status, etc.)
    """
    data = validate_response_structure(response)
    
    # Check if it's a DataFrame response
    if all(key in data for key in ['data', 'columns', 'name', 'shape']):
        return data
    
    # Check if it's an operation status response
    if 'result' in data or 'status' in data:
        return data
    
    # For other valid JSON responses, just return the data
    return data


def extract_dataframe_data(response_data: Dict[str, Any]) -> List[List[Any]]:
    """Extract the data rows from a DataFrame response."""
    if 'data' not in response_data:
        raise DataFrameAPIError("Response does not contain data field")
    return response_data['data']


def extract_dataframe_columns(response_data: Dict[str, Any]) -> List[str]:
    """Extract the column names from a DataFrame response.""" 
    if 'columns' not in response_data:
        raise DataFrameAPIError("Response does not contain columns field")
    return response_data['columns']


def extract_dataframe_shape(response_data: Dict[str, Any]) -> tuple:
    """Extract the shape (rows, cols) from a DataFrame response."""
    if 'shape' not in response_data:
        raise DataFrameAPIError("Response does not contain shape field")
    shape = response_data['shape']
    if not isinstance(shape, list) or len(shape) != 2:
        raise DataFrameAPIError(f"Invalid shape format: {shape}")
    return tuple(shape)


class DataFrameTestHelper:
    """Helper class for common DataFrame test operations."""
    
    def __init__(self, api_client):
        self.api_client = api_client
    
    def assert_dataframe_not_empty(self, df_name: str):
        """Assert that a DataFrame exists and is not empty."""
        response = self.api_client.get_dataframe(df_name)
        data = validate_dataframe_response(response)
        shape = extract_dataframe_shape(data)
        assert shape[0] > 0, f"DataFrame {df_name} is empty"
        assert shape[1] > 0, f"DataFrame {df_name} has no columns"
    
    def assert_dataframe_has_columns(self, df_name: str, expected_columns: List[str]):
        """Assert that a DataFrame has the expected columns."""
        response = self.api_client.get_dataframe(df_name)
        data = validate_dataframe_response(response)
        columns = extract_dataframe_columns(data)
        
        for col in expected_columns:
            assert col in columns, f"Expected column '{col}' not found in {df_name}"
    
    def assert_dataframe_shape(self, df_name: str, expected_rows: int = None, expected_cols: int = None):
        """Assert that a DataFrame has the expected shape."""
        response = self.api_client.get_dataframe(df_name)
        data = validate_dataframe_response(response)
        shape = extract_dataframe_shape(data)
        
        if expected_rows is not None:
            assert shape[0] == expected_rows, f"Expected {expected_rows} rows, got {shape[0]}"
        
        if expected_cols is not None:
            assert shape[1] == expected_cols, f"Expected {expected_cols} columns, got {shape[1]}"
    
    def get_dataframe_data_by_column(self, df_name: str, column: str) -> List[Any]:
        """Get all values from a specific column."""
        response = self.api_client.get_dataframe(df_name)
        data = validate_dataframe_response(response)
        
        columns = extract_dataframe_columns(data)
        if column not in columns:
            raise DataFrameAPIError(f"Column '{column}' not found in {df_name}")
        
        col_index = columns.index(column)
        rows = extract_dataframe_data(data)
        
        return [row[col_index] for row in rows]