"""
Sample data fixtures for testing.
Provides predefined test data for consistent testing across different test scenarios.
"""

import pytest
import tempfile
import os
import csv
import json
from typing import Dict, List, Any


@pytest.fixture
def sample_people_data() -> List[Dict[str, Any]]:
    """Sample people data matching the format of people.csv."""
    return [
        {"id": 1, "name": "Alice Johnson", "age": 28, "city": "New York"},
        {"id": 2, "name": "Bob Smith", "age": 34, "city": "Los Angeles"},
        {"id": 3, "name": "Carol Davis", "age": 22, "city": "Chicago"},
        {"id": 4, "name": "David Wilson", "age": 41, "city": "Houston"},
        {"id": 5, "name": "Emma Brown", "age": 29, "city": "Phoenix"},
        {"id": 6, "name": "Frank Miller", "age": 35, "city": "Philadelphia"},
        {"id": 7, "name": "Grace Lee", "age": 26, "city": "San Antonio"},
        {"id": 8, "name": "Henry Taylor", "age": 31, "city": "San Diego"},
        {"id": 9, "name": "Ivy Chen", "age": 24, "city": "Dallas"},
        {"id": 10, "name": "Jack Anderson", "age": 38, "city": "San Jose"}
    ]


@pytest.fixture
def sample_purchases_data() -> List[Dict[str, Any]]:
    """Sample purchases data for testing merge operations."""
    return [
        {"id": 1, "product": "Laptop", "quantity": 1, "price": 999.99, "city": "New York", "date": "2023-01-15"},
        {"id": 2, "product": "Mouse", "quantity": 2, "price": 25.50, "city": "Los Angeles", "date": "2023-01-16"},
        {"id": 3, "product": "Keyboard", "quantity": 1, "price": 75.00, "city": "Chicago", "date": "2023-01-17"},
        {"id": 1, "product": "Monitor", "quantity": 2, "price": 299.99, "city": "New York", "date": "2023-01-18"},
        {"id": 4, "product": "Webcam", "quantity": 1, "price": 89.99, "city": "Houston", "date": "2023-01-19"},
        {"id": 2, "product": "Headphones", "quantity": 1, "price": 150.00, "city": "Los Angeles", "date": "2023-01-20"},
        {"id": 5, "product": "Tablet", "quantity": 1, "price": 499.99, "city": "Phoenix", "date": "2023-01-21"},
        {"id": 3, "product": "Phone", "quantity": 1, "price": 799.99, "city": "Chicago", "date": "2023-01-22"}
    ]


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file for testing uploads."""
    def _create_temp_csv(data: List[Dict[str, Any]], filename: str = None) -> str:
        """Create a temporary CSV file with the given data."""
        fd, path = tempfile.mkstemp(suffix='.csv', prefix=filename or 'test_')
        
        try:
            with os.fdopen(fd, 'w', newline='') as tmp_file:
                if data:
                    fieldnames = data[0].keys()
                    writer = csv.DictWriter(tmp_file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
            return path
        except Exception:
            os.unlink(path)
            raise
    
    created_files = []
    
    def cleanup():
        for file_path in created_files:
            try:
                os.unlink(file_path)
            except OSError:
                pass
    
    def create_csv(data: List[Dict[str, Any]], filename: str = None) -> str:
        path = _create_temp_csv(data, filename)
        created_files.append(path)
        return path
    
    create_csv.cleanup = cleanup
    yield create_csv
    cleanup()


@pytest.fixture
def temp_json_file():
    """Create a temporary JSON file for testing uploads."""
    def _create_temp_json(data: Any, filename: str = None) -> str:
        """Create a temporary JSON file with the given data."""
        fd, path = tempfile.mkstemp(suffix='.json', prefix=filename or 'test_')
        
        try:
            with os.fdopen(fd, 'w') as tmp_file:
                json.dump(data, tmp_file, indent=2)
            return path
        except Exception:
            os.unlink(path)
            raise
    
    created_files = []
    
    def cleanup():
        for file_path in created_files:
            try:
                os.unlink(file_path)
            except OSError:
                pass
    
    def create_json(data: Any, filename: str = None) -> str:
        path = _create_temp_json(data, filename)
        created_files.append(path)
        return path
    
    create_json.cleanup = cleanup
    yield create_json
    cleanup()


@pytest.fixture
def sample_filter_test_cases():
    """Comprehensive filter test cases for different scenarios."""
    return {
        'age_filters': {
            'gte_30': [{'col': 'age', 'op': 'gte', 'value': 30}],
            'lt_25': [{'col': 'age', 'op': 'lt', 'value': 25}],
            'eq_28': [{'col': 'age', 'op': 'eq', 'value': 28}],
            'between_25_35': [
                {'col': 'age', 'op': 'gte', 'value': 25},
                {'col': 'age', 'op': 'lte', 'value': 35}
            ]
        },
        'city_filters': {
            'new_york': [{'col': 'city', 'op': 'eq', 'value': 'New York'}],
            'not_chicago': [{'col': 'city', 'op': 'ne', 'value': 'Chicago'}],
            'contains_san': [{'col': 'city', 'op': 'contains', 'value': 'San'}]
        },
        'name_filters': {
            'starts_with_a': [{'col': 'name', 'op': 'startswith', 'value': 'A'}],
            'ends_with_son': [{'col': 'name', 'op': 'endswith', 'value': 'son'}]
        },
        'complex_filters': {
            'young_new_yorkers': [
                {'col': 'age', 'op': 'lt', 'value': 30},
                {'col': 'city', 'op': 'eq', 'value': 'New York'}
            ],
            'older_west_coast': [
                {'col': 'age', 'op': 'gte', 'value': 30},
                {'col': 'city', 'op': 'in', 'value': ['Los Angeles', 'San Diego', 'San Jose']}
            ]
        }
    }


@pytest.fixture
def sample_groupby_test_cases():
    """Comprehensive groupby test cases for different scenarios."""
    return {
        'single_column': {
            'by_city': {
                'by': ['city'],
                'agg': {'id': 'count', 'age': 'avg'}
            },
            'by_age_range': {
                'by': ['age'],
                'agg': {'id': 'count'}
            }
        },
        'multiple_columns': {
            'by_city_and_age_group': {
                'by': ['city', 'age'],
                'agg': {'id': 'count', 'name': 'first'}
            }
        },
        'aggregation_types': {
            'count_only': {'id': 'count'},
            'avg_age': {'age': 'avg'},
            'min_max_age': {'age': 'min'},
            'multiple_aggs': {'id': 'count', 'age': 'avg'}
        }
    }


@pytest.fixture  
def sample_merge_test_cases():
    """Comprehensive merge test cases for different scenarios."""
    return {
        'join_types': ['inner', 'left', 'right', 'outer'],
        'join_keys': {
            'single_key': 'id',
            'multiple_keys': 'id,city'  # if both DataFrames have city
        },
        'expected_results': {
            'inner': 'should_have_matching_records_only',
            'left': 'should_preserve_all_left_records',
            'right': 'should_preserve_all_right_records', 
            'outer': 'should_have_all_records_from_both'
        }
    }


@pytest.fixture
def sample_pivot_test_cases():
    """Comprehensive pivot test cases for different scenarios.""" 
    return {
        'longer': {
            'basic_melt': {
                'id_vars': ['id', 'city'],
                'value_vars': ['product', 'quantity'],
                'var_name': 'attribute',
                'value_name': 'value'
            },
            'melt_with_date': {
                'id_vars': ['id', 'date'],
                'value_vars': ['product', 'price', 'quantity'],
                'var_name': 'measure',
                'value_name': 'amount'
            }
        },
        'wider': {
            'cast_by_product': {
                'index': ['id'],
                'columns': ['product'],
                'values': ['quantity']
            }
        }
    }


@pytest.fixture
def sample_mutate_expressions():
    """Sample expressions for mutate operations."""
    return {
        'simple_math': {
            'total_value': "col('quantity') * col('price')",
            'price_per_unit': "col('price') / col('quantity')",
            'age_plus_ten': "col('age') + 10"
        },
        'string_operations': {
            'full_location': "concat(col('city'), ', USA')",
            'name_upper': "upper(col('name'))",
            'name_length': "length(col('name'))"
        },
        'conditional': {
            'age_category': "when(col('age') < 30, 'Young').when(col('age') < 40, 'Middle').otherwise('Senior')",
            'high_value': "when(col('price') > 500, 'Expensive').otherwise('Affordable')"
        },
        'date_operations': {
            'year_from_date': "year(col('date'))",
            'month_from_date': "month(col('date'))",
            'days_since_epoch': "datediff(col('date'), '1970-01-01')"
        }
    }


@pytest.fixture
def expected_data_shapes():
    """Expected data shapes for validation."""
    return {
        'people': {'rows': 10, 'cols': 4},
        'purchases': {'rows': 8, 'cols': 6},
        'select_two_cols': {'cols': 2},
        'filter_age_gte_30': {'rows_max': 10},  # <= original count
        'groupby_by_city': {'rows_max': 10},  # <= number of unique cities
        'merge_inner': {'cols_min': 6}  # at least cols from both tables minus join key
    }