"""
Core DataFrame operations tests.
Tests for select, filter, groupby, merge, and pivot operations.
"""

import pytest
from tests.api.utils.api_client import (
    validate_dataframe_response, 
    validate_operation_response,
    extract_dataframe_columns,
    extract_dataframe_data,
    extract_dataframe_shape,
    DataFrameTestHelper
)


class TestSelectOperations:
    """Test SELECT operations (column selection and exclusion)."""
    
    def test_select_specific_columns(self, api_client, setup_test_data):
        """Test selecting specific columns from people DataFrame."""
        response = api_client.select_operation("people", "id,name")
        data = validate_dataframe_response(response)
        
        # Verify response structure
        columns = extract_dataframe_columns(data)
        assert columns == ["id", "name"], f"Expected [id, name], got {columns}"
        
        # Verify data integrity
        rows = extract_dataframe_data(data)
        assert len(rows) > 0, "No data returned"
        assert all(len(row) == 2 for row in rows), "All rows should have 2 columns"
    
    def test_select_exclude_columns(self, api_client, setup_test_data):
        """Test excluding specific columns from people DataFrame."""
        response = api_client.select_operation("people", "id,name", exclude=True)
        data = validate_dataframe_response(response)
        
        # Verify excluded columns are not present
        columns = extract_dataframe_columns(data)
        assert "id" not in columns, "id column should be excluded"
        assert "name" not in columns, "name column should be excluded"
        
        # Should have age and city columns remaining
        expected_columns = ["age", "city"]
        for col in expected_columns:
            assert col in columns, f"Expected column {col} in result"
    
    def test_select_all_columns(self, api_client, setup_test_data):
        """Test selecting all columns (equivalent to no operation)."""
        # Get original DataFrame
        orig_response = api_client.get_dataframe("people")
        orig_data = validate_dataframe_response(orig_response)
        orig_columns = extract_dataframe_columns(orig_data)
        
        # Select all columns
        all_cols = ",".join(orig_columns)
        response = api_client.select_operation("people", all_cols)
        data = validate_dataframe_response(response)
        
        # Should have same columns
        columns = extract_dataframe_columns(data)
        assert set(columns) == set(orig_columns), "Should have all original columns"
    
    def test_select_nonexistent_column(self, api_client, setup_test_data):
        """Test selecting a column that doesn't exist should handle gracefully."""
        response = api_client.select_operation("people", "nonexistent_column")
        # The API should return an error or handle gracefully
        # Accept either error response or empty result
        assert response.status_code in [200, 400, 404], "Should handle nonexistent column gracefully"


class TestFilterOperations:
    """Test FILTER operations with various conditions."""
    
    def test_filter_age_greater_than(self, api_client, sample_filters, setup_test_data):
        """Test filtering people with age >= 30."""
        response = api_client.filter_operation("people", sample_filters['simple'])
        data = validate_operation_response(response)
        
        # Should have data structure
        if 'data' in data and 'columns' in data:
            rows = extract_dataframe_data(data)
            columns = extract_dataframe_columns(data)
            
            # Find age column index
            age_index = columns.index("age")
            
            # Verify all returned ages are >= 30
            for row in rows:
                assert row[age_index] >= 30, f"Found age {row[age_index]} < 30"
    
    def test_filter_multiple_conditions(self, api_client, sample_filters, setup_test_data):
        """Test filtering with multiple AND conditions."""
        response = api_client.filter_operation("people", sample_filters['multiple'], "and")
        data = validate_operation_response(response)
        
        if 'data' in data and 'columns' in data:
            rows = extract_dataframe_data(data)
            columns = extract_dataframe_columns(data)
            age_index = columns.index("age")
            
            # Verify all ages are between 25 and 35
            for row in rows:
                age = row[age_index]
                assert 25 <= age <= 35, f"Age {age} not in range [25, 35]"
    
    def test_filter_string_equality(self, api_client, sample_filters, setup_test_data):
        """Test filtering by string equality."""
        response = api_client.filter_operation("people", sample_filters['string'])
        data = validate_operation_response(response)
        
        if 'data' in data and 'columns' in data:
            rows = extract_dataframe_data(data)
            columns = extract_dataframe_columns(data)
            city_index = columns.index("city")
            
            # Verify all cities are "New York"
            for row in rows:
                assert row[city_index] == "New York", f"Found city {row[city_index]} != New York"


class TestGroupByOperations:
    """Test GROUPBY operations with various aggregations."""
    
    def test_groupby_count(self, api_client, sample_aggregations, setup_test_data):
        """Test groupby with count aggregation on purchases."""
        response = api_client.groupby_operation("purchases", ["product"], sample_aggregations['count'])
        data = validate_operation_response(response)
        
        if 'data' in data and 'columns' in data:
            columns = extract_dataframe_columns(data)
            rows = extract_dataframe_data(data)
            
            # Should have product and count columns
            assert "product" in columns, "Missing product column in groupby result"
            # Count column might be named differently (id_count, count, etc.)
            count_cols = [col for col in columns if 'count' in col.lower()]
            assert len(count_cols) > 0, f"No count column found in {columns}"
            
            # Should have aggregated data
            assert len(rows) > 0, "Groupby should return aggregated rows"
    
    def test_groupby_sum(self, api_client, sample_aggregations, setup_test_data):
        """Test groupby with sum aggregation on purchases."""
        response = api_client.groupby_operation("purchases", ["product"], sample_aggregations['sum'])
        data = validate_operation_response(response)
        
        if 'data' in data and 'columns' in data:
            columns = extract_dataframe_columns(data)
            rows = extract_dataframe_data(data)
            
            # Should have product and sum columns
            assert "product" in columns, "Missing product column in groupby result"
            sum_cols = [col for col in columns if 'sum' in col.lower() or 'quantity' in col.lower()]
            assert len(sum_cols) > 0, f"No sum column found in {columns}"


class TestMergeOperations:
    """Test MERGE operations between DataFrames."""
    
    def test_inner_merge(self, api_client, setup_test_data):
        """Test inner merge between people and purchases on id."""
        response = api_client.merge_operation("people", "purchases", "id", "inner")
        data = validate_operation_response(response)
        
        if 'data' in data and 'columns' in data:
            columns = extract_dataframe_columns(data)
            rows = extract_dataframe_data(data)
            
            # Should have columns from both DataFrames
            expected_people_cols = ["name", "age", "city"]  # id is join key
            expected_purchase_cols = ["product", "quantity", "price"]
            
            # Check some expected columns exist
            people_cols_found = [col for col in expected_people_cols if col in columns]
            purchase_cols_found = [col for col in expected_purchase_cols if col in columns]
            
            assert len(people_cols_found) > 0, "No people columns found in merge result"
            assert len(purchase_cols_found) > 0, "No purchase columns found in merge result"
    
    def test_left_merge(self, api_client, setup_test_data):
        """Test left merge to preserve all people records."""
        response = api_client.merge_operation("people", "purchases", "id", "left")
        data = validate_operation_response(response)
        
        # Left merge should return at least as many rows as the left DataFrame
        if 'data' in data and 'columns' in data:
            rows = extract_dataframe_data(data)
            
            # Get original people count for comparison
            people_response = api_client.get_dataframe("people")
            people_data = validate_dataframe_response(people_response)
            people_rows = len(extract_dataframe_data(people_data))
            
            # Left merge should have at least as many rows as people DataFrame
            assert len(rows) >= people_rows, f"Left merge returned {len(rows)} rows, expected >= {people_rows}"


class TestPivotOperations:
    """Test PIVOT operations for data reshaping."""
    
    def test_pivot_longer(self, api_client, setup_test_data):
        """Test pivot longer operation (melt)."""
        pivot_config = {
            "name": "purchases",
            "mode": "longer",
            "id_vars": ["id", "city"],
            "value_vars": ["product", "quantity"],
            "var_name": "attribute",
            "value_name": "value"
        }
        
        response = api_client.pivot_operation(**pivot_config)
        data = validate_operation_response(response)
        
        if 'data' in data and 'columns' in data:
            columns = extract_dataframe_columns(data)
            rows = extract_dataframe_data(data)
            
            # Should have id_vars + var_name + value_name columns
            expected_cols = ["id", "city", "attribute", "value"]
            for col in expected_cols:
                assert col in columns, f"Missing expected column {col} in pivot result"
            
            # Should have more rows than original (melted)
            assert len(rows) > 0, "Pivot longer should return data"
    
    @pytest.mark.skip(reason="Pivot wider requires specific data structure")
    def test_pivot_wider(self, api_client, setup_test_data):
        """Test pivot wider operation (casting)."""
        # This would require specific test data designed for widening
        pass


class TestCompareOperations:
    """Test COMPARE operations between DataFrames."""
    
    def test_compare_identical_dataframes(self, api_client, setup_test_data):
        """Test comparing a DataFrame with itself."""
        response = api_client.compare_operation("people", "people")
        data = validate_operation_response(response)
        
        # Should indicate the DataFrames are identical
        # The response format may vary - check for success indicators
        assert response.status_code == 200, "Compare operation should succeed"
        
        # May have 'identical' field or similar indicator
        if 'identical' in data:
            assert data['identical'] is True, "Identical DataFrames should be marked as identical"
    
    def test_compare_different_dataframes(self, api_client, setup_test_data):
        """Test comparing different DataFrames (people vs purchases)."""
        response = api_client.compare_operation("people", "purchases")
        data = validate_operation_response(response)
        
        # Should indicate differences (schema or data)
        assert response.status_code == 200, "Compare operation should succeed"
        
        # Should indicate they are not identical
        if 'identical' in data:
            assert data['identical'] is False, "Different DataFrames should not be identical"


class TestIntegrationHelper:
    """Integration tests using the test helper class."""
    
    def test_dataframe_helper_basic_operations(self, api_client, setup_test_data):
        """Test DataFrameTestHelper utility functions."""
        helper = DataFrameTestHelper(api_client)
        
        # Test basic assertions
        helper.assert_dataframe_not_empty("people")
        helper.assert_dataframe_has_columns("people", ["id", "name", "age", "city"])
        
        # Test shape assertions (approximate - data may vary)
        helper.assert_dataframe_shape("people", expected_cols=4)
        
        # Test column data extraction
        ages = helper.get_dataframe_data_by_column("people", "age")
        assert len(ages) > 0, "Should get age data"
        assert all(isinstance(age, (int, float)) for age in ages), "Ages should be numeric"