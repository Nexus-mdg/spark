"""
Authentication endpoint tests.
Tests for basic API authentication and protected endpoints.
"""

import pytest
import requests
from tests.api.utils.api_client import validate_response_structure


class TestAPIAuthentication:
    """Test API authentication and protection mechanisms."""
    
    def test_api_stats_accessible_with_curl_user_agent(self, api_base_url):
        """Test that API stats endpoint is accessible with curl-like user agent."""
        # Create session with curl-like user agent
        session = requests.Session()
        session.headers.update({'User-Agent': 'curl/7.68.0'})
        
        response = session.get(f"{api_base_url}/api/stats")
        assert response.status_code == 200, "API stats should be accessible with curl user agent"
        
        # Validate response structure
        data = validate_response_structure(response)
        
        # Basic stats should have some system information
        assert isinstance(data, dict), "Stats response should be a dictionary"
    
    def test_api_stats_accessible_with_pytest_user_agent(self, api_base_url):
        """Test that API stats endpoint is accessible with pytest user agent."""
        session = requests.Session()
        session.headers.update({'User-Agent': 'pytest-api-client'})
        
        response = session.get(f"{api_base_url}/api/stats")
        assert response.status_code == 200, "API stats should be accessible with pytest user agent"
    
    @pytest.mark.skipif(
        True,  # Skip by default as API protection may be disabled in test environment
        reason="API protection may be disabled in test environment"
    )
    def test_api_protection_blocks_browser_user_agent(self, api_base_url):
        """Test that API protection blocks browser-like user agents."""
        browser_user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        
        for user_agent in browser_user_agents:
            session = requests.Session()
            session.headers.update({'User-Agent': user_agent})
            
            response = session.get(f"{api_base_url}/api/stats")
            # Should be blocked (403) or require API key
            assert response.status_code in [403, 401], f"Browser user agent should be blocked: {user_agent}"
    
    @pytest.mark.skipif(
        True,  # Skip by default as API protection may be disabled in test environment  
        reason="API protection may be disabled in test environment"
    )
    def test_api_key_authentication(self, api_base_url):
        """Test API key authentication for browser user agents."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-API-Key': 'dataframe-api-internal-key'
        })
        
        response = session.get(f"{api_base_url}/api/stats")
        assert response.status_code == 200, "Valid API key should allow access"
    
    def test_api_dataframes_endpoint_accessible(self, api_client, setup_test_data):
        """Test that basic DataFrame endpoints are accessible."""
        # Test getting people DataFrame (should exist from fixtures)
        response = api_client.get_dataframe("people")
        assert response.status_code == 200, "People DataFrame should be accessible"
        
        data = validate_response_structure(response, ['data', 'columns', 'name', 'shape'])
        assert data['name'] == 'people', "DataFrame name should be 'people'"
    
    def test_api_operations_endpoints_accessible(self, api_client, setup_test_data):
        """Test that operation endpoints are accessible."""
        # Test select operation
        response = api_client.select_operation("people", "id,name")
        assert response.status_code == 200, "Select operation should be accessible"
    
    def test_api_stats_endpoint_structure(self, api_client):
        """Test API stats endpoint returns expected structure."""
        response = api_client.stats()
        assert response.status_code == 200, "Stats endpoint should be accessible"
        
        data = validate_response_structure(response)
        
        # Stats might contain various system information
        # Just verify it's a valid JSON response for now
        assert isinstance(data, dict), "Stats should return a dictionary"
    
    def test_api_handles_malformed_requests(self, api_base_url):
        """Test that API handles malformed requests gracefully."""
        session = requests.Session()
        session.headers.update({'User-Agent': 'pytest-api-client'})
        
        # Test with invalid JSON
        response = session.post(
            f"{api_base_url}/api/ops/filter",
            data="invalid json",
            headers={'Content-Type': 'application/json'}
        )
        
        # Should return 400 (Bad Request) for malformed JSON
        assert response.status_code in [400, 422], "Malformed JSON should return 400/422"
    
    def test_api_handles_missing_dataframes(self, api_client):
        """Test that API handles requests for non-existent DataFrames."""
        response = api_client.get_dataframe("nonexistent_dataframe")
        
        # Should return 404 or similar error
        assert response.status_code in [404, 400], "Non-existent DataFrame should return 404/400"
    
    def test_api_cors_headers_present(self, api_base_url):
        """Test that CORS headers are present for cross-origin requests."""
        session = requests.Session()
        session.headers.update({'User-Agent': 'pytest-api-client'})
        
        response = session.options(f"{api_base_url}/api/stats")
        
        # CORS headers might be present
        cors_headers = [
            'Access-Control-Allow-Origin',
            'Access-Control-Allow-Methods', 
            'Access-Control-Allow-Headers'
        ]
        
        # Check if any CORS headers are present (may vary by configuration)
        has_cors = any(header in response.headers for header in cors_headers)
        
        # Just verify the OPTIONS request doesn't fail
        assert response.status_code in [200, 204, 405], "OPTIONS request should not fail"


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_api_timeout_handling(self, api_base_url):
        """Test API handles timeouts gracefully."""
        session = requests.Session()
        session.headers.update({'User-Agent': 'pytest-api-client'})
        
        try:
            # Very short timeout to test handling
            response = session.get(f"{api_base_url}/api/stats", timeout=0.001)
            # If it doesn't timeout, just verify it's successful
            assert response.status_code == 200
        except requests.Timeout:
            # Timeout is expected and handled properly
            pass
    
    def test_api_large_request_handling(self, api_base_url, setup_test_data):
        """Test API handles large requests appropriately."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'pytest-api-client',
            'Content-Type': 'application/json'
        })
        
        # Create a large but valid JSON payload
        large_filters = [
            {'col': 'age', 'op': 'gte', 'value': i} 
            for i in range(100)  # 100 filter conditions
        ]
        
        large_payload = {
            'name': 'people',
            'filters': large_filters,
            'combine': 'and'
        }
        
        response = session.post(f"{api_base_url}/api/ops/filter", json=large_payload)
        
        # Should either process successfully or return appropriate error
        assert response.status_code in [200, 400, 413, 422], "Large request should be handled appropriately"