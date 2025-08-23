"""
Test configuration management.
Provides centralized configuration for different test environments.
"""

import os
from typing import Dict, Any


class TestConfig:
    """Central configuration for test environments."""
    
    def __init__(self):
        self.environment = os.getenv('TEST_ENV', 'local')
        self.api_base_url = os.getenv('API_BASE', 'http://localhost:4999')
        self.ui_base_url = os.getenv('UI_BASE', 'http://localhost:5001')
        self.timeout = int(os.getenv('TEST_TIMEOUT', '60'))
        self.parallel_workers = int(os.getenv('TEST_PARALLEL_WORKERS', '2'))
        
    @property
    def is_local(self) -> bool:
        """Check if running in local environment."""
        return self.environment == 'local'
    
    @property
    def is_ci(self) -> bool:
        """Check if running in CI environment."""
        return self.environment in ['ci', 'github-actions']
    
    @property
    def is_docker(self) -> bool:
        """Check if running in Docker environment."""
        return self.environment == 'docker'
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API testing configuration."""
        return {
            'base_url': self.api_base_url,
            'timeout': self.timeout,
            'headers': {
                'User-Agent': 'pytest-api-client',
                'Content-Type': 'application/json'
            }
        }
    
    def get_visual_config(self) -> Dict[str, Any]:
        """Get visual testing configuration."""
        return {
            'base_url': self.ui_base_url,
            'timeout': self.timeout * 1000,  # Playwright uses milliseconds
            'viewport': {'width': 1280, 'height': 720},
            'browser': 'chromium',
            'headless': self.is_ci or self.is_docker
        }
    
    def get_test_data_config(self) -> Dict[str, Any]:
        """Get test data configuration."""
        return {
            'sample_data_dir': os.path.join(
                os.path.dirname(__file__), 
                '../dataframe-api/data/sample'
            ),
            'temp_dir': '/tmp/test_data',
            'cleanup_after_tests': True
        }


# Global test configuration instance
test_config = TestConfig()


def get_config() -> TestConfig:
    """Get the global test configuration."""
    return test_config


def configure_for_environment(env: str) -> None:
    """Configure tests for specific environment."""
    os.environ['TEST_ENV'] = env
    global test_config
    test_config = TestConfig()