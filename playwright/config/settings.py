import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for Playwright tests"""
    
    # Application URLs
    BASE_URL = os.getenv('BASE_URL', 'http://dataframe-ui-x:5001')
    API_BASE_URL = os.getenv('API_BASE_URL', 'http://dataframe-api:4999')
    
    # Test settings
    TIMEOUT = int(os.getenv('TIMEOUT', '30000'))  # 30 seconds
    SLOW_TIMEOUT = int(os.getenv('SLOW_TIMEOUT', '60000'))  # 60 seconds
    
    # Browser settings
    HEADLESS = os.getenv('HEADLESS', 'true').lower() == 'true'
    BROWSER = os.getenv('BROWSER', 'chromium')  # chromium, firefox, webkit
    
    # Screenshot settings
    SCREENSHOT_ON_FAILURE = os.getenv('SCREENSHOT_ON_FAILURE', 'true').lower() == 'true'
    SCREENSHOT_DIR = os.getenv('SCREENSHOT_DIR', 'screenshots')
    
    # Test data
    SAMPLE_CSV_PATH = os.getenv('SAMPLE_CSV_PATH', '/app/test-data/sample.csv')
    
    # Authentication (if enabled)
    TEST_USERNAME = os.getenv('TEST_USERNAME', 'admin')
    TEST_PASSWORD = os.getenv('TEST_PASSWORD', '')
    
    # Waits and delays
    DEFAULT_WAIT = int(os.getenv('DEFAULT_WAIT', '5000'))  # 5 seconds
    NAVIGATION_WAIT = int(os.getenv('NAVIGATION_WAIT', '3000'))  # 3 seconds