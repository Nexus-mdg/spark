import asyncio
import os
import requests
from typing import Optional, Dict, Any
from playwright.async_api import Page, expect
from config import Config

class TestHelpers:
    """Helper utilities for Playwright tests"""
    
    @staticmethod
    async def wait_for_app_ready(page: Page, timeout: int = 30000) -> None:
        """Wait for the application to be ready"""
        try:
            # Wait for the main content to load
            await page.wait_for_selector('main', timeout=timeout)
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            # Wait for any loading indicators to disappear
            loading_selectors = [
                'text=Loading',
                '[data-testid="loading"]',
                '.spinner',
                '[role="progressbar"]'
            ]
            
            for selector in loading_selectors:
                try:
                    await page.wait_for_selector(selector, state='hidden', timeout=2000)
                except:
                    pass  # Selector might not exist, which is fine
                    
        except Exception as e:
            print(f"Warning: App readiness check failed: {e}")
    
    @staticmethod
    async def ensure_dataframes_list_loaded(page: Page) -> None:
        """Ensure the dataframes list is properly loaded"""
        # Wait for the dataframes table or empty state
        try:
            await page.wait_for_selector('table[class*="min-w-full"], text="No cached DataFrames"', timeout=10000)
        except:
            # If table doesn't load, check if there's an error message
            error_elements = await page.query_selector_all('text="Error"')
            if error_elements:
                error_text = await error_elements[0].text_content()
                raise Exception(f"Error loading dataframes: {error_text}")
    
    @staticmethod
    async def upload_sample_dataframe(page: Page, csv_content: str = None) -> str:
        """Upload a sample dataframe for testing purposes"""
        if csv_content is None:
            csv_content = """id,name,age,city
1,Alice,25,New York
2,Bob,30,Los Angeles
3,Charlie,35,Chicago
4,Diana,28,Houston
5,Eve,32,Phoenix"""
        
        # Create a temporary file with CSV content
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_file_path = f.name
        
        try:
            # Find and fill the upload form
            await page.fill('input[placeholder="Optional"]', 'test_dataframe')
            
            # Upload the file
            file_input = await page.query_selector('input[type="file"]')
            if file_input:
                await file_input.set_input_files(temp_file_path)
                
            # Submit the form
            upload_button = await page.query_selector('button:has-text("Upload DataFrame")')
            if upload_button:
                await upload_button.click()
                
            # Wait for upload to complete
            await page.wait_for_selector('text="Upload successful"', timeout=10000)
            
            return 'test_dataframe'
            
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_file_path)
            except:
                pass
    
    @staticmethod
    async def take_screenshot(page: Page, name: str) -> str:
        """Take a screenshot with a given name"""
        screenshot_dir = Config.SCREENSHOT_DIR
        os.makedirs(screenshot_dir, exist_ok=True)
        
        screenshot_path = os.path.join(screenshot_dir, f"{name}.png")
        await page.screenshot(path=screenshot_path)
        return screenshot_path
    
    @staticmethod
    async def wait_for_element_count(page: Page, selector: str, expected_count: int, timeout: int = 5000) -> bool:
        """Wait for a specific number of elements matching the selector"""
        try:
            await page.wait_for_function(
                f"document.querySelectorAll('{selector}').length === {expected_count}",
                timeout=timeout
            )
            return True
        except:
            return False
    
    @staticmethod
    def check_api_health() -> bool:
        """Check if the API is healthy and responsive"""
        try:
            response = requests.get(f"{Config.API_BASE_URL}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    @staticmethod
    async def clear_all_dataframes(page: Page) -> None:
        """Clear all dataframes using the clear cache functionality"""
        try:
            # Look for clear cache button
            clear_button = await page.query_selector('button[title="Clear all cache"]')
            if clear_button:
                await clear_button.click()
                
                # Confirm the dialog
                confirm_button = await page.query_selector('button:has-text("Confirm")')
                if confirm_button:
                    await confirm_button.click()
                    await page.wait_for_selector('text="Cache cleared"', timeout=5000)
        except:
            pass  # If clearing fails, it's not critical for tests