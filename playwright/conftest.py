import pytest
import asyncio
import os
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from config import Config
from utils import TestHelpers

# Configure pytest with playwright
def pytest_configure(config):
    """Configure pytest with playwright settings"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def browser():
    """Launch browser for the test session"""
    async with async_playwright() as p:
        if Config.BROWSER == 'firefox':
            browser = await p.firefox.launch(headless=Config.HEADLESS)
        elif Config.BROWSER == 'webkit':
            browser = await p.webkit.launch(headless=Config.HEADLESS)
        else:  # Default to chromium
            browser = await p.chromium.launch(headless=Config.HEADLESS)
        
        yield browser
        await browser.close()

@pytest.fixture(scope="function")
async def context(browser: Browser):
    """Create a new browser context for each test"""
    context = await browser.new_context(
        viewport={'width': 1280, 'height': 720},
        ignore_https_errors=True,
    )
    yield context
    await context.close()

@pytest.fixture(scope="function")  
async def page(context: BrowserContext):
    """Create a new page for each test"""
    page = await context.new_page()
    
    # Set default timeout
    page.set_default_timeout(Config.TIMEOUT)
    
    # For tests that don't need the application, skip navigation
    # Tests can override this by calling page.goto() themselves
    yield page
    
    # Take screenshot on failure if configured
    if Config.SCREENSHOT_ON_FAILURE:
        import inspect
        test_name = inspect.stack()[1].function
        try:
            screenshot_dir = Config.SCREENSHOT_DIR
            os.makedirs(screenshot_dir, exist_ok=True)
            await page.screenshot(path=f"{screenshot_dir}/failure_{test_name}.png")
        except:
            pass

@pytest.fixture(autouse=True)
def skip_if_no_app():
    """Skip tests that require the app if BASE_URL indicates testing mode"""
    # For infrastructure tests, we don't need to check the app
    pass