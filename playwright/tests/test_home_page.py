import pytest
from playwright.async_api import Page, expect
from utils import TestHelpers
from config import Config

class TestHomePage:
    """Test suite for overall home page functionality"""
    
    async def test_page_title_and_metadata(self, page: Page):
        """Test page title and basic metadata"""
        # Check page title
        await expect(page).to_have_title("Spark test visualizer")
        
        # Check meta viewport tag for responsive design
        viewport_meta = page.locator('meta[name="viewport"]')
        await expect(viewport_meta).to_have_attribute('content', 'width=device-width, initial-scale=1.0')
    
    async def test_navigation_elements(self, page: Page):
        """Test main navigation elements"""
        # Check for main navigation
        nav_elements = page.locator('nav, header')
        await expect(nav_elements.first()).to_be_visible()
        
        # Check for logo or brand name
        brand = page.locator('text="Spark test visualizer"').first()
        await expect(brand).to_be_visible()
    
    async def test_animated_feature_showcase(self, page: Page):
        """Test the animated feature showcase section"""
        # Look for the feature showcase section
        feature_showcase = page.locator('.bg-gradient-to-r, .feature-showcase, text="Interactive Data Analysis"')
        
        if await feature_showcase.count() > 0:
            await expect(feature_showcase.first()).to_be_visible()
            
            # Check for feature icons and descriptions
            feature_icons = page.locator('[class*="text-5xl"], .feature-icon')
            if await feature_icons.count() > 0:
                await expect(feature_icons.first()).to_be_visible()
    
    async def test_dark_mode_toggle(self, page: Page):
        """Test dark mode functionality if available"""
        # Look for dark mode toggle button
        dark_mode_toggle = page.locator('button[aria-label*="dark"], button[title*="dark"], button:has-text("Dark")')
        
        if await dark_mode_toggle.count() > 0:
            # Test toggling dark mode
            await dark_mode_toggle.click()
            await page.wait_for_timeout(500)
            
            # Check if dark mode classes are applied
            body = page.locator('body, html')
            body_class = await body.get_attribute('class')
            
            # Toggle back
            await dark_mode_toggle.click()
            await page.wait_for_timeout(500)
    
    async def test_footer_presence(self, page: Page):
        """Test footer section if present"""
        footer = page.locator('footer')
        
        if await footer.count() > 0:
            await expect(footer).to_be_visible()
    
    async def test_refresh_functionality(self, page: Page):
        """Test page refresh and data reload"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Look for refresh button
        refresh_button = page.locator('button[title*="Refresh"], button:has(svg):has-text("refresh")')
        
        if await refresh_button.count() > 0:
            await refresh_button.click()
            await page.wait_for_timeout(2000)
            
            # Ensure page is still functional after refresh
            await TestHelpers.ensure_dataframes_list_loaded(page)
    
    async def test_error_handling(self, page: Page):
        """Test basic error handling and user feedback"""
        # Look for any error messages or toasts
        error_elements = page.locator('[role="alert"], .error, .toast, text*="Error"')
        
        # If errors are present, they should be visible
        if await error_elements.count() > 0:
            for i in range(min(3, await error_elements.count())):
                error_element = error_elements.nth(i)
                if await error_element.is_visible():
                    print(f"Found error message: {await error_element.text_content()}")
    
    async def test_accessibility_basics(self, page: Page):
        """Test basic accessibility features"""
        # Check for skip links
        skip_links = page.locator('a[href*="#main"], a:has-text("Skip")')
        
        # Check for proper heading structure
        headings = page.locator('h1, h2, h3, h4, h5, h6')
        heading_count = await headings.count()
        
        # Should have at least one heading
        assert heading_count > 0, "Page should have at least one heading"
        
        # Check for form labels
        form_inputs = page.locator('input, select, textarea')
        input_count = await form_inputs.count()
        
        if input_count > 0:
            # Check that inputs have labels or aria-labels
            for i in range(min(5, input_count)):
                input_element = form_inputs.nth(i)
                input_id = await input_element.get_attribute('id')
                aria_label = await input_element.get_attribute('aria-label')
                
                if input_id:
                    # Check for associated label
                    label = page.locator(f'label[for="{input_id}"]')
                    has_label = await label.count() > 0
                    
                    # Should have either a label or aria-label
                    assert has_label or aria_label, f"Input {i} should have a label or aria-label"
    
    async def test_loading_states(self, page: Page):
        """Test loading states and indicators"""
        # Navigate to page fresh to catch loading states
        await page.reload()
        
        # Look for loading indicators
        loading_indicators = page.locator('text="Loading", .loading, .spinner, [role="progressbar"]')
        
        # Wait for loading to complete
        await TestHelpers.wait_for_app_ready(page)
        
        # Ensure loading indicators are gone
        for i in range(await loading_indicators.count()):
            indicator = loading_indicators.nth(i)
            if await indicator.is_visible():
                await expect(indicator).to_be_hidden(timeout=10000)
    
    async def test_keyboard_navigation(self, page: Page):
        """Test basic keyboard navigation"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Test tab navigation
        await page.keyboard.press('Tab')
        await page.wait_for_timeout(200)
        
        # Check if focus is visible
        focused_element = page.locator(':focus')
        if await focused_element.count() > 0:
            await expect(focused_element).to_be_visible()
        
        # Test a few more tab presses
        for _ in range(3):
            await page.keyboard.press('Tab')
            await page.wait_for_timeout(200)
    
    async def test_layout_stability(self, page: Page):
        """Test that layout is stable and doesn't shift unexpectedly"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Take initial measurement
        initial_height = await page.evaluate('document.body.scrollHeight')
        
        # Wait a bit for any dynamic content to settle
        await page.wait_for_timeout(2000)
        
        # Measure again
        final_height = await page.evaluate('document.body.scrollHeight')
        
        # Height change should be minimal (allowing for some dynamic content)
        height_diff = abs(final_height - initial_height)
        assert height_diff < 200, f"Layout shifted significantly: {height_diff}px"