import pytest
from playwright.async_api import Page, expect

class TestInfrastructure:
    """Basic infrastructure tests to validate Playwright setup"""
    
    async def test_playwright_working(self, page: Page):
        """Test that Playwright can navigate to a basic page"""
        # Navigate to a basic HTML page with content
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Test Page</title>
        </head>
        <body>
            <h1>Playwright Test Infrastructure</h1>
            <p>This page validates that Playwright is working correctly.</p>
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Playwright</td>
                        <td>Working</td>
                    </tr>
                    <tr>
                        <td>Python</td>
                        <td>Working</td>
                    </tr>
                </tbody>
            </table>
        </body>
        </html>
        """
        
        # Set content directly instead of navigating to external URL
        await page.set_content(html_content)
        
        # Verify the page loaded correctly
        await expect(page).to_have_title("Test Page")
        
        # Check for the heading
        heading = page.locator('h1')
        await expect(heading).to_contain_text("Playwright Test Infrastructure")
        
        # Check for the table
        table = page.locator('table')
        await expect(table).to_be_visible()
        
        # Check table content
        playwright_status = page.locator('td:has-text("Playwright")')
        await expect(playwright_status).to_be_visible()
        
        # Check for working status
        working_status = page.locator('td:has-text("Working")')
        await expect(working_status).to_have_count(2)
    
    async def test_screenshot_capability(self, page: Page):
        """Test that screenshot capability works"""
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Screenshot Test</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .container { max-width: 800px; margin: 0 auto; }
                .success { color: green; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Visual Test Infrastructure</h1>
                <p class="success">✓ Playwright visual testing is configured and working!</p>
                <p>This demonstrates that the Playwright container can:</p>
                <ul>
                    <li>Run Python scripts</li>
                    <li>Load web pages</li>
                    <li>Execute DOM queries</li>
                    <li>Take screenshots</li>
                    <li>Validate page content</li>
                </ul>
            </div>
        </body>
        </html>
        """
        
        await page.set_content(html_content)
        
        # Verify content is visible
        container = page.locator('.container')
        await expect(container).to_be_visible()
        
        success_message = page.locator('.success')
        await expect(success_message).to_contain_text("Playwright visual testing is configured")
        
        # Test that we can take a screenshot
        screenshot_path = await page.screenshot()
        assert screenshot_path is not None
        assert len(screenshot_path) > 0