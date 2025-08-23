import pytest
from playwright.async_api import Page, expect
from utils import TestHelpers
from config import Config

class TestDataframesList:
    """Test suite for dataframes list functionality on home page"""
    
    async def test_home_page_loads(self, page: Page):
        """Test that the home page loads successfully"""
        # Check that the main title is present
        await expect(page.locator('h1, h2')).to_contain_text('Spark test visualizer')
        
        # Check that the main navigation/header is present
        header = page.locator('header, nav, [role="banner"]')
        await expect(header).to_be_visible()
        
        # Check that the main content area is present
        main_content = page.locator('main')
        await expect(main_content).to_be_visible()
    
    async def test_dataframes_table_structure(self, page: Page):
        """Test that the dataframes table structure is correct"""
        # Wait for dataframes section to load
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Check if the "Cached DataFrames" section exists
        section_header = page.locator('text="Cached DataFrames"')
        await expect(section_header).to_be_visible()
        
        # Check table headers are present (even if no data)
        table_headers = ['Name', 'Description', 'Dimensions', 'Size', 'Created', 'Actions']
        
        for header in table_headers:
            header_element = page.locator(f'th:has-text("{header}")')
            await expect(header_element).to_be_visible()
    
    async def test_empty_state_display(self, page: Page):
        """Test that empty state is displayed correctly when no dataframes exist"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Clear any existing dataframes first
        await TestHelpers.clear_all_dataframes(page)
        
        # Check for empty state message
        empty_message = page.locator('text="No cached DataFrames"')
        await expect(empty_message).to_be_visible(timeout=10000)
    
    async def test_dataframe_upload_form(self, page: Page):
        """Test that the dataframe upload form is present and functional"""
        # Check upload form section
        upload_section = page.locator('text="Upload DataFrame"')
        await expect(upload_section).to_be_visible()
        
        # Check form elements
        name_input = page.locator('input[placeholder*="Optional"]').first()
        await expect(name_input).to_be_visible()
        
        file_input = page.locator('input[type="file"]')
        await expect(file_input).to_be_visible()
        
        upload_button = page.locator('button:has-text("Upload DataFrame")')
        await expect(upload_button).to_be_visible()
        
        # Check dataframe type options
        static_option = page.locator('text="Static"')
        await expect(static_option).to_be_visible()
        
        ephemeral_option = page.locator('text="Ephemeral"')
        await expect(ephemeral_option).to_be_visible()
        
        temporary_option = page.locator('text="Temporary"')
        await expect(temporary_option).to_be_visible()
    
    @pytest.mark.slow
    async def test_dataframe_upload_and_display(self, page: Page):
        """Test uploading a dataframe and verifying it appears in the list"""
        # Clear existing dataframes first
        await TestHelpers.clear_all_dataframes(page)
        
        # Upload a sample dataframe
        dataframe_name = await TestHelpers.upload_sample_dataframe(page)
        
        # Wait for the page to refresh and show the new dataframe
        await page.wait_for_timeout(2000)
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Check that the dataframe appears in the table
        dataframe_link = page.locator(f'text="{dataframe_name}"')
        await expect(dataframe_link).to_be_visible(timeout=10000)
        
        # Check that row count and columns are displayed
        dimensions_cell = page.locator('td:has-text(" x ")')
        await expect(dimensions_cell).to_be_visible()
        
        # Check that actions are available
        preview_button = page.locator('button[title="Preview"]')
        await expect(preview_button).to_be_visible()
        
        delete_button = page.locator('button[title="Delete"]')
        await expect(delete_button).to_be_visible()
    
    async def test_pagination_controls(self, page: Page):
        """Test pagination controls when they should be visible"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Check if pagination exists (it may not if there are few items)
        pagination_area = page.locator('.pagination, [role="navigation"]:has-text("Showing")')
        
        # If pagination exists, test the elements
        if await pagination_area.count() > 0:
            # Check for page indicators
            showing_text = page.locator('text*="Showing"')
            await expect(showing_text).to_be_visible()
            
            # Check for navigation buttons (they may be disabled)
            next_button = page.locator('button:has-text("Next")')
            prev_button = page.locator('button:has-text("Previous"), button:has-text("Prev")')
            
            if await next_button.count() > 0:
                await expect(next_button).to_be_visible()
            if await prev_button.count() > 0:
                await expect(prev_button).to_be_visible()
    
    async def test_filter_functionality(self, page: Page):
        """Test the dataframes filter/search functionality"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Look for the filter input
        filter_input = page.locator('input[placeholder*="Search"], input[placeholder*="Filter"]')
        
        if await filter_input.count() > 0:
            await expect(filter_input).to_be_visible()
            
            # Test typing in the filter
            await filter_input.fill('test')
            await page.wait_for_timeout(1000)  # Wait for filter to apply
            
            # Clear the filter
            await filter_input.clear()
            await page.wait_for_timeout(1000)
    
    async def test_statistics_display(self, page: Page):
        """Test that statistics cards are displayed correctly"""
        # Look for statistics/stats section
        stats_elements = [
            'text="DataFrames"',
            'text="MB Total"', 
            'text="Total Rows"',
            'text="Max Columns"'
        ]
        
        for stat_text in stats_elements:
            stat_element = page.locator(stat_text)
            await expect(stat_element).to_be_visible()
    
    async def test_charts_section(self, page: Page):
        """Test that charts section is present (may be hidden if no data)"""
        # Charts may only appear when there's data, so this is a basic structure test
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Look for potential chart containers (they may be hidden if no data)
        chart_sections = page.locator('canvas, .chart, [role="img"]')
        
        # If charts exist, they should be in the viewport
        chart_count = await chart_sections.count()
        if chart_count > 0:
            # At least one chart should be visible
            first_chart = chart_sections.first()
            await expect(first_chart).to_be_visible()
    
    async def test_responsive_layout(self, page: Page):
        """Test responsive layout at different screen sizes"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Test mobile viewport
        await page.set_viewport_size({'width': 375, 'height': 667})
        await page.wait_for_timeout(1000)
        
        # Main content should still be visible
        main_content = page.locator('main')
        await expect(main_content).to_be_visible()
        
        # Test tablet viewport
        await page.set_viewport_size({'width': 768, 'height': 1024})
        await page.wait_for_timeout(1000)
        
        # Test desktop viewport
        await page.set_viewport_size({'width': 1280, 'height': 720})
        await page.wait_for_timeout(1000)
        
        # Ensure layout is still functional
        await expect(main_content).to_be_visible()
    
    async def test_table_sorting(self, page: Page):
        """Test table sorting functionality"""
        await TestHelpers.ensure_dataframes_list_loaded(page)
        
        # Look for sortable column headers
        sortable_headers = page.locator('th button, th[role="button"], th:has(svg)')
        
        if await sortable_headers.count() > 0:
            # Click on the first sortable header
            first_header = sortable_headers.first()
            await first_header.click()
            await page.wait_for_timeout(500)
            
            # Click again to test reverse sorting
            await first_header.click()
            await page.wait_for_timeout(500)