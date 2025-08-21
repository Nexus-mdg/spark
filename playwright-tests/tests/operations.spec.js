import { test, expect } from '@playwright/test';

test.describe('Operations Page', () => {
  test.beforeEach(async ({ page }) => {
    // Mock authentication if needed
    await page.goto('/operations');
  });

  test('should display operations page layout', async ({ page }) => {
    // Check page title
    await expect(page.locator('h1, h2').first()).toContainText('Operations');
    
    // Check that main sections are visible
    await expect(page.locator('main')).toBeVisible();
    
    // Take screenshot of operations page
    await expect(page).toHaveScreenshot('operations-page.png');
  });

  test('should show dataframe selection interface', async ({ page }) => {
    // Look for dataframe selector
    const dfSelector = page.locator('select, [role="combobox"]').first();
    if (await dfSelector.isVisible()) {
      await expect(dfSelector).toBeVisible();
    }
    
    // Take screenshot of selection interface
    await expect(page).toHaveScreenshot('operations-dataframe-selection.png');
  });

  test('should display operation buttons/controls', async ({ page }) => {
    // Look for operation controls
    const operationButtons = page.locator('button:has-text("Select"), button:has-text("Filter"), button:has-text("Group"), button:has-text("Merge")');
    
    // Take screenshot of operation controls
    await expect(page).toHaveScreenshot('operations-controls.png');
  });

  test('should handle mobile responsive layout', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    
    // Check mobile layout
    await expect(page.locator('main')).toBeVisible();
    
    // Take screenshot of mobile layout
    await expect(page).toHaveScreenshot('operations-mobile.png');
  });
});