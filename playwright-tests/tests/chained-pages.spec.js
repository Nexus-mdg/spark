import { test, expect } from '@playwright/test';

test.describe('Chained Operations Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/chained-operations');
  });

  test('should display chained operations page layout', async ({ page }) => {
    // Check page header
    await expect(page.locator('h1, h2').first()).toContainText(/Chained.*Operations/i);
    
    // Check main content area
    await expect(page.locator('main')).toBeVisible();
    
    // Take screenshot
    await expect(page).toHaveScreenshot('chained-operations-page.png');
  });

  test('should show operation chain builder', async ({ page }) => {
    // Look for chain building interface
    const chainBuilder = page.locator('.chain, .pipeline, .steps').first();
    
    // Take screenshot of chain builder
    await expect(page).toHaveScreenshot('chained-operations-builder.png');
  });

  test('should display available operations', async ({ page }) => {
    // Look for operation buttons or selectors
    const operations = page.locator('button, select, .operation').first();
    
    // Take screenshot of available operations
    await expect(page).toHaveScreenshot('chained-operations-available.png');
  });
});

test.describe('Chained Pipelines Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/chained-pipelines');
  });

  test('should display chained pipelines page layout', async ({ page }) => {
    // Check page header
    await expect(page.locator('h1, h2').first()).toContainText(/Chained.*Pipelines/i);
    
    // Check main content area
    await expect(page.locator('main')).toBeVisible();
    
    // Take screenshot
    await expect(page).toHaveScreenshot('chained-pipelines-page.png');
  });

  test('should show pipeline management interface', async ({ page }) => {
    // Look for pipeline controls
    const pipelineControls = page.locator('.pipeline, .steps, button').first();
    
    // Take screenshot of pipeline interface
    await expect(page).toHaveScreenshot('chained-pipelines-interface.png');
  });

  test('should handle complex pipeline visualization', async ({ page }) => {
    // Look for pipeline visualization elements
    const visualization = page.locator('.pipeline, .flow, .diagram').first();
    
    // Take screenshot of visualization
    await expect(page).toHaveScreenshot('chained-pipelines-visualization.png');
  });
});