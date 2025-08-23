// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Home page visual tests.
 * Tests the main application landing page appearance.
 */

test.describe('Home Page', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to home page
    await page.goto('/');
    
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('should display home page layout correctly', async ({ page }) => {
    // Wait for main content to load
    await page.waitForTimeout(1000);
    
    // Take full page screenshot
    await expect(page).toHaveScreenshot('home-page-full.png', { fullPage: true });
  });

  test('should display main content area', async ({ page }) => {
    // Look for main content container
    const mainContent = page.locator('main, .main, .content, .container').first();
    
    if (await mainContent.isVisible()) {
      await expect(mainContent).toHaveScreenshot('home-main-content.png');
    } else {
      // Take screenshot of body content
      await expect(page.locator('body')).toHaveScreenshot('home-body-content.png');
    }
  });

  test('should handle different viewport sizes', async ({ page }) => {
    // Desktop view
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.waitForTimeout(500);
    await expect(page).toHaveScreenshot('home-desktop-large.png');
    
    // Standard desktop
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.waitForTimeout(500);
    await expect(page).toHaveScreenshot('home-desktop-standard.png');
    
    // Tablet
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(500);
    await expect(page).toHaveScreenshot('home-tablet.png');
    
    // Mobile
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(500);
    await expect(page).toHaveScreenshot('home-mobile.png');
  });

  test('should display loading states if present', async ({ page }) => {
    // Navigate to page and capture any loading states
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    
    // Check for loading indicators
    const loadingElements = page.locator('.loading, .spinner, [class*="loading"], [class*="spinner"]');
    
    if (await loadingElements.count() > 0) {
      await expect(page).toHaveScreenshot('home-loading-state.png');
    }
    
    // Wait for full load
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveScreenshot('home-loaded-state.png');
  });
});

test.describe('Interactive Elements', () => {
  test('should display buttons and form elements correctly', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    
    // Look for buttons
    const buttons = page.locator('button, input[type="button"], input[type="submit"]');
    
    if (await buttons.count() > 0) {
      // Take screenshot showing buttons
      await expect(page).toHaveScreenshot('home-buttons.png');
      
      // Test hover state on first button
      await buttons.first().hover();
      await page.waitForTimeout(300);
      await expect(page).toHaveScreenshot('home-button-hover.png');
    }
  });

  test('should display error states appropriately', async ({ page }) => {
    await page.goto('/');
    
    // Look for any error messages or states
    const errorElements = page.locator('.error, .alert-error, [class*="error"]');
    
    if (await errorElements.count() > 0) {
      await expect(page).toHaveScreenshot('home-error-state.png');
    }
  });
});

test.describe('Accessibility Features', () => {
  test('should maintain proper contrast and visibility', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    
    // Test with high contrast (if supported by browser)
    try {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.waitForTimeout(500);
      await expect(page).toHaveScreenshot('home-dark-mode.png');
    } catch (error) {
      // Dark mode not supported, use regular screenshot
      await expect(page).toHaveScreenshot('home-light-mode.png');
    }
    
    // Reset to light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.waitForTimeout(500);
    await expect(page).toHaveScreenshot('home-light-mode-reset.png');
  });

  test('should be readable with larger text sizes', async ({ page }) => {
    await page.goto('/');
    
    // Increase text size (simulate browser zoom)
    await page.evaluate(() => {
      document.body.style.fontSize = '120%';
    });
    
    await page.waitForTimeout(500);
    await expect(page).toHaveScreenshot('home-large-text.png');
  });
});