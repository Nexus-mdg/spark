// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Header component visual tests.
 * Tests the navigation header appearance and functionality.
 */

test.describe('Header Component', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the main application
    await page.goto('/');
    
    // If login is required, this test will need to handle authentication
    // For now, we'll test what's visible on the login page or public areas
  });

  test('should display header elements correctly', async ({ page }) => {
    // Look for header element
    const header = page.locator('header, .header, nav, .nav');
    
    // If header is present on login page or after login
    if (await header.count() > 0) {
      await expect(header.first()).toBeVisible();
      
      // Take screenshot of header
      await expect(header.first()).toHaveScreenshot('header-component.png');
    } else {
      // Take screenshot of the page top area
      await expect(page.locator('body').first()).toHaveScreenshot('page-top-area.png', {
        clip: { x: 0, y: 0, width: 1280, height: 100 }
      });
    }
  });

  test('should display logo and branding', async ({ page }) => {
    // Look for logo elements
    const logoElements = page.locator('img[alt*="logo"], .logo, [class*="logo"]');
    
    if (await logoElements.count() > 0) {
      await expect(logoElements.first()).toBeVisible();
      await expect(logoElements.first()).toHaveScreenshot('header-logo.png');
    }
    
    // Look for title or branding text
    const titleElements = page.locator('h1, .title, [class*="title"]');
    if (await titleElements.count() > 0) {
      await expect(titleElements.first()).toBeVisible();
    }
  });

  test('should be responsive on different screen sizes', async ({ page }) => {
    // Test desktop view
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.waitForTimeout(500);
    
    const header = page.locator('header, .header, nav, .nav');
    if (await header.count() > 0) {
      await expect(header.first()).toHaveScreenshot('header-desktop.png');
    }
    
    // Test tablet view
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(500);
    
    if (await header.count() > 0) {
      await expect(header.first()).toHaveScreenshot('header-tablet.png');
    }
    
    // Test mobile view
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(500);
    
    if (await header.count() > 0) {
      await expect(header.first()).toHaveScreenshot('header-mobile.png');
    }
  });
});

test.describe('Navigation Elements', () => {
  test('should display navigation links when present', async ({ page }) => {
    await page.goto('/');
    
    // Look for navigation links
    const navLinks = page.locator('a[href], button');
    
    if (await navLinks.count() > 0) {
      // Take screenshot showing navigation
      await expect(page).toHaveScreenshot('navigation-links.png', {
        clip: { x: 0, y: 0, width: 1280, height: 200 }
      });
    }
  });

  test('should handle hover states appropriately', async ({ page }) => {
    await page.goto('/');
    
    const navLinks = page.locator('a[href], button').first();
    
    if (await navLinks.count() > 0) {
      // Take screenshot of normal state
      await expect(page).toHaveScreenshot('nav-normal-state.png', {
        clip: { x: 0, y: 0, width: 1280, height: 200 }
      });
      
      // Hover over first link
      await navLinks.hover();
      await page.waitForTimeout(500);
      
      // Take screenshot of hover state
      await expect(page).toHaveScreenshot('nav-hover-state.png', {
        clip: { x: 0, y: 0, width: 1280, height: 200 }
      });
    }
  });
});