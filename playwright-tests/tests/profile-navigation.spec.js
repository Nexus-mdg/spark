import { test, expect } from '@playwright/test';

test.describe('User Profile Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/profile');
  });

  test('should display user profile layout', async ({ page }) => {
    // Check page header
    await expect(page.locator('h1, h2').first()).toContainText(/Profile|User/i);
    
    // Check main content
    await expect(page.locator('main')).toBeVisible();
    
    // Take screenshot
    await expect(page).toHaveScreenshot('user-profile-page.png');
  });

  test('should show user information sections', async ({ page }) => {
    // Look for user info sections
    const sections = page.locator('.card, .section, .profile-section');
    
    // Take screenshot of profile sections
    await expect(page).toHaveScreenshot('user-profile-sections.png');
  });

  test('should display settings interface', async ({ page }) => {
    // Look for settings controls
    const settings = page.locator('input, select, button').first();
    
    // Take screenshot of settings
    await expect(page).toHaveScreenshot('user-profile-settings.png');
  });

  test('should show password change interface', async ({ page }) => {
    // Look for password change button/form
    const passwordSection = page.locator('text=Password, text=Security').first();
    
    if (await passwordSection.isVisible()) {
      // Click to expand if needed
      const changeButton = page.locator('button:has-text("Change")');
      if (await changeButton.isVisible()) {
        await changeButton.click();
      }
    }
    
    // Take screenshot of password section
    await expect(page).toHaveScreenshot('user-profile-password.png');
  });
});

test.describe('Navigation and Header', () => {
  test('should display header navigation across pages', async ({ page }) => {
    await page.goto('/');
    
    // Check header is visible
    const header = page.locator('header, nav, .header').first();
    await expect(header).toBeVisible();
    
    // Take screenshot of header
    await expect(page).toHaveScreenshot('header-navigation.png');
  });

  test('should show responsive navigation menu', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    
    // Look for mobile menu button
    const mobileMenu = page.locator('button[aria-label*="menu"], .hamburger, .menu-toggle').first();
    
    if (await mobileMenu.isVisible()) {
      await mobileMenu.click();
    }
    
    // Take screenshot of mobile navigation
    await expect(page).toHaveScreenshot('mobile-navigation.png');
  });
});