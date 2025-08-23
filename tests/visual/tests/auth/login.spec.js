// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Login flow visual tests.
 * Tests the authentication interface and login/logout flows.
 */

test.describe('Login Page', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to login page
    await page.goto('/');
  });

  test('should display login form correctly', async ({ page }) => {
    // Wait for the login form to be visible
    await expect(page.locator('form')).toBeVisible();
    
    // Check for essential login elements
    await expect(page.locator('input[type="text"], input[type="email"]')).toBeVisible();
    await expect(page.locator('input[type="password"]')).toBeVisible();
    await expect(page.locator('button[type="submit"], input[type="submit"]')).toBeVisible();
    
    // Take screenshot of login form
    await expect(page).toHaveScreenshot('login-form.png');
  });

  test('should display login form with gradient background', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
    
    // Check for gradient background elements (based on Login.jsx structure)
    const backgroundElements = page.locator('.bg-gradient-to-br');
    if (await backgroundElements.count() > 0) {
      await expect(backgroundElements.first()).toBeVisible();
    }
    
    // Take full page screenshot
    await expect(page).toHaveScreenshot('login-page-full.png', { fullPage: true });
  });

  test('should show validation messages appropriately', async ({ page }) => {
    // Try to submit empty form
    const submitButton = page.locator('button[type="submit"], input[type="submit"]').first();
    await submitButton.click();
    
    // Wait a moment for any validation messages
    await page.waitForTimeout(1000);
    
    // Take screenshot of validation state
    await expect(page).toHaveScreenshot('login-validation.png');
  });

  test('should be responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    
    // Wait for responsive layout
    await page.waitForTimeout(500);
    
    // Take mobile screenshot
    await expect(page).toHaveScreenshot('login-mobile.png');
  });

  test('should handle theme variations if present', async ({ page }) => {
    // Check for theme-related elements or classes
    const themeElements = page.locator('[class*="theme"], [class*="gradient"]');
    
    if (await themeElements.count() > 0) {
      // Take screenshot with current theme
      await expect(page).toHaveScreenshot('login-theme-default.png');
    } else {
      // Take standard screenshot
      await expect(page).toHaveScreenshot('login-standard.png');
    }
  });
});

test.describe('Login Functionality', () => {
  test('should navigate correctly after successful login attempt', async ({ page }) => {
    await page.goto('/');
    
    // Fill in login form (using placeholder credentials for visual testing)
    const usernameField = page.locator('input[type="text"], input[type="email"]').first();
    const passwordField = page.locator('input[type="password"]').first();
    
    if (await usernameField.isVisible()) {
      await usernameField.fill('test@example.com');
    }
    
    if (await passwordField.isVisible()) {
      await passwordField.fill('testpassword');
    }
    
    // Take screenshot before submission
    await expect(page).toHaveScreenshot('login-filled-form.png');
    
    // Submit form (this may fail due to invalid credentials, which is expected)
    const submitButton = page.locator('button[type="submit"], input[type="submit"]').first();
    await submitButton.click();
    
    // Wait for any response (success or error)
    await page.waitForTimeout(2000);
    
    // Take screenshot of result state
    await expect(page).toHaveScreenshot('login-result.png');
  });

  test('should show error messages properly', async ({ page }) => {
    await page.goto('/');
    
    // Fill invalid credentials
    const usernameField = page.locator('input[type="text"], input[type="email"]').first();
    const passwordField = page.locator('input[type="password"]').first();
    
    if (await usernameField.isVisible()) {
      await usernameField.fill('invalid@example.com');
    }
    
    if (await passwordField.isVisible()) {
      await passwordField.fill('wrongpassword');
    }
    
    // Submit form
    const submitButton = page.locator('button[type="submit"], input[type="submit"]').first();
    await submitButton.click();
    
    // Wait for error message to appear
    await page.waitForTimeout(3000);
    
    // Look for error message elements
    const errorElements = page.locator('.error, .alert, [class*="error"], [class*="alert"]');
    
    // Take screenshot showing error state
    await expect(page).toHaveScreenshot('login-error-state.png');
  });
});