import { test, expect } from '@playwright/test';

test.describe('Login Page', () => {
  test('should display login form correctly', async ({ page }) => {
    await page.goto('/');
    
    // Check that login form is visible
    await expect(page.locator('form')).toBeVisible();
    await expect(page.locator('input[type="text"]')).toBeVisible();
    await expect(page.locator('input[type="password"]')).toBeVisible();
    await expect(page.locator('button[type="submit"]')).toBeVisible();
    
    // Take screenshot for visual comparison
    await expect(page).toHaveScreenshot('login-page.png');
  });

  test('should show validation errors for empty fields', async ({ page }) => {
    await page.goto('/');
    
    // Try to submit empty form
    await page.click('button[type="submit"]');
    
    // Check for error messages
    await expect(page.locator('text=Username is required')).toBeVisible();
    await expect(page.locator('text=Password is required')).toBeVisible();
    
    // Take screenshot of error state
    await expect(page).toHaveScreenshot('login-validation-errors.png');
  });

  test('should handle login form interactions', async ({ page }) => {
    await page.goto('/');
    
    // Fill form fields
    await page.fill('input[type="text"]', 'testuser');
    await page.fill('input[type="password"]', 'testpass');
    
    // Take screenshot of filled form
    await expect(page).toHaveScreenshot('login-form-filled.png');
  });
});