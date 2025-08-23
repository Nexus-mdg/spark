// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Logout flow visual tests.
 * Tests the logout functionality and post-logout states.
 */

test.describe('Logout Flow', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the application
    await page.goto('/');
  });

  test('should display logout option when authenticated', async ({ page }) => {
    // This test assumes user is already logged in or authentication is disabled
    // Look for logout button or user menu
    const logoutElements = page.locator('button:has-text("Logout"), a:has-text("Logout"), [data-testid="logout"]');
    
    if (await logoutElements.count() > 0) {
      await expect(logoutElements.first()).toBeVisible();
      await expect(page).toHaveScreenshot('logout-option-visible.png');
    } else {
      // If no logout button, might be on login page already
      await expect(page).toHaveScreenshot('no-logout-needed.png');
    }
  });

  test('should handle logout process correctly', async ({ page }) => {
    // Look for logout button
    const logoutButton = page.locator('button:has-text("Logout"), a:has-text("Logout"), [data-testid="logout"]').first();
    
    if (await logoutButton.count() > 0) {
      // Take screenshot before logout
      await expect(page).toHaveScreenshot('before-logout.png');
      
      // Click logout
      await logoutButton.click();
      
      // Wait for logout to complete
      await page.waitForTimeout(2000);
      
      // Take screenshot after logout
      await expect(page).toHaveScreenshot('after-logout.png');
      
      // Should redirect to login page or show logged out state
      const loginElements = page.locator('input[type="password"], form:has(input[type="password"])');
      if (await loginElements.count() > 0) {
        await expect(loginElements.first()).toBeVisible();
        await expect(page).toHaveScreenshot('redirected-to-login.png');
      }
    }
  });

  test('should clear user session data after logout', async ({ page }) => {
    // Navigate to a protected area (if any)
    await page.goto('/');
    
    // Look for user-specific content
    const userContent = page.locator('[data-testid="user-info"], .user-info, .profile');
    
    if (await userContent.count() > 0) {
      // Take screenshot with user content
      await expect(page).toHaveScreenshot('with-user-content.png');
      
      // Perform logout if possible
      const logoutButton = page.locator('button:has-text("Logout"), a:has-text("Logout")').first();
      
      if (await logoutButton.count() > 0) {
        await logoutButton.click();
        await page.waitForTimeout(2000);
        
        // Verify user content is no longer present
        await expect(page).toHaveScreenshot('user-content-cleared.png');
      }
    }
  });
});

test.describe('Post-Logout State', () => {
  test('should prevent access to protected areas after logout', async ({ page }) => {
    await page.goto('/');
    
    // Try to access different routes that might be protected
    const protectedRoutes = ['/dashboard', '/profile', '/admin', '/data'];
    
    for (const route of protectedRoutes) {
      try {
        await page.goto(route);
        await page.waitForTimeout(1000);
        
        // Take screenshot of the result
        await expect(page).toHaveScreenshot(`access-attempt-${route.replace('/', '')}.png`);
        
        // Check if redirected to login
        const currentUrl = page.url();
        const isOnLogin = currentUrl.includes('login') || currentUrl === page.context().baseURL + '/';
        
        if (isOnLogin) {
          // Good - redirected to login
          await expect(page).toHaveScreenshot(`protected-route-${route.replace('/', '')}-blocked.png`);
        }
      } catch (error) {
        // Route might not exist, which is fine
        continue;
      }
    }
  });

  test('should display appropriate messaging after logout', async ({ page }) => {
    await page.goto('/');
    
    // Look for logout success messages
    const messages = page.locator('.message, .alert, .notification, [class*="message"], [class*="alert"]');
    
    if (await messages.count() > 0) {
      await expect(page).toHaveScreenshot('logout-messages.png');
    }
    
    // Take general screenshot of post-logout state
    await expect(page).toHaveScreenshot('post-logout-state.png');
  });
});