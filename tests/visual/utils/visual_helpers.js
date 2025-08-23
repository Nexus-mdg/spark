/**
 * Visual testing helper utilities.
 * Provides common functions for screenshot comparison and visual regression testing.
 */

/**
 * Wait for all images to load on the page
 * @param {import('@playwright/test').Page} page 
 */
export async function waitForImages(page) {
  await page.evaluate(() => {
    const images = Array.from(document.querySelectorAll('img'));
    return Promise.all(
      images.map(img => {
        if (img.complete) return Promise.resolve();
        return new Promise(resolve => {
          img.onload = resolve;
          img.onerror = resolve;
        });
      })
    );
  });
}

/**
 * Wait for animations to complete
 * @param {import('@playwright/test').Page} page 
 * @param {number} timeout - Timeout in milliseconds
 */
export async function waitForAnimations(page, timeout = 1000) {
  await page.waitForFunction(() => {
    const animations = document.getAnimations();
    return animations.length === 0 || animations.every(anim => anim.playState === 'finished');
  }, { timeout });
}

/**
 * Hide elements that may cause flaky screenshots (like timestamps, random IDs)
 * @param {import('@playwright/test').Page} page 
 * @param {string[]} selectors - Array of CSS selectors to hide
 */
export async function hideFlakySeletors(page, selectors = []) {
  const defaultSelectors = [
    '[data-testid="timestamp"]',
    '.timestamp',
    '[class*="timestamp"]',
    '.random-id',
    '[class*="random"]'
  ];
  
  const allSelectors = [...defaultSelectors, ...selectors];
  
  for (const selector of allSelectors) {
    await page.locator(selector).evaluateAll(elements => {
      elements.forEach(el => el.style.visibility = 'hidden');
    }).catch(() => {
      // Ignore if selector doesn't exist
    });
  }
}

/**
 * Setup page for consistent screenshots
 * @param {import('@playwright/test').Page} page 
 * @param {Object} options
 * @param {boolean} options.hideScrollbars - Hide scrollbars for cleaner screenshots
 * @param {boolean} options.disableAnimations - Disable CSS animations
 * @param {string[]} options.hideSelectors - Additional selectors to hide
 */
export async function setupPageForScreenshot(page, options = {}) {
  const {
    hideScrollbars = true,
    disableAnimations = true,
    hideSelectors = []
  } = options;
  
  // Disable animations for consistent screenshots
  if (disableAnimations) {
    await page.addStyleTag({
      content: `
        *, *::before, *::after {
          animation-duration: 0s !important;
          animation-delay: 0s !important;
          transition-duration: 0s !important;
          transition-delay: 0s !important;
        }
      `
    });
  }
  
  // Hide scrollbars
  if (hideScrollbars) {
    await page.addStyleTag({
      content: `
        ::-webkit-scrollbar {
          display: none;
        }
        * {
          scrollbar-width: none;
        }
      `
    });
  }
  
  // Hide flaky selectors
  if (hideSelectors.length > 0) {
    await hideFlakySeletors(page, hideSelectors);
  }
  
  // Wait for images and any remaining animations
  await waitForImages(page);
  await waitForAnimations(page);
}

/**
 * Take screenshot with consistent settings
 * @param {import('@playwright/test').Page} page 
 * @param {string} name - Screenshot name
 * @param {Object} options - Playwright screenshot options
 */
export async function takeConsistentScreenshot(page, name, options = {}) {
  // Setup page for screenshot
  await setupPageForScreenshot(page);
  
  // Wait a moment for everything to settle
  await page.waitForTimeout(300);
  
  // Take screenshot with default settings
  return await page.screenshot({
    fullPage: false,
    ...options,
    path: name.endsWith('.png') ? name : `${name}.png`
  });
}

/**
 * Compare element screenshot with baseline
 * @param {import('@playwright/test').Locator} locator 
 * @param {string} name 
 * @param {Object} options 
 */
export async function compareElementScreenshot(locator, name, options = {}) {
  const page = locator.page();
  await setupPageForScreenshot(page);
  
  return await locator.screenshot({
    ...options,
    path: name.endsWith('.png') ? name : `${name}.png`
  });
}

/**
 * Test responsive breakpoints
 * @param {import('@playwright/test').Page} page 
 * @param {Function} testFunction - Function to run at each breakpoint
 * @param {Object[]} breakpoints - Array of {name, width, height} objects
 */
export async function testResponsiveBreakpoints(page, testFunction, breakpoints = null) {
  const defaultBreakpoints = [
    { name: 'mobile', width: 375, height: 667 },
    { name: 'tablet', width: 768, height: 1024 },
    { name: 'desktop', width: 1280, height: 720 },
    { name: 'desktop-large', width: 1920, height: 1080 }
  ];
  
  const testBreakpoints = breakpoints || defaultBreakpoints;
  
  for (const breakpoint of testBreakpoints) {
    await page.setViewportSize({ width: breakpoint.width, height: breakpoint.height });
    await page.waitForTimeout(500); // Allow time for responsive layout
    await testFunction(breakpoint);
  }
}

/**
 * Wait for element to be stable (not moving/changing)
 * @param {import('@playwright/test').Locator} locator 
 * @param {number} timeout 
 */
export async function waitForElementStable(locator, timeout = 2000) {
  let previousBox = null;
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeout) {
    try {
      const currentBox = await locator.boundingBox();
      
      if (previousBox && currentBox) {
        // Check if position and size are stable
        const stable = 
          Math.abs(currentBox.x - previousBox.x) < 1 &&
          Math.abs(currentBox.y - previousBox.y) < 1 &&
          Math.abs(currentBox.width - previousBox.width) < 1 &&
          Math.abs(currentBox.height - previousBox.height) < 1;
        
        if (stable) {
          return;
        }
      }
      
      previousBox = currentBox;
      await locator.page().waitForTimeout(100);
    } catch (error) {
      // Element not ready yet, continue waiting
      await locator.page().waitForTimeout(100);
    }
  }
}

/**
 * Mask dynamic content for consistent screenshots
 * @param {import('@playwright/test').Page} page 
 * @param {string[]} selectors - Selectors for elements to mask
 * @param {string} maskColor - Color to use for masking (default: #ff0000)
 */
export async function maskDynamicContent(page, selectors, maskColor = '#ff0000') {
  for (const selector of selectors) {
    await page.locator(selector).evaluateAll((elements, color) => {
      elements.forEach(el => {
        el.style.backgroundColor = color;
        el.style.color = color;
      });
    }, maskColor).catch(() => {
      // Ignore if selector doesn't exist
    });
  }
}