// @ts-check
import { defineConfig } from '@playwright/test';

/**
 * Playwright configuration for visual testing.
 * @see https://playwright.dev/docs/test-configuration
 */
export default defineConfig({
  testDir: './tests',
  
  /* Run tests in files in parallel */
  fullyParallel: true,
  
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  
  /* Retry on CI only */
  retries: process.env.CI ? 2 : 0,
  
  /* Opt out of parallel tests on CI. */
  workers: process.env.CI ? 1 : undefined,
  
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: [
    ['html', { outputFolder: '../reports/visual_reports' }],
    ['json', { outputFile: '../reports/visual_reports/results.json' }],
    ['list']
  ],
  
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: process.env.UI_BASE || 'http://localhost:5001',
    
    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'on-first-retry',
    
    /* Take screenshot only when test fails */
    screenshot: 'only-on-failure',
    
    /* Record video only when test fails */
    video: 'retain-on-failure',
    
    /* Timeout for each action (e.g., click, fill) */
    actionTimeout: 30 * 1000,
    
    /* Global timeout for each test */
    timeout: 60 * 1000,
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: 'chromium',
      use: { 
        ...{}, // devices['Desktop Chrome']
        viewport: { width: 1280, height: 720 },
        // Ignore HTTPS errors in local development
        ignoreHTTPSErrors: true,
      },
    },

    // Uncomment for additional browser testing in Phase 3
    // {
    //   name: 'firefox',
    //   use: { ...devices['Desktop Firefox'] },
    // },
    //
    // {
    //   name: 'webkit',
    //   use: { ...devices['Desktop Safari'] },
    // },

    /* Test against mobile viewports. */
    // {
    //   name: 'Mobile Chrome',
    //   use: { ...devices['Pixel 5'] },
    // },
    // {
    //   name: 'Mobile Safari',
    //   use: { ...devices['iPhone 12'] },
    // },
  ],

  /* Run your local dev server before starting the tests */
  // webServer: {
  //   command: 'npm run start',
  //   url: 'http://127.0.0.1:3000',
  //   reuseExistingServer: !process.env.CI,
  // },

  /* Output directories */
  outputDir: '../reports/visual_reports/test-results',
  
  /* Global setup and teardown */
  // globalSetup: require.resolve('./utils/global-setup'),
  // globalTeardown: require.resolve('./utils/global-teardown'),

  /* Expect configuration */
  expect: {
    /* Maximum time expect() should wait for the condition to be met. */
    timeout: 10 * 1000,
    
    /* Threshold for visual comparisons */
    toHaveScreenshot: {
      threshold: 0.3,
      mode: 'strict'
    },
    
    toMatchSnapshot: {
      threshold: 0.3
    }
  },
});