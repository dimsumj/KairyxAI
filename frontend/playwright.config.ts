import { defineConfig, devices } from '@playwright/test';

const managedServices = process.env.KAIRYX_E2E_MANAGED_SERVICES === '1';
const frontendPort = Number(process.env.KAIRYX_E2E_FRONTEND_PORT || 3000);
const baseURL = process.env.KAIRYX_E2E_BASE_URL || `http://127.0.0.1:${frontendPort}`;

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL,
    trace: 'on-first-retry',
  },
  ...(managedServices
    ? {}
    : {
        webServer: {
          command: `npm run dev -- --host 127.0.0.1 --port ${frontendPort} --strictPort`,
          url: baseURL,
          reuseExistingServer: !process.env.CI,
          timeout: 120 * 1000,
        },
      }),
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
