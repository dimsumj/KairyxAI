import { expect, test } from '@playwright/test';

const backendUrl = process.env.KAIRYX_E2E_BACKEND_URL || 'http://127.0.0.1:8000';

test('saves an AppsFlyer connector against the current backend host', async ({ page }) => {
  const connectors: Array<{ type: string; name: string; details: string }> = [];
  let capturedUrl = '';
  let capturedPayload: Record<string, unknown> | null = null;

  page.on('dialog', async (dialog) => {
    await dialog.accept();
  });

  await page.addInitScript((url) => {
    Object.assign(window, { KAIRYX_BACKEND_URL: url });
  }, backendUrl);

  await page.route(`${backendUrl}/**`, async (route) => {
    const request = route.request();
    const url = new URL(request.url());

    if (request.resourceType() === 'document' || url.pathname === '/') {
      await route.continue();
      return;
    }

    if (url.pathname === '/configure-appsflyer' && request.method() === 'POST') {
      capturedUrl = request.url();
      capturedPayload = request.postDataJSON() as Record<string, unknown>;
      connectors.push({ type: 'appsflyer', name: 'AppsFlyer 1', details: 'Configured' });
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ message: 'AppsFlyer credentials configured and cached.' }),
      });
      return;
    }

    if (url.pathname === '/connectors/list') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ connectors }),
      });
      return;
    }

    if (url.pathname === '/health') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ status: 'ok' }),
      });
      return;
    }

    if (url.pathname === '/list-imports') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ imports: [] }),
      });
      return;
    }

    if (url.pathname === '/list-configured-sources') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ sources: [] }),
      });
      return;
    }

    if (url.pathname === '/services-health') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({}),
      });
      return;
    }

    if (url.pathname === '/action-history') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ action_history: [] }),
      });
      return;
    }

    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({}),
    });
  });

  await page.goto('/');
  await page.getByRole('link', { name: 'Connectors' }).click();
  await page.locator('#add-connector-btn').click();
  await page.locator('#connector-type').selectOption('appsflyer');
  await page.locator('#appsflyer_api_token').fill('af-token');
  await page.locator('#appsflyer_app_id').fill('demo-app');
  await page.locator('#appsflyer_pull_api_url').fill('https://example.com/pull');
  await page.locator('#save-connector-btn').click();

  await expect(page.locator('#connector-list')).toContainText('AppsFlyer 1');
  expect(capturedUrl).toBe(`${backendUrl}/configure-appsflyer`);
  expect(capturedPayload).toEqual({
    api_token: 'af-token',
    app_id: 'demo-app',
    pull_api_url: 'https://example.com/pull',
  });
});
