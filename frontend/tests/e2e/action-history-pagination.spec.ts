import { expect, test } from '@playwright/test';

test('paginates action history with configurable page size', async ({ page }) => {
  const history = Array.from({ length: 60 }, (_, index) => ({
    timestamp: new Date(Date.UTC(2026, 2, 5, 12, index, 0)).toISOString(),
    summary: `Action ${index + 1}`,
    status: index % 2 === 0 ? 'completed' : 'started',
    details: `Detail ${index + 1}`,
  }));

  page.on('dialog', async (dialog) => {
    await dialog.accept();
  });

  await page.route('http://127.0.0.1:8000/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());

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

    if (url.pathname === '/connectors/list') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ connectors: [] }),
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
        body: JSON.stringify({ action_history: history }),
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
  await page.getByRole('link', { name: 'Action History' }).click();

  const rows = page.locator('#action-history-results tr');
  await expect(rows).toHaveCount(25);
  await expect(page.locator('#action-history-results')).toContainText('Action 1');
  await expect(page.locator('#action-history-results')).toContainText('Action 25');
  await expect(page.locator('#action-history-results')).not.toContainText('Action 26');
  await expect(page.locator('#action-history-pagination-controls')).toContainText('Page 1 of 3');

  await page.locator('#action-history-pagination-controls button', { hasText: 'Next' }).click();
  await expect(rows).toHaveCount(25);
  await expect(page.locator('#action-history-results')).toContainText('Action 26');
  await expect(page.locator('#action-history-results')).toContainText('Action 50');
  await expect(page.locator('#action-history-results')).not.toContainText('Action 25');
  await expect(page.locator('#action-history-pagination-controls')).toContainText('Page 2 of 3');

  await page.locator('#action-history-items-per-page').selectOption('50');
  await expect(rows).toHaveCount(50);
  await expect(page.locator('#action-history-results')).toContainText('Action 1');
  await expect(page.locator('#action-history-results')).toContainText('Action 50');
  await expect(page.locator('#action-history-results')).not.toContainText('Action 51');
  await expect(page.locator('#action-history-pagination-controls')).toContainText('Page 1 of 2');
});
