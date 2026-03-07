import { expect, test } from '@playwright/test';

const backendUrl = process.env.KAIRYX_E2E_BACKEND_URL || 'http://127.0.0.1:8000';

test('uses a single churn button that switches to stop while prediction is running', async ({ page }) => {
  let stopRequested = false;
  let stopCallCount = 0;
  let statusPollCount = 0;

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
        body: JSON.stringify({
          imports: [
            {
              name: 'Demo Import',
              status: 'Ready to Use',
              timestamp: '2026-03-05T12:00:00Z',
              expiration_timestamp: '2026-03-08T12:00:00Z',
            },
          ],
        }),
      });
      return;
    }

    if (url.pathname === '/predict-churn-for-import-async' && request.method() === 'POST') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ prediction_job_id: 'prediction-1' }),
      });
      return;
    }

    if (url.pathname === '/prediction-job/prediction-1' && request.method() === 'GET') {
      statusPollCount += 1;
      const status = stopRequested && statusPollCount > 1 ? 'Stopped' : 'Processing';
      const predictions = status === 'Stopped'
        ? [
            {
              user_id: 'player-1',
              ltv: 10,
              session_count: 2,
              event_count: 5,
              predicted_churn_risk: 'High',
              churn_reason: 'Low activity',
              suggested_action: 'Send winback',
            },
          ]
        : [];

      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          prediction_job: {
            id: 'prediction-1',
            status,
            stop_requested: stopRequested,
            processed_count: status === 'Stopped' ? 1 : 0,
            total_count: 1,
            progress_pct: status === 'Stopped' ? 100 : 0,
            predictions,
          },
        }),
      });
      return;
    }

    if (url.pathname === '/prediction-job/prediction-1/stop' && request.method() === 'POST') {
      stopRequested = true;
      stopCallCount += 1;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          prediction_job: {
            id: 'prediction-1',
            status: 'Processing',
            stop_requested: true,
            processed_count: 0,
            total_count: 1,
            progress_pct: 0,
            predictions: [],
          },
        }),
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
  await expect(page.locator('#dataset-select')).toContainText('Demo Import');
  await expect(page.locator('#stop-predict-btn')).toHaveCount(0);

  const predictButton = page.locator('#predict-churn-btn');
  await expect(predictButton).toHaveText('Predict Churn');

  await predictButton.click();
  await expect(predictButton).toHaveText('Stop');
  await expect(page.locator('#prediction-progress-info')).toContainText('Processing');

  await predictButton.click();
  await expect(predictButton).toHaveText('Stopping...');

  await expect(page.locator('#prediction-progress-info')).toContainText('Stopped', { timeout: 5000 });
  await expect(predictButton).toHaveText('Predict Churn');
  await expect(page.locator('#operator-hub-results')).toContainText('player-1');
  expect(stopCallCount).toBe(1);
});
