import { expect, type Page, test, type TestInfo } from '@playwright/test';

const backendUrl = 'http://127.0.0.1:8000';
const managedServices = process.env.KAIRYX_E2E_MANAGED_SERVICES === '1';

test.skip(!managedServices, 'This test requires the managed e2e backend service.');

async function captureStep(page: Page, testInfo: TestInfo, name: string) {
  const path = testInfo.outputPath(`${name}.png`);
  await page.screenshot({ path, fullPage: true });
  await testInfo.attach(name, { path, contentType: 'image/png' });
}

async function acceptNextDialog(page: Page, action: () => Promise<unknown>) {
  const dialogPromise = page.waitForEvent('dialog');
  await action();
  const dialog = await dialogPromise;
  const message = dialog.message();
  await dialog.accept();
  return message;
}

function parseJobName(message: string) {
  return String(message || '').match(/'([^']+)'/)?.[1] ?? null;
}

test('captures screenshots while creating connector, importing data, and running prediction', async ({ page }, testInfo) => {
  test.setTimeout(180000);

  await page.goto('/');
  await expect(page.locator('#status-text')).toHaveText('Backend Connected', { timeout: 30000 });

  const initialConnectorsPromise = page.waitForResponse(
    (response) => response.url() === `${backendUrl}/connectors/list` && response.request().method() === 'GET',
  );
  await page.getByRole('link', { name: 'Connectors' }).click();
  const initialConnectors = await (await initialConnectorsPromise).json();
  const existingConnectorNames = new Set((initialConnectors.connectors || []).map((connector: { name: string }) => connector.name));

  await page.locator('#add-connector-btn').click();
  await page.locator('#connector-type').selectOption('adjust');
  await page.locator('#adjust_api_token').fill('e2e-cleanup-token');
  await captureStep(page, testInfo, 'adding-connector');

  const savedConnectorsPromise = page.waitForResponse(
    (response) => response.url() === `${backendUrl}/connectors/list` && response.request().method() === 'GET',
  );
  await acceptNextDialog(page, () => page.locator('#save-connector-btn').click());
  const savedConnectors = await (await savedConnectorsPromise).json();
  const connectorNames = (savedConnectors.connectors || []).map((connector: { name: string }) => connector.name);
  const newConnectorName = connectorNames.find((name: string) => !existingConnectorNames.has(name));

  expect(newConnectorName).toBeTruthy();
  await expect(page.locator('#connector-list')).toContainText(newConnectorName!);
  await captureStep(page, testInfo, 'saved-connector');

  const importsListPromise = page.waitForResponse(
    (response) => response.url() === `${backendUrl}/list-imports` && response.request().method() === 'GET',
  );
  const configuredSourcesPromise = page.waitForResponse(
    (response) => response.url() === `${backendUrl}/list-configured-sources` && response.request().method() === 'GET',
  );
  await page.getByRole('link', { name: 'Player Cohorts' }).click();
  await Promise.all([importsListPromise, configuredSourcesPromise]);

  await expect(page.locator('#cohort-source-select')).toContainText(newConnectorName!);
  await page.locator('#cohort-source-select').selectOption({ label: newConnectorName! });
  await page.locator('#start-date-cohort').fill('2099-01-01');
  await page.locator('#end-date-cohort').fill('2099-01-01');

  const importResponsePromise = page.waitForResponse(
    (response) => response.url() === `${backendUrl}/ingest-and-process-data` && response.request().method() === 'POST',
  );
  const importDialogPromise = acceptNextDialog(page, () => page.locator('#import-data-btn').click());
  const importResponse = await importResponsePromise;
  const importBody = await importResponse.json();
  const importMessage = await importDialogPromise;
  const jobName = parseJobName(importBody.message || importMessage);

  expect(jobName).toBeTruthy();

  const importRow = page.locator('#import-list-container tbody tr').filter({ hasText: jobName! });
  await expect(importRow).toContainText('Ready to Use', { timeout: 60000 });
  await captureStep(page, testInfo, 'imported-data');

  const operatorHubImportsPromise = page.waitForResponse(
    (response) => response.url() === `${backendUrl}/list-imports` && response.request().method() === 'GET',
  );
  await page.getByRole('link', { name: 'Operator Hub' }).click();
  await operatorHubImportsPromise;

  await expect(page.locator('#dataset-select')).toContainText(jobName!);
  await page.locator('#dataset-select').selectOption({ label: jobName! });

  await page.locator('#predict-churn-btn').click();
  await expect(page.locator('#predict-churn-btn')).toHaveText('Stop');
  await expect(page.locator('#prediction-progress-info')).toContainText('Prediction job:', { timeout: 10000 });
  await expect(page.locator('#prediction-progress-info')).toContainText('Ready', { timeout: 60000 });
  await expect(page.locator('#predict-churn-btn')).toHaveText('Predict Churn');
  await expect(page.locator('#operator-hub-results tr').first()).not.toContainText('No player data available');
  await captureStep(page, testInfo, 'prediction-ran');
});
