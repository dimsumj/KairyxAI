import { expect, test } from '@playwright/test';

test('loads the operator dashboard shell', async ({ page }) => {
  await page.goto('/');

  await expect(page).toHaveTitle(/Kairyx AI/i);
  await expect(page.getByText(/Kairyx AI/i)).toBeVisible();
  await expect(page.getByRole('heading', { name: /Operator Hub/i })).toBeVisible();
  await expect(page.getByRole('link', { name: /Player Cohorts/i })).toBeVisible();
});
