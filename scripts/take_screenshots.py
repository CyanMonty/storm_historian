"""Script to capture UI screenshots for documentation."""
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

SCREENSHOTS_DIR = Path(__file__).parent.parent / "docs" / "screenshots"


async def take_screenshots():
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={"width": 1400, "height": 2200})

        # 01 - National Overview (full view with charts and choropleth map)
        await page.goto("http://localhost:8502/")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(5000)
        await page.screenshot(path=str(SCREENSHOTS_DIR / "01_national_overview.png"))
        print("Saved 01_national_overview.png")

        # 02 - National full page (tall viewport should show map)
        await page.screenshot(path=str(SCREENSHOTS_DIR / "02_national_full.png"), full_page=True)
        print("Saved 02_national_full.png")

        # 03 - State Overview (Texas) - tall viewport shows charts + hotspot map
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(500)
        select = await page.query_selector('[data-baseweb="select"]')
        await select.click()
        await page.keyboard.type("Texas")
        await page.wait_for_timeout(500)
        option = await page.query_selector('[role="option"]')
        await option.click()
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(5000)
        await page.screenshot(path=str(SCREENSHOTS_DIR / "03_state_overview_texas.png"))
        print("Saved 03_state_overview_texas.png")

        # 04 - Texas full page
        await page.screenshot(path=str(SCREENSHOTS_DIR / "04_state_full_texas.png"), full_page=True)
        print("Saved 04_state_full_texas.png")

        # 05 - ZIP Explorer (empty)
        await page.goto("http://localhost:8502/zip")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)
        await page.screenshot(path=str(SCREENSHOTS_DIR / "05_zip_explorer.png"))
        print("Saved 05_zip_explorer.png")

        # 06 - ZIP results (77002 - Houston TX) - tall viewport
        await page.fill('[placeholder="e.g. 77002"]', "77002")
        await page.click('button:has-text("Search")')
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(5000)
        await page.screenshot(path=str(SCREENSHOTS_DIR / "06_zip_results.png"))
        print("Saved 06_zip_results.png")

        # 07 - no longer needed (full page above covers it)

        await browser.close()

    print("All screenshots saved to:", SCREENSHOTS_DIR)


if __name__ == "__main__":
    asyncio.run(take_screenshots())
