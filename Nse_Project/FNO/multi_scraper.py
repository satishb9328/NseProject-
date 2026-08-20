import asyncio
import os
from datetime import datetime
from playwright.async_api import async_playwright

# The expiry dates currently visible on your NSE screen
expiry_dates = ["28-Jul-2026", "25-Aug-2026", "29-Sep-2026"]

async def download_all_historical_data():
    async with async_playwright() as p:
        # Launch browser (headless=False mimics a real user better to avoid NSE blocks)
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        print("Visiting NSE homepage first to generate session cookies...")
        await page.goto("https://www.nseindia.com", wait_until="networkidle")
        await asyncio.sleep(2)

        for expiry in expiry_dates:
            # 1. Convert '28-Jul-2026' to '28-07-2026'
            date_obj = datetime.strptime(expiry, "%d-%b-%Y")
            formatted_date = date_obj.strftime("%d-%m-%Y")

            # 2. Construct the exact URL using the NSE formula
            identifier = f"FUTIDXNIFTY{formatted_date}XX0.00"
            url = f"https://www.nseindia.com/get-quote/derivatives/NIFTY/NIFTY-50?identifier={identifier}"

            print(f"\nNavigating to: {url}")
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(3) # Give the page time to fully render

            try:
                # 3. Click the 'Historical Data' tab
                await page.click('a:has-text("Historical Data")', timeout=5000)
                print("Clicked Historical Data tab...")
                await asyncio.sleep(3) # Wait for the data table to populate

                # 4. Listen for the download and click the CSV button
                async with page.expect_download() as download_info:
                    await page.click('a:has-text("Download (.csv)")', timeout=5000)

                # 5. Save the file with the specific expiry date in the name
                download = await download_info.value
                filename = f"NIFTY_FUT_{formatted_date}.csv"
                filepath = os.path.join(os.getcwd(), filename)
                await download.save_as(filepath)
                print(f"✅ Successfully downloaded: {filename}")

            except Exception as e:
                print(f"❌ Failed to download data for {expiry}. Error: {e}")

            # CRITICAL: Wait 5 seconds between downloads so NSE doesn't ban your IP
            print("Waiting 5 seconds before fetching the next contract...")
            await asyncio.sleep(5)

        print("\nAll downloads complete!")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(download_all_historical_data())