from playwright.sync_api import sync_playwright
import time
import pandas as pd
import os


def download_graph_csv():
    import sys
    import asyncio

    if sys.platform == 'win32':
        # Force the Proactor policy right before Playwright executes
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Path to save the file. Keep this in sync with Live_dashboard.views.PREOPEN_TBT_DIR
    # via the shared PREOPEN_TBT_DIR env var so the backend always reads where we write.
    save_dir = os.getenv("PREOPEN_TBT_DIR", r"C:\Users\ADMIN\Desktop\preopen")

    # Create the folder if it doesn't exist
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    file_path = os.path.join(save_dir, "nifty50_graph_data.csv")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            print("NSE page is uploading ...")
            page.goto("https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market", timeout=60000)

            page.wait_for_load_state("networkidle")
            time.sleep(5)

            print("Opening menu...")
            page.evaluate(
                "document.querySelector('.highcharts-contextbutton').dispatchEvent(new MouseEvent('click', {bubbles: true}))")
            time.sleep(2)

            with page.expect_download() as download_info:
                print("Clicking Download CSV...")
                page.get_by_text("Download CSV").click(force=True)

            # Saving the file to the specified path
            download = download_info.value
            download.save_as(file_path)
            print(f"Success! File saved at: {file_path}")

        except Exception as e:
            print(f"Error occurred: {e}")

        finally:
            browser.close()

    # Displaying data in the terminal
    if os.path.exists(file_path):
        print("\n--- CSV Data Preview ---")
        df = pd.read_csv(file_path)
        print(df.head(10).to_string())
    else:
        print("File not found!")


if __name__ == "__main__":
    download_graph_csv()