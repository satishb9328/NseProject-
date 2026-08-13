import os
import json
import subprocess
import shutil
import sys
import logging
import re
import requests
import argparse
import warnings


def _find_chrome_exe():
    # common locations on Windows
    candidates = [
        os.path.join(os.environ.get("PROGRAMFILES", r"C:\\Program Files"), "Google\\Chrome\\Application\\chrome.exe"),
        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)"), "Google\\Chrome\\Application\\chrome.exe"),
        os.path.join(os.environ.get("LOCALAPPDATA", r"C:\\Users\\%USERNAME%\\AppData\\Local"), "Google\\Chrome\\Application\\chrome.exe"),
    ]
    which = shutil.which("chrome") or shutil.which("chrome.exe")
    if which:
        candidates.insert(0, which)
    for p in candidates:
        try:
            if p and os.path.isfile(p):
                return p
        except Exception:
            continue
    return None


def get_chrome_major_version():
    exe = _find_chrome_exe()
    if not exe:
        return None
    try:
        out = subprocess.check_output([exe, "--version"], stderr=subprocess.STDOUT, text=True)
        # expected like: "Google Chrome 142.0.7444.176"
        m = re.search(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", out)
        if m:
            return int(m.group(1))
    except Exception:
        return None
    return None


def _parse_chrome_version_from_exception(exc_text):
    # look for phrases like: 'Current browser version is 142.0.7444.176'
    m = re.search(r"Current browser version is (\d+)\.(?:[0-9.]+)", exc_text)
    if m:
        return int(m.group(1))
    # fallback: any version-like pattern
    m2 = re.search(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", exc_text)
    if m2:
        return int(m2.group(1))
    return None


def make_chrome_options():
    try:
        import undetected_chromedriver as uc
    except ImportError:
        raise RuntimeError('undetected_chromedriver is required for Selenium fallback')
    opts = uc.ChromeOptions()
    # chrome_options.add_argument('--headless')  # Uncomment for headless mode
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--window-size=1920,1080')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    # Compatibility: some uc/selenium combos expect an explicit headless attribute
    if not hasattr(opts, 'headless'):
        opts.headless = False
    return opts


def fetch_via_requests(url):
    session = requests.Session()
    # Use a modern desktop UA to reduce chance of blocking
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.7444.176 Safari/537.36"
    headers = {
        'User-Agent': ua,
        'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }
    try:
        # First request homepage to get cookies and any required headers
        r = session.get('https://www.nseindia.com', headers=headers, timeout=15)
        r.raise_for_status()

        # Update headers for API call with cookies
        headers['Referer'] = 'https://www.nseindia.com/'
        headers['Accept'] = 'application/json, text/plain, */*'
        headers['Sec-Fetch-Dest'] = 'empty'
        headers['Sec-Fetch-Mode'] = 'cors'
        headers['Sec-Fetch-Site'] = 'same-origin'

        # Then request the API endpoint
        r2 = session.get(url, headers=headers, timeout=15)
        r2.raise_for_status()

        # Verify it's valid JSON before returning
        try:
            json.loads(r2.text)
            return r2.text
        except json.JSONDecodeError:
            # sometimes the API responds with compressed or garbled content;
            # try to decompress common encodings before giving up
            logging.warning('Response is not valid JSON, attempting to decompress')
            raw = r2.content
            # try gzip, zlib and brotli
            for mod in ("gzip", "zlib", "brotli"):
                try:
                    # import lazily to avoid requiring packages if unused
                    m = __import__(mod)
                    decompressed = m.decompress(raw)
                    # if decompression succeeds, update raw and try to parse
                    raw = decompressed
                except Exception:
                    pass
            try:
                text = raw.decode(r2.encoding or "utf-8", errors="ignore")
                json.loads(text)
                return text
            except Exception:
                logging.warning('Decompression attempt failed, giving up on requests fetch')
                return None
    except Exception as e:
        logging.error(f'Requests fallback failed: {e}')
        return None


def fetch_and_save_preopen_data_selenium(use_selenium=False):
    url = 'https://www.nseindia.com/api/chart-databyindex-preopen'

    # First, try the requests-based fetch (fast and avoids Chrome)
    try:
        logging.info('Attempting requests-based fetch for preopen data')
        data = fetch_via_requests(url)
        if data:
            folder = os.path.join(os.path.dirname(__file__), 'data')
            os.makedirs(folder, exist_ok=True)
            file_path = os.path.join(folder, 'preopen_data.json')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(data)
            logging.info(f"Saved scraped data to: {file_path} (via requests)")
            return
        logging.info('Requests-based fetch returned no data')
    except Exception:
        logging.exception('Requests-based fetch attempt raised an exception')

    # Auto-enable selenium fallback if requests failed.
    if not use_selenium:
        logging.info('Requests fetch failed; enabling selenium fallback automatically')
        use_selenium = True

    # Selenium fallback: defer imports so requests-only usage does not require selenium packages.
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.common.by import By
        from selenium import webdriver
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
    except Exception as e:
        logging.exception('Selenium dependencies are not available. Install undetected_chromedriver, selenium, and webdriver-manager to use fallback.')
        raise RuntimeError('Selenium fallback unavailable') from e

    # Monkey-patch Chrome's __del__ to suppress cleanup errors
    original_del = getattr(uc.Chrome, '__del__', None)
    if original_del:
        def patched_del(self):
            try:
                original_del(self)
            except (OSError, Exception):
                pass
        uc.Chrome.__del__ = patched_del

    driver = None
    try:
        logging.info('Trying selenium fallback to fetch preopen data')
        opts = make_chrome_options()
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=opts)
        logging.info('Successfully started Chrome with webdriver-manager')

        driver.get('https://www.nseindia.com')
        driver.get(url)

        if driver.find_elements(By.TAG_NAME, 'pre'):
            data = driver.find_element(By.TAG_NAME, 'pre').text
        else:
            data = driver.execute_script('return document.documentElement.innerText') or driver.page_source

        folder = os.path.join(os.path.dirname(__file__), 'data')
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, 'preopen_data.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(data)
        logging.info(f"Saved scraped data to: {file_path} (via selenium)")

    except Exception as e:
        logging.exception('Error while fetching preopen data via selenium:')
        raise

    finally:
        if driver is not None:
            import io
            try:
                old_stderr = sys.stderr
                sys.stderr = io.StringIO()
                driver.quit()
                driver = None
                import gc
                import time
                time.sleep(0.1)
                gc.collect()
            except (OSError, Exception):
                pass
            finally:
                sys.stderr = old_stderr
                driver = None


def show_raw_preopen_data():
    file_path = os.path.join(os.path.dirname(__file__), 'data', 'preopen_data.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(json.dumps(data, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch NSE preopen data')
    parser.add_argument('--selenium', action='store_true', help='Force use of Selenium instead of requests')
    parser.add_argument('--show', action='store_true', help='Show the raw preopen data after fetching')
    args = parser.parse_args()

    # Fetch the data
    try:
        fetch_and_save_preopen_data_selenium(use_selenium=args.selenium)
        logging.info("Preopen data fetched successfully!")

        # Optionally show the data
        if args.show:
            print("\n" + "="*50)
            print("RAW PREOPEN DATA:")
            print("="*50 + "\n")
            show_raw_preopen_data()
    except Exception as e:
        logging.error(f"Failed to fetch preopen data: {e}")
        sys.exit(1)


