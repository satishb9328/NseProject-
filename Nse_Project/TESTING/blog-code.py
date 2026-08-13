"""
Automated Screener for Fetching Trending Value stocks
Reference: Refer to Shankar Nath's GARP Investing video for detailed rules

-- Dependencies to be installed --
pip install beautifulsoup4==4.11.2
pip install openpyxl
pip install pandas

Disclaimer:
The information provided is for educational and informational purposes only and
should not be construed as financial, investment, or legal advice. The content is based on publicly available
information and personal opinions and may not be suitable for all investors. Investing involves risks,
including the loss of principal.
"""

import time
import pandas as pd


def fetch_screener_data(link):
    data = pd.DataFrame()
    current_page = 1
    page_limit = 100

    while current_page < page_limit:
        url = f"{link}?page={current_page}"
        all_tables = pd.read_html(url, flavor="bs4")
        combined_df = pd.concat(all_tables)

        combined_df = combined_df.drop(
            combined_df[combined_df["S.No."].isnull()].index
        )

        if len(combined_df.index) < 26:
            data = pd.concat([data, combined_df], axis=0)
            break

        data = pd.concat([data, combined_df], axis=0)
        current_page += 1
        time.sleep(3)

    data = data.iloc[0:].drop(data[data["S.No."] == "S.No."].index)
    return data


if __name__ == "__main__":
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    # Example saved screen URL
    screen_url = "https://www.screener.in/screens/2115435/garp3months/"
    df = fetch_screener_data(screen_url)
    print(df)