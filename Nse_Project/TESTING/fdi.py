import feedparser
import pandas as pd
import urllib.parse

# Define ANSI escape codes for beautiful terminal formatting
GREEN = "\033[92m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"

print(f"{BOLD}========================================================================={RESET}")
print(f"{BOLD}          GENUINE INDIAN STOCK DISCLOSURES (LIVE RSS FEED)              {RESET}")
print(f"{BOLD}=========================================================================\n")

# Clean, modern search query targeting real Indian exchange disclosures
search_query = 'NSE (bulk deal OR insider trading OR FII OR DII)'

# Use the canonical Google News RSS search endpoint and build query safely.
feed_params = {
    "q": search_query,
    "hl": "en-IN",
    "gl": "IN",
    "ceid": "IN:en",
}
FEED_URL = "https://news.google.com/rss/search?" + urllib.parse.urlencode(feed_params)

try:
    # Safely parse the live data array
    feed = feedparser.parse(FEED_URL)

    if feed.entries and len(feed.entries) > 0:
        print(f"{BOLD}⚡ LIVE INSTITUTIONAL ANNOUNCEMENTS & MARKET DISCLOSURES:{RESET}")
        print("-" * 85)

        html_rows = []

        # Pull the top 8 most recent corporate disclosure records
        for entry in feed.entries[:8]:
            title = entry.get('title', 'N/A')
            source = entry.get('source', {}).get('text', 'Market Source')
            date = entry.get('published', 'N/A')
            link = entry.get('link', '#')

            # Smart contextual highlights for tracking buy vs sell flows
            title_lower = title.lower()
            if any(word in title_lower for word in ["buy", "acquire", "invest", "picks up", "bought"]):
                color = GREEN
                action_type = "BUY / INFLOW"
            elif any(word in title_lower for word in ["sell", "dump", "dispose", "offloads", "sold"]):
                color = RED
                action_type = "SELL / OUTFLOW"
            else:
                color = RESET
                action_type = "MARKET UPDATE"

            # Print cleanly formatted rows directly to your PowerShell console
            print(f"{BOLD}[{action_type}]{RESET} {color}{title}{RESET}")
            print(f"   Source: {source} | Posted: {date}")
            print("-" * 85)

            # Build data grid elements for your HTML frontend dashboard module
            html_rows.append({
                "Action Type": action_type,
                "Market Announcement": title,
                "Publisher": source,
                "Filing Link": f"<a href='{link}' target='_blank' style='color:#10b981;'>Read Article</a>"
            })

        # Export data automatically to your web page file
        df_web = pd.DataFrame(html_rows)
        df_web.to_html("terminal_table.html", index=False, escape=False, classes="terminal-style-table")
        print(f"\n✔ Live corporate feed exported cleanly to 'terminal_table.html'")

    else:
        print("⚠ News registry returned empty rows. Double check internet connectivity.")

except Exception as e:
    print(f"❌ Failed to parse public announcement feeds: {e}")

print(f"\n{BOLD}========================================================================={RESET}")
