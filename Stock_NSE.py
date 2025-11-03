
import requests
from bs4 import BeautifulSoup
import time
import sys
import threading
import datetime
from pymongo import MongoClient

# ---------------------------
# MongoDB Connection
# ---------------------------
client = MongoClient("mongodb://localhost:27017/")  # Update if your MongoDB is hosted elsewhere
db = client["stock_market"]  # Database name
collection = db["live_prices"]  # Collection name


def get_stock_info(ticker: str):
    """
    Given an NSE ticker symbol (e.g. "INFY", "TCS"),
    fetch the company name and latest price from Google Finance.
    Returns (company_name, price) or (None, None) if something fails.
    """
    url = f"https://www.google.com/finance/quote/{ticker}:NSE?hl=en&gl=in"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0.0.0 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"Error fetching page for {ticker}: status code {resp.status_code}", file=sys.stderr)
            return None, None
    except Exception as e:
        print(f"Error fetching {ticker}: {e}", file=sys.stderr)
        return None, None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract company name
    name_tag = soup.find("div", class_="zzDege")
    company_name = name_tag.text.strip() if name_tag else "N/A"

    # Extract latest price
    price_tag = soup.find("div", class_="YMlKec fxKbKc") or soup.find("div", attrs={"class": "YMlKec"})
    if price_tag:
        price_text = price_tag.text.strip()
        price_clean = price_text.replace('₹', '').replace(',', '')
        try:
            price = float(price_clean)
        except ValueError:
            price = None
    else:
        price = None

    return company_name, price


def fetch_and_store(ticker: str, results: list):
    """Thread worker function to fetch stock info and add to results list"""
    name, price = get_stock_info(ticker)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    record = {
        "timestamp": now,
        "ticker": ticker,
        "company_name": name if name else "N/A",
        "price": price if price else "N/A"
    }

    if name is None or price is None:
        print(f"[{now}] Could not fetch info for {ticker}.")
    else:
        print(f"[{now}] {name} ({ticker}) → ₹{price:.2f}")

    results.append(record)


def is_market_open():
    """Check if current time is between 9:15 AM and 3:30 PM on weekdays"""
    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


def main():
    tickers_input = input("Enter NSE tickers separated by commas (e.g. INFY,TCS,RELIANCE): ")
    tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]

    if not tickers:
        print("No tickers provided.")
        return

    print(f"Fetching info for {', '.join(tickers)} every 5 seconds. Data will be stored in MongoDB. Press Ctrl+C to stop.")

    try:
        while True:
            if is_market_open():
                results = []
                threads = []

                # Start threads for each ticker
                for ticker in tickers:
                    t = threading.Thread(target=fetch_and_store, args=(ticker, results))
                    threads.append(t)
                    t.start()

                # Wait for all threads to finish
                for t in threads:
                    t.join()

                # Insert results into MongoDB
                if results:
                    collection.insert_many(results)

                # Sleep for 5 seconds
                time.sleep(5)
            else:
                print("Market closed. Waiting...")
                time.sleep(60)  # check every 1 minute if market is open
    except KeyboardInterrupt:
        print("\nExiting on user interrupt.")


if __name__ == "__main__":
    main()
