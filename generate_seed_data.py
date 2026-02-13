import os
import pandas as pd
import data_fetcher as df
import time

def save_market_data(market_name, fetch_func, filename, limit=200):
    print(f"[{market_name}] Fetching list (limit={limit})...")
    try:
        # 1. Get List
        stock_list = fetch_func(limit)
        print(f"[{market_name}] Found {len(stock_list)} items.")
        
        # 2. Fetch Data
        print(f"[{market_name}] Fetching financial data...")
        df_result = df.fetch_stock_data(stock_list, progress_callback=lambda p, m: print(f"  {p*100:.1f}% {m}", end='\r'))
        print(f"\n[{market_name}] Data fetch complete. Shape: {df_result.shape}")
        
        # 3. Process Data (Add valuation metrics)
        if not df_result.empty:
            import valuation
            df_result = valuation.process_dataframe(df_result)
        
        # 4. Save to CSV (ONLY if valid data exists)
        if df_result.empty or len(df_result) < 10:
            print(f"[{market_name}] ⚠️ Data too small or empty ({len(df_result)} rows). Skipping save.")
            return

        os.makedirs("seeds", exist_ok=True)
        path = os.path.join("seeds", filename)
        df_result.to_csv(path, index=False)
        print(f"[{market_name}] Saved to {path} ({len(df_result)} rows)")
        
    except Exception as e:
        print(f"[{market_name}] Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("🚀 Starting Seed Data Generation...")
    
    # 1. Korea
    save_market_data("Korea", df.get_kospi200, "korea.csv", limit=200)
    
    # 2. USA
    # For USA, we combine SP500 and Nasdaq
    print("\n[USA] Fetching S&P 500 & Nasdaq 100...")
    sp500 = df.get_sp500(200)
    nasdaq = df.get_nasdaq100(100)
    # Merge and deduplicate
    combined = sp500 + nasdaq
    # Deduplicate by ticker
    seen = set()
    unique_list = []
    for item in combined:
        if item['ticker_yf'] not in seen:
            unique_list.append(item)
            seen.add(item['ticker_yf'])
    
    # Fetch
    print(f"[USA] Total unique tickers: {len(unique_list)}")
    df_usa = df.fetch_stock_data(unique_list, progress_callback=lambda p, m: print(f"  {p*100:.1f}% {m}", end='\r'))
    if not df_usa.empty:
        import valuation
        df_usa = valuation.process_dataframe(df_usa)
    os.makedirs("seeds", exist_ok=True)
    df_usa.to_csv("seeds/usa.csv", index=False)
    print(f"\n[USA] Saved to seeds/usa.csv")

    # 3. Japan
    save_market_data("Japan", df.get_nikkei225, "japan.csv", limit=200)

    # 4. Europe
    # Euro Stoxx 50 is literally 50 stocks, but we can try for 50 
    save_market_data("Europe", df.get_eurostoxx50, "europe.csv", limit=100)

    print("\n✅ All seed data generated in 'seeds/' folder.")

if __name__ == "__main__":
    main()
