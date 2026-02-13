import requests
import pandas as pd
import io

def debug_tables(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        dfs = pd.read_html(io.StringIO(resp.text))
        print(f"\nURL: {url}")
        for i, df in enumerate(dfs):
            print(f"[{i}] {df.columns.tolist()[:3]}... (Rows: {len(df)})")
    except Exception as e:
        print(f"Err: {e}")

if __name__ == "__main__":
    debug_tables("https://en.wikipedia.org/wiki/Nikkei_225")
    debug_tables("https://en.wikipedia.org/wiki/EURO_STOXX_50")
