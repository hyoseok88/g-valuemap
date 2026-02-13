import FinanceDataReader as fdr
import pandas as pd

def test_fdr():
    print("Testing fdr.StockListing('KOSPI')...")
    try:
        df = fdr.StockListing("KOSPI")
        print(f"Result type: {type(df)}")
        if df is not None:
             print(f"Shape: {df.shape}")
             print("Columns:", df.columns)
             print("Head:", df.head())
        else:
             print("df is None")
    except Exception as e:
        print(f"Error listing KOSPI: {e}")
        import traceback
        traceback.print_exc()

    print("\nTesting fdr.StockListing('KRX')...")
    try:
        df = fdr.StockListing("KRX")
        if df is not None:
             print(f"Shape: {df.shape}")
        else:
             print("df is None")
    except Exception as e:
        print(f"Error listing KRX: {e}")

if __name__ == "__main__":
    test_fdr()
