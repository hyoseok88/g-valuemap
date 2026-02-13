import yfinance as yf

def test_oxy():
    t = yf.Ticker("OXY")
    info = t.info
    
    print("Ticker: OXY")
    print(f"Price: {info.get('currentPrice')}")
    print(f"OCF (operatingCashFlow): {info.get('operatingCashFlow')}")
    print(f"Total Cash From Operating Activities: {info.get('totalCashFromOperatingActivities')}")
    print(f"Free Cash Flow: {info.get('freeCashFlow')}")
    print(f"Net Income: {info.get('netIncomeToCommon')}")
    
    try:
        print("\nChecking DataFrame cashflow:")
        cf = t.cash_flow
        if not cf.empty:
            # Look for 'Total Cash From Operating Activities' or similar
            # Row name might vary: 'Total Cash From Operating Activities', 'Operating Cash Flow'
            print("Annual Cash Flow columns:", cf.columns)
            print("Annual Cash Flow index:", cf.index)
            if "Total Cash From Operating Activities" in cf.index:
                print("Annual OCF:", cf.loc["Total Cash From Operating Activities"].iloc[0])
            elif "Operating Cash Flow" in cf.index:
                print("Annual OCF:", cf.loc["Operating Cash Flow"].iloc[0])
            
        qcf = t.quarterly_cash_flow
        if not qcf.empty:
             print("Quarterly Cash Flow index:", qcf.index)
             if "Total Cash From Operating Activities" in qcf.index:
                print("Quarterly OCF (Latest):", qcf.loc["Total Cash From Operating Activities"].iloc[0])
             elif "Operating Cash Flow" in qcf.index:
                print("Quarterly OCF (Latest):", qcf.loc["Operating Cash Flow"].iloc[0])
                
    except Exception as e:
        print("Error fetching cashflow DF:", e)

    # Check if fast_info helps
    fi = t.fast_info
    print(f"Fast Info Last Price: {fi.last_price}")

if __name__ == "__main__":
    test_oxy()
