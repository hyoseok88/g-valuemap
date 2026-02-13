"""
data_fetcher.py — 다중 소스 주식 데이터 수집기
1) 지수별 종목 리스트 (FinanceDataReader, Wikipedia 크롤링, 폴백)
2) 개별 종목 검색
3) yfinance 재무 데이터 수집 및 멀티프로세싱
"""

import time
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import FinanceDataReader as fdr
import FinanceDataReader as fdr
from concurrent.futures import ThreadPoolExecutor
import re
import io


def _get_wiki_table(url: str, table_idx: int = 0) -> pd.DataFrame:
    """위키피디아 테이블 스크래핑 (User-Agent 헤더 적용)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        dfs = pd.read_html(io.StringIO(resp.text))
        
        # 만약 table_idx가 -1이면 모든 테이블 중 가장 적합한 것을 찾음
        if table_idx == -1:
            for df in dfs:
                cols = [str(c).lower() for c in df.columns]
                has_ticker = any(any(k in c for k in ['symbol', 'ticker', 'code', 'ticker symbol']) for c in cols)
                has_name = any(any(k in c for k in ['company', 'name', 'constituent', 'constituent name']) for c in cols)
                if has_ticker and has_name:
                    return df
            return dfs[0] if dfs else pd.DataFrame()

        if len(dfs) > table_idx:
            return dfs[table_idx]
    except Exception as e:
        print(f"Wiki scraping failed: {url} -> {e}")
    return pd.DataFrame()


# ============================================================
# 1. 지수별 종목 리스트 수집
# ============================================================

def get_kospi200(limit: int = 30) -> list[dict]:
    """KOSPI 200 (fdr 사용, 실패시 폴백)."""
    try:
        df = fdr.StockListing("KOSPI") 
        if df is None or df.empty:
            raise Exception("fdr returned empty")
            
        marcap_col = None
        for col in ["Marcap", "MarCap", "Market Cap", "시가총액", "marcap"]:
            if col in df.columns:
                marcap_col = col
                break
        
        if marcap_col:
            df[marcap_col] = pd.to_numeric(df[marcap_col], errors='coerce')
            df = df.sort_values(marcap_col, ascending=False)
        
        df = df.head(limit)

        results = []
        for _, row in df.iterrows():
            code = str(row.get("Code", row.get("Symbol", row.get("종목코드", "")))).strip()
            name = str(row.get("Name", row.get("종목명", ""))).strip()
            if len(code) == 6 and code.isdigit():
                results.append({
                    "ticker_yf": f"{code}.KS",
                    "ticker_display": code,
                    "name": name,
                    "market": "Korea",
                })
        
        if len(results) < 5:
            raise Exception("Too few results from fdr")
            
        return results

    except Exception:
        return _get_kospi200_fallback(limit)


def _get_kospi200_fallback(limit: int = 30) -> list[dict]:
    major = [
        ("005930.KS","삼성전자"), ("000660.KS","SK하이닉스"), ("373220.KS","LG엔솔"),
        ("207940.KS","삼성바이오"), ("005380.KS","현대차"), ("005935.KS","삼성전자우"),
        ("000270.KS","기아"), ("006400.KS","삼성SDI"), ("051910.KS","LG화학"),
        ("035420.KS","NAVER"), ("005490.KS","POSCO홀딩스"), ("035720.KS","카카오")
    ]
    return [{"ticker_yf": t, "ticker_display": t.split(".")[0], "name": n, "market": "Korea"} for t, n in major[:limit]]


def get_sp500(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        df = _get_wiki_table(url, 0)
        
        ticker_col = next((c for c in df.columns if 'symbol' in str(c).lower() or 'ticker' in str(c).lower()), df.columns[0])
        name_col = next((c for c in df.columns if 'security' in str(c).lower() or 'company' in str(c).lower()), df.columns[1])
        
        results = []
        for _, row in df.iterrows():
            t = str(row[ticker_col]).replace(".", "-")
            n = str(row[name_col])
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"
            })
        return results[:limit]
    except Exception:
        major = [("AAPL","Apple"), ("MSFT","Microsoft"), ("GOOGL","Google"), ("AMZN","Amazon"), ("NVDA","Nvidia")]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"} for t, n in major[:limit]]


def get_nasdaq100(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        df = _get_wiki_table(url, -1)
        
        ticker_col = next((c for c in df.columns if 'ticker' in str(c).lower() or 'symbol' in str(c).lower()), None)
        name_col = next((c for c in df.columns if 'company' in str(c).lower() or 'security' in str(c).lower()), None)
        
        results = []
        for _, row in df.iterrows():
            t = str(row[ticker_col]).replace(".", "-")
            n = str(row[name_col])
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"
            })
        return results[:limit]
    except Exception:
        return get_sp500(limit)


def get_nikkei225(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/Nikkei_225"
        df = _get_wiki_table(url, -1)
        
        ticker_col = next((c for c in df.columns if 'symbol' in str(c).lower() or 'ticker' in str(c).lower()), None)
        name_col = next((c for c in df.columns if 'company' in str(c).lower() or 'constituent' in str(c).lower()), None)
        
        results = []
        for _, row in df.iterrows():
            t = str(row[ticker_col])
            n = str(row[name_col])
            if t.isdigit():
                t = f"{t}.T"
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "Japan"
            })
        return results[:limit]
    except Exception:
        major = [
            ("7203.T","Toyota"), ("6758.T","Sony"), ("9984.T","SoftBank"), ("6861.T","Keyence"),
            ("8035.T","Tokyo Electron"), ("6098.T","Recruit"), ("9432.T","NTT"), ("4502.T","Takeda")
        ]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "Japan"} for t, n in major[:limit]]


def get_eurostoxx50(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/EURO_STOXX_50"
        df = _get_wiki_table(url, -1)
        
        ticker_col = next((c for c in df.columns if 'ticker' in str(c).lower() or 'symbol' in str(c).lower()), None)
        name_col = next((c for c in df.columns if 'name' in str(c).lower() or 'company' in str(c).lower()), None)
            
        results = []
        for _, row in df.iterrows():
            t = str(row[ticker_col])
            n = str(row[name_col])
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "Europe"
            })
        return results[:limit]
    except Exception:
        major = [
            ("ASML.AS","ASML"), ("MC.PA","LVMH"), ("SAP.DE","SAP"), ("SIE.DE","Siemens"),
            ("TTE.PA","TotalEnergies"), ("SAN.MC","Santander"), ("LIN.DE","Linde"), ("OR.PA","L'Oreal")
        ]
        return [{"ticker_yf": t, "ticker_display": d, "name": d, "market": "Europe"} for t, d in major[:limit]]


def get_csi300(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/CSI_300_Index"
        df = _get_wiki_table(url, -1)
        
        ticker_col = next((c for c in df.columns if 'ticker' in str(c).lower() or 'code' in str(c).lower()), None)
        name_col = next((c for c in df.columns if 'stock' in str(c).lower() or 'company' in str(c).lower()), None)
        
        results = []
        for _, row in df.iterrows():
            t = str(row[ticker_col]).strip()
            if len(t) == 6:
                if t.startswith('6'): t = f"{t}.SS"
                else: t = f"{t}.SZ"
            n = str(row[name_col])
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "China"
            })
        return results[:limit]
    except Exception:
        major = [("600519.SS","Kweichow Moutai"), ("000858.SZ","Wuliangye"), ("601318.SS","Ping An Insurance")]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "China"} for t, n in major[:limit]]



def fetch_single_stock(query: str) -> pd.DataFrame:
    if not query:
        return pd.DataFrame()
        
    query = resolve_ticker_from_name(query)
    q = query.strip().upper()
    candidates = [q]
    
    if q.isdigit() and len(q) == 6:
        # 한국: 코스피(.KS) 먼저, 없으면 코스닥(.KQ)
        candidates = [f"{q}.KS", f"{q}.KQ"] 
    elif q.isdigit() and len(q) == 4:
         candidates = [f"{q}.T", f"{q}.HK"] 

    for sym in candidates:
        try:
            # detailed=True를 설정하여 OCF 누락 시 DataFrame까지 조회하도록 함
            df = fetch_stock_data([{"ticker_yf": sym, "ticker_display": sym, "name": sym, "market": "Global", "detailed": True}])
            if not df.empty and "price" in df.columns and df["price"].iloc[0] > 0:
                return df
        except:
            continue
            
    return pd.DataFrame()


    return pd.DataFrame()


def resolve_ticker_from_name(query: str) -> str:
    """한글 종목명인 경우 KRX 리스트에서 종목코드를 찾습니다."""
    # 영어/숫자만 있으면 티커로 간주
    if re.match(r'^[A-Za-z0-9\.\-]+$', query):
        return query
        
    try:
        # 한글이 포함된 경우 KRX 종목 찾기
        df_krx = fdr.StockListing('KRX')
        # 정확히 일치하는 종목 찾기
        matched = df_krx[df_krx['Name'] == query]
        if not matched.empty:
            return matched.iloc[0]['Code']
            
        # 포함되는 종목 찾기 (첫 번째)
        matched = df_krx[df_krx['Name'].str.contains(query, na=False)]
        if not matched.empty:
            return matched.iloc[0]['Code']
    except Exception:
        pass
        
    return query


# ============================================================
# 2. 재무 데이터 수집 (yfinance)
# ============================================================

def fetch_stock_data(stock_list: list[dict], progress_callback=None) -> pd.DataFrame:
    if not stock_list:
        return pd.DataFrame()
    
    tickers = [s["ticker_yf"] for s in stock_list]
    batch_size = 10 # 20 -> 10 Reduced batch size
    results = []
    total = len(tickers)
    start_time_all = time.time()
    
    for i in range(0, total, batch_size):
        chunk_tkrs = tickers[i : i + batch_size]
        chunk_meta = stock_list[i : i + batch_size]
        
    # Max workers increased for performance (User Request)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_one, item): item for item in chunk_meta}
        for future in futures:
            res = future.result()
            if res:
                results.append(res)
    
    processed = min(i + batch_size, total)
    if progress_callback:
        elapsed = time.time() - start_time_all
        avg_time = elapsed / processed if processed > 0 else 0
        remain = (total - processed) * avg_time
        eta_str = f"{remain:.0f}초" if remain > 60 else f"{remain:.1f}초"
        
        current_name = chunk_meta[-1]['name']
        progress_callback(processed / total, f"({processed}/{total}) {current_name} 등 수집 중... (남은 시간: 약 {eta_str})")
    
    time.sleep(0.5) # Reduced sleep as we want speed

    return pd.DataFrame(results)


def _fetch_one(meta: dict) -> dict:
    ticker = meta["ticker_yf"]
    try:
        t = yf.Ticker(ticker)
        # Fast Info First for Price/Mcap (Speed)
        fi = t.fast_info
        price = fi.last_price if hasattr(fi, "last_price") else 0
        mcap = fi.market_cap if hasattr(fi, "market_cap") else 0
        currency = fi.currency if hasattr(fi, "currency") else "USD"

        # Fallback to info if fast_info missing
        if not price or not mcap:
             info = t.info
             if not price: price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
             if not mcap: mcap = info.get("marketCap") or 0
             if not currency: currency = info.get("currency", "USD")
        else:
             info = {} # Load info only if needed for sector/financials? 
                       # Wait, we need info for sector and simple financials.
                       # Triggering t.info is slow.
                       # But we need 'sector'.
             info = t.info

        
        # 현금흐름 (OCF) & 감가상각비 (Depreciation)
        ocf = info.get("operatingCashFlow") # 1st attempt
        depreciation = None
        net_income = info.get("netIncomeToCommon")

        # Robust Extraction from CashFlow DataFrame
        # If OCF missing OR for Depreciation extraction
        cf_df = None
        try:
             cf_df = t.cash_flow
        except:
             pass

        if cf_df is not None and not cf_df.empty:
            # 1. OCF Extraction (Enhanced)
            if ocf is None:
                possible_names = [
                    "Operating Cash Flow", 
                    "Total Cash From Operating Activities", 
                    "Cash Flow From Continuing Operating Activities",
                    "Cash Flow From Operating Activities"
                ]
                for name in possible_names:
                    if name in cf_df.index:
                        val = cf_df.loc[name].iloc[0] # Most recent
                        if pd.notna(val):
                            ocf = val
                            break
            
            # 2. Depreciation Extraction (For FFO Proxy)
            dep_names = [
                "Depreciation",
                "Depreciation And Amortization",
                "Depreciation & Amortization",
                "D&A"
            ]
            for name in dep_names:
                if name in cf_df.index:
                    val = cf_df.loc[name].iloc[0]
                    if pd.notna(val):
                        depreciation = val
                        break

        # 성장성 지표
        rev_growth = info.get("revenueGrowth", 0)
        earn_growth = info.get("earningsGrowth", 0)

        return {
            "ticker_yf": ticker,
            "ticker_display": meta.get("ticker_display", ticker),
            "name": meta.get("name", ticker),
            "market": meta.get("market", ""),
            "sector": info.get("sector", "Unknown"),
            "price": price,
            "currency": currency,
            "market_cap": mcap,
            "ocf": ocf,
            "fcf": info.get("freeCashFlow"), # Not critical
            "revenue_growth": rev_growth,
            "earnings_growth": earn_growth,
            "revenue_history": {},
            "cf_history": {},
            "ttm_ocf": ocf,
            "ttm_net_income": net_income,
            "ttm_depreciation": depreciation, 
            "ttm_ffo_proxy": None # Calculated in valuation.py
        }

    except Exception:
        return None


def get_history(ticker: str, period: str = "2y") -> pd.DataFrame:
    """주가 기록 가져오기 (주봉 차트용)."""
    try:
        t = yf.Ticker(ticker)
        # Auto adjust for splits
        hist = t.history(period=period, auto_adjust=True)
        return hist
    except Exception:
        return pd.DataFrame()
