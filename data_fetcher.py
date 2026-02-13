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


def _get_wiki_table(url: str, table_idx: int = 0) -> pd.DataFrame:
    """위키피디아 테이블 스크래핑 (User-Agent 헤더 적용)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        dfs = pd.read_html(resp.text)
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
        # 1. fdr 시도
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
        ("035420.KS","NAVER"), ("005490.KS","POSCO홀딩스"), ("035720.KS","카카오"),
        ("068270.KS","셀트리온"), ("028260.KS","삼성물산"), ("012330.KS","현대모비스"),
        ("105560.KS","KB금융"), ("055550.KS","신한지주"), ("096770.KS","SK이노베이션"),
        ("032830.KS","삼성생명"), ("015760.KS","한국전력"), ("034730.KS","SK"),
        ("003550.KS","LG"), ("017670.KS","SK텔레콤"), ("086790.KS","하나금융지주"),
        ("316140.KS","우리금융지주"), ("018260.KS","삼성에스디에스"), ("000810.KS","삼성화재"),
        ("329180.KS","HD현대중공업"), ("010130.KS","고려아연"), ("009150.KS","삼성전기"),
        ("010950.KS","S-Oil"), ("011200.KS","HMM"), ("003490.KS","대한항공"),
        ("034020.KS","두산에너빌리티"), ("323410.KS","카카오뱅크"), ("036570.KS","엔씨소프트"),
        ("259960.KS","크래프톤"), ("352820.KS","하이브"), ("011070.KS","LG이노텍"),
        ("024110.KS","기업은행"), ("090430.KS","아모레퍼시픽"), ("009540.KS","HD한국조선해양"),
        ("251270.KS","넷마블"), ("010140.KS","삼성중공업"), ("086280.KS","현대글로비스")
    ]
    return [{"ticker_yf": t, "ticker_display": t.split(".")[0], "name": n, "market": "Korea"} for t, n in major[:limit]]


def get_sp500(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        df = _get_wiki_table(url, 0)
        
        ticker_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
        name_col = "Security" if "Security" in df.columns else df.columns[1]
        
        tickers = df[ticker_col].tolist()
        names = df[name_col].tolist()
        
        results = []
        for t, n in zip(tickers, names):
            t = str(t).replace(".", "-")
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"
            })
        return results[:limit]
    except Exception:
        return [{"ticker_yf": "AAPL", "ticker_display": "AAPL", "name": "Apple", "market": "USA"}]


def get_nasdaq100(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        df = _get_wiki_table(url, 4)
        if df.empty or "Ticker" not in df.columns:
             df = _get_wiki_table(url, 3)
        
        ticker_col = "Ticker" if "Ticker" in df.columns else "Symbol"
        name_col = "Company" if "Company" in df.columns else "Security"
        
        if ticker_col not in df.columns:
             return get_sp500(limit)
             
        tickers = df[ticker_col].tolist()
        names = df[name_col].tolist()
        
        results = []
        for t, n in zip(tickers, names):
            t = str(t).replace(".", "-")
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"
            })
        return results[:limit]
    except Exception:
        return get_sp500(limit)


def get_nikkei225(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/Nikkei_225"
        df = _get_wiki_table(url, 3)
        if "Symbol" not in df.columns and "Ticker" not in df.columns:
            df = _get_wiki_table(url, 4)
            
        ticker_col = "Symbol" if "Symbol" in df.columns else ("Ticker" if "Ticker" in df.columns else None)
        name_col = "Company" if "Company" in df.columns else "Constituent"
        
        if not ticker_col:
            raise Exception("Column not found")

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
        major = [("7203.T","Toyota"), ("6758.T","Sony"), ("9984.T","SoftBank"), ("6861.T","Keyence")]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "Japan"} for t, n in major]


def get_eurostoxx50(limit: int = 30) -> list[dict]:
    try:
        url = "https://en.wikipedia.org/wiki/EURO_STOXX_50"
        df = _get_wiki_table(url, 3)
        if "Ticker" not in df.columns:
             df = _get_wiki_table(url, 4)

        ticker_col = "Ticker" if "Ticker" in df.columns else "Symbol"
        name_col = "Name" if "Name" in df.columns else "Company"
        
        if ticker_col not in df.columns:
            raise Exception("Column not found")
            
        results = []
        for _, row in df.iterrows():
            t = str(row[ticker_col])
            n = str(row[name_col])
            results.append({
                "ticker_yf": t, "ticker_display": t, "name": n, "market": "Europe"
            })
        return results[:limit]
    except Exception:
        major = [("ASML.AS","ASML"), ("MC.PA","LVMH"), ("SAP.DE","SAP"), ("SIE.DE","Siemens")]
        return [{"ticker_yf": t, "ticker_display": d, "name": d, "market": "Europe"} for t, d in major]


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
        
        # Max workers reduced to avoid rate limiting
        with ThreadPoolExecutor(max_workers=2) as executor:
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
        
        time.sleep(1.0) # 0.5 -> 1.0 Increased sleep 

    return pd.DataFrame(results)


def _fetch_one(meta: dict) -> dict:
    ticker = meta["ticker_yf"]
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        # 가격
        price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
        if not price:
             fi = t.fast_info
             price = fi.last_price if hasattr(fi, "last_price") else 0
        
        mcap = info.get("marketCap")
        if not mcap:
            fi = t.fast_info
            mcap = fi.market_cap if hasattr(fi, "market_cap") else 0
            
        currency = info.get("currency", "USD")
        
        # 현금흐름
        ocf = info.get("operatingCashFlow")
        fcf = info.get("freeCashFlow")
        
        # OCF Fallback (상세 모드일 때만)
        if (ocf is None) and meta.get("detailed", False):
            try:
                cf_df = t.cash_flow
                if not cf_df.empty:
                    # 행 이름이 조금씩 다를 수 있음
                    for row_name in ["Total Cash From Operating Activities", "Operating Cash Flow"]:
                         if row_name in cf_df.index:
                             val = cf_df.loc[row_name].iloc[0]
                             if val:
                                 ocf = val
                                 break
            except Exception:
                pass
        
        
        # 성장성 지표 (Trend 대체용)
        rev_growth = info.get("revenueGrowth", 0) # YoY
        earn_growth = info.get("earningsGrowth", 0) # YoY

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
            "fcf": fcf,
            "revenue_growth": rev_growth, # For rapid trend check
            "earnings_growth": earn_growth, # Proxy for CF trend
            # 호환성 유지
            "revenue_history": {},
            "cf_history": {},
            "ttm_ocf": ocf,
            "ttm_net_income": info.get("netIncomeToCommon"),
            "ttm_depreciation": None, 
            "ttm_ffo_proxy": None
        }

    except Exception:
        return None
