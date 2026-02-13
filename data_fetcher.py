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
from concurrent.futures import ThreadPoolExecutor
import re
import io


def _get_wiki_table(url: str, table_idx: int = 0) -> pd.DataFrame:
    """위키피디아 테이블 스크래핑 (User-Agent 헤더 적용)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=15)
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

def get_kospi200(limit: int = 200) -> list[dict]:
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


def _get_kospi200_fallback(limit: int = 200) -> list[dict]:
    major = [
        ("005930.KS","삼성전자"), ("000660.KS","SK하이닉스"), ("373220.KS","LG엔솔"),
        ("207940.KS","삼성바이오"), ("005380.KS","현대차"), ("005935.KS","삼성전자우"),
        ("000270.KS","기아"), ("006400.KS","삼성SDI"), ("051910.KS","LG화학"),
        ("035420.KS","NAVER"), ("005490.KS","POSCO홀딩스"), ("035720.KS","카카오"),
        ("068270.KS","셀트리온"), ("028260.KS","삼성물산"), ("012330.KS","현대모비스"),
        ("105560.KS","KB금융"), ("055550.KS","신한지주"), ("096770.KS","SK이노베이션"),
        ("032830.KS","삼성생명"), ("015760.KS","한국전력"), ("034730.KS","SK"),
        ("003550.KS","LG"), ("017670.KS","SK텔레콤"), ("086790.KS","하나금융지주"),
        ("316140.KS","우리금융지주"), ("018260.KS","삼성에스디에스"), ("000810.KS","삼성화재")
    ]
    return [{"ticker_yf": t, "ticker_display": t.split(".")[0], "name": n, "market": "Korea"} for t, n in major[:limit]]


def get_sp500(limit: int = 200) -> list[dict]:
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
        major = [("AAPL","Apple"), ("MSFT","Microsoft"), ("GOOGL","Google"), ("AMZN","Amazon"), ("NVDA","Nvidia"), ("META","Meta"), ("TSLA","Tesla")]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"} for t, n in major[:limit]]


def get_nasdaq100(limit: int = 200) -> list[dict]:
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


def get_nikkei225(limit: int = 200) -> list[dict]:
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
        
        if len(results) < 5:
            raise Exception("Scraping results too small")
            
        return results[:limit]
    except Exception:
        # 확장된 니케이 폴백 (일본 우량주 중심)
        major = [
            ("7203.T","Toyota"), ("6758.T","Sony"), ("9984.T","SoftBank"), ("6861.T","Keyence"),
            ("8035.T","Tokyo Electron"), ("6098.T","Recruit"), ("9432.T","NTT"), ("4502.T","Takeda"),
            ("8306.T","MUFG"), ("6501.T","Hitachi"), ("4063.T","Shin-Etsu"), ("6367.T","Daikin"),
            ("7751.T","Canon"), ("6954.T","Fanuc"), ("4519.T","Chugai"), ("6981.T","Murata"),
            ("7267.T","Honda"), ("8058.T","Mitsubishi Corp"), ("8001.T","Itochu"), ("2914.T","JT"),
            ("4452.T","Kao"), ("9022.T","JR Central"), ("6702.T","Fujitsu"), ("6723.T","Renesas"),
            ("8316.T","SMFG"), ("9983.T","Fast Retailing"), ("4661.T","Oriental Land"), ("6902.T","Denso"),
            ("8766.T","Tokio Marine"), ("6594.T","Nidec"), ("4901.T","Fujifilm"), ("6762.T","TDK"),
            ("7269.T","Suzuki"), ("5108.T","Bridgestone"), ("7011.T","MHI"), ("4568.T","Daiichi Sankyo"),
            ("4543.T","Terumo"), ("6301.T","Komatsu"), ("9020.T","JR East"), ("8031.T","Mitsui & Co"),
            ("8801.T","Mitsui Fudosan"), ("8802.T","Mitsubishi Estate"), ("3382.T","7&i"), ("7974.T","Nintendo"),
            ("4578.T","Otsuka"), ("4523.T","Eisai"), ("6869.T","Sysmex"), ("9101.T","NYK"), ("9104.T","MOL")
        ]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "Japan"} for t, n in major[:limit]]


def get_eurostoxx50(limit: int = 200) -> list[dict]:
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
        
        if len(results) < 5:
            raise Exception("Scraping results too small")
            
        return results[:limit]
    except Exception:
        # 확장된 유럽 폴백 (Eurozone 우량주 중심)
        major = [
            ("ASML.AS","ASML"), ("MC.PA","LVMH"), ("SAP.DE","SAP"), ("SIE.DE","Siemens"),
            ("TTE.PA","TotalEnergies"), ("SAN.MC","Santander"), ("LIN.DE","Linde"), ("OR.PA","L'Oreal"),
            ("AIR.PA","Airbus"), ("RMS.PA","Hermes"), ("ALV.DE","Allianz"), ("ABI.BE","Anheuser-Busch"),
            ("DTE.DE","Deutsche Telekom"), ("MBG.DE","Mercedes-Benz"), ("VOW3.DE","Volkswagen"),
            ("BASF.DE","BASF"), ("BMW.DE","BMW"), ("BAYN.DE","Bayer"), ("MUV2.DE","Munich Re"),
            ("ADS.DE","Adidas"), ("IFX.DE","Infineon"), ("DHL.DE","DHL"), ("CRG.IR","CRH"),
            ("BBVA.MC","BBVA"), ("ITX.MC","Inditex"), ("IBE.MC","Iberdrola"), ("ENI.MI","Eni"),
            ("ISP.MI","Intesa Sanpaolo"), ("ENEL.MI","Enel"), ("RACE.MI","Ferrari"), ("PRY.MI","Prysmian"),
            ("PRX.AS","Prosus"), ("AD.AS","Ahold Delhaize"), ("ING.AS","ING"), ("PHIA.AS","Philips"),
            ("KNEBV.HE","Kone"), ("NOKIA.HE","Nokia"), ("SAF.PA","Safran"), ("CS.PA","AXA"),
            ("BNP.PA","BNP Paribas"), ("GLE.PA","Societe Generale"), ("KER.PA","Kering"),
            ("EL.PA","EssilorLuxottica"), ("AI.PA","Air Liquide"), ("DG.PA","Vinci"), ("BN.PA","Danone")
        ]
        return [{"ticker_yf": t, "ticker_display": d, "name": d, "market": "Europe"} for t, d in major[:limit]]


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
            df = fetch_stock_data([{"ticker_yf": sym, "ticker_display": sym, "name": sym, "market": "Global"}])
            if not df.empty and "price" in df.columns and df["price"].iloc[0] > 0:
                return df
        except:
            continue
            
    return pd.DataFrame()


def resolve_ticker_from_name(query: str) -> str:
    """한글 종목명인 경우 KRX 리스트에서 종목코드를 찾습니다."""
    if re.match(r'^[A-Za-z0-9\.\-]+$', query):
        return query
        
    try:
        df_krx = fdr.StockListing('KRX')
        matched = df_krx[df_krx['Name'] == query]
        if not matched.empty:
            return matched.iloc[0]['Code']
            
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
    
    total = len(stock_list)
    results = []
    start_time_all = time.time()
    
    # 200개 규모 수집을 위해 batch_size 및 병렬성 조절
    batch_size = 20
    for i in range(0, total, batch_size):
        chunk = stock_list[i : i + batch_size]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            chunk_results = list(executor.map(_fetch_one, chunk))
            results.extend([r for r in chunk_results if r])
            
        processed = min(i + batch_size, total)
        if progress_callback:
            elapsed = time.time() - start_time_all
            avg_time = elapsed / processed if processed > 0 else 0
            remain = (total - processed) * avg_time
            eta_str = f"{remain:.0f}초" if remain > 60 else f"{remain:.1f}초"
            
            current_name = chunk[-1]['name']
            progress_callback(processed / total, f"({processed}/{total}) {current_name} 등 수집 중... (남은 시간: 약 {eta_str})")
        
        time.sleep(0.1) # 속도 향상을 위해 지연 시간 단축

    return pd.DataFrame(results)


def _fetch_one(meta: dict) -> dict:
    ticker = meta["ticker_yf"]
    try:
        t = yf.Ticker(ticker)
        fi = t.fast_info
        price = fi.last_price if hasattr(fi, "last_price") else 0
        mcap = fi.market_cap if hasattr(fi, "market_cap") else 0
        currency = fi.currency if hasattr(fi, "currency") else "USD"

        if not price or not mcap:
             info = t.info
             price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
             mcap = info.get("marketCap") or 0
             currency = info.get("currency", "USD")
        else:
             info = t.info

        ocf = info.get("operatingCashFlow")
        net_income = info.get("netIncomeToCommon")
        depreciation = None

        if ocf is None or net_income is None:
             try:
                 cf_df = t.cash_flow
                 if not cf_df.empty:
                     if ocf is None:
                         for name in ["Operating Cash Flow", "Total Cash From Operating Activities"]:
                             if name in cf_df.index:
                                 ocf = cf_df.loc[name].iloc[0]
                                 break
                     for name in ["Depreciation", "Depreciation And Amortization"]:
                         if name in cf_df.index:
                             depreciation = cf_df.loc[name].iloc[0]
                             break
             except:
                 pass

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
            "revenue_growth": info.get("revenueGrowth", 0),
            "earnings_growth": info.get("earningsGrowth", 0),
            "ttm_ocf": ocf,
            "ttm_net_income": net_income,
            "ttm_depreciation": depreciation
        }
    except Exception:
        return None


def get_history(ticker: str, period: str = "2y") -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        return t.history(period=period, auto_adjust=True)
    except Exception:
        return pd.DataFrame()
