"""
data_fetcher.py — 글로벌 주식 데이터 수집 모듈 (v2)
속도 최적화: yf.download() 배치 + 개별 Ticker CF 보완
"""

import time
import warnings
import pandas as pd
import numpy as np
import yfinance as yf

warnings.filterwarnings("ignore")


# ============================================================
# 1. 지수별 구성종목 리스트
# ============================================================

def get_kospi200(limit: int = 30) -> list[dict]:
    """KOSPI 200 구성종목 (FinanceDataReader 우선, 폴백 하드코딩)."""
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KRX-KOSPI")
        if df is None or df.empty:
            df = fdr.StockListing("KOSPI")
        if df is None or df.empty:
            return _get_kospi200_fallback(limit)

        marcap_col = None
        for col in ["Marcap", "MarCap", "Market Cap", "시가총액", "marcap"]:
            if col in df.columns:
                marcap_col = col
                break
        if marcap_col:
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
        return results if results else _get_kospi200_fallback(limit)
    except Exception:
        return _get_kospi200_fallback(limit)


def _get_kospi200_fallback(limit: int = 30) -> list[dict]:
    """KOSPI 주요 종목 하드코딩 폴백."""
    major = [
        ("005930.KS", "005930", "삼성전자"), ("000660.KS", "000660", "SK하이닉스"),
        ("373220.KS", "373220", "LG에너지솔루션"), ("207940.KS", "207940", "삼성바이오로직스"),
        ("005380.KS", "005380", "현대자동차"), ("006400.KS", "006400", "삼성SDI"),
        ("051910.KS", "051910", "LG화학"), ("000270.KS", "000270", "기아"),
        ("035420.KS", "035420", "NAVER"), ("005490.KS", "005490", "POSCO홀딩스"),
        ("055550.KS", "055550", "신한지주"), ("105560.KS", "105560", "KB금융"),
        ("035720.KS", "035720", "카카오"), ("003670.KS", "003670", "포스코퓨처엠"),
        ("012330.KS", "012330", "현대모비스"), ("066570.KS", "066570", "LG전자"),
        ("028260.KS", "028260", "삼성물산"), ("003550.KS", "003550", "LG"),
        ("032830.KS", "032830", "삼성생명"), ("086790.KS", "086790", "하나금융지주"),
        ("034730.KS", "034730", "SK"), ("138040.KS", "138040", "메리츠금융지주"),
        ("096770.KS", "096770", "SK이노베이션"), ("010130.KS", "010130", "고려아연"),
        ("030200.KS", "030200", "KT"), ("033780.KS", "033780", "KT&G"),
        ("018260.KS", "018260", "삼성에스디에스"), ("009150.KS", "009150", "삼성전기"),
        ("011200.KS", "011200", "HMM"), ("036570.KS", "036570", "엔씨소프트"),
        ("017670.KS", "017670", "SK텔레콤"), ("316140.KS", "316140", "우리금융지주"),
        ("003490.KS", "003490", "대한항공"), ("010950.KS", "010950", "S-Oil"),
        ("024110.KS", "024110", "기업은행"), ("011170.KS", "011170", "롯데케미칼"),
        ("009540.KS", "009540", "HD한국조선해양"), ("042700.KS", "042700", "한미반도체"),
        ("000810.KS", "000810", "삼성화재"), ("015760.KS", "015760", "한국전력"),
    ]
    return [{"ticker_yf": t, "ticker_display": d, "name": n, "market": "Korea"} for t, d, n in major[:limit]]


def get_sp500(limit: int = 30) -> list[dict]:
    """S&P 500 구성종목 (Wikipedia 스크래핑)."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        tickers = df["Symbol"].tolist()[:limit]
        names = df["Security"].tolist()[:limit]
        return [{"ticker_yf": t.replace(".", "-"), "ticker_display": t, "name": n, "market": "USA"} for t, n in zip(tickers, names)]
    except Exception:
        fallback = [
            ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("GOOGL", "Alphabet"),
            ("AMZN", "Amazon"), ("NVDA", "NVIDIA"), ("META", "Meta"),
            ("TSLA", "Tesla"), ("BRK-B", "Berkshire Hathaway"), ("UNH", "UnitedHealth"),
            ("JNJ", "Johnson & Johnson"), ("JPM", "JPMorgan"), ("V", "Visa"),
            ("XOM", "Exxon Mobil"), ("PG", "Procter & Gamble"), ("MA", "Mastercard"),
            ("HD", "Home Depot"), ("CVX", "Chevron"), ("LLY", "Eli Lilly"),
            ("ABBV", "AbbVie"), ("MRK", "Merck"), ("AVGO", "Broadcom"),
            ("PEP", "PepsiCo"), ("COST", "Costco"), ("ADBE", "Adobe"),
            ("TMO", "Thermo Fisher"), ("CSCO", "Cisco"), ("CRM", "Salesforce"),
            ("ACN", "Accenture"), ("WMT", "Walmart"), ("NFLX", "Netflix"),
        ]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"} for t, n in fallback[:limit]]


def get_nasdaq100(limit: int = 30) -> list[dict]:
    """Nasdaq 100 구성종목."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        df = None
        for table in tables:
            if "Ticker" in table.columns or "Symbol" in table.columns:
                df = table
                break
        if df is None:
            df = tables[4] if len(tables) > 4 else tables[0]
        ticker_col = "Ticker" if "Ticker" in df.columns else "Symbol"
        name_col = "Company" if "Company" in df.columns else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        tickers = df[ticker_col].tolist()[:limit]
        names = df[name_col].tolist()[:limit]
        return [{"ticker_yf": t.replace(".", "-"), "ticker_display": t, "name": n, "market": "USA"} for t, n in zip(tickers, names)]
    except Exception:
        fallback = [("AAPL","Apple"),("MSFT","Microsoft"),("GOOGL","Alphabet"),("AMZN","Amazon"),("NVDA","NVIDIA"),("META","Meta"),("TSLA","Tesla"),("AVGO","Broadcom"),("COST","Costco"),("NFLX","Netflix")]
        return [{"ticker_yf": t, "ticker_display": t, "name": n, "market": "USA"} for t, n in fallback[:limit]]


def get_nikkei225(limit: int = 30) -> list[dict]:
    """Nikkei 225 주요 종목."""
    major = [
        ("7203.T","7203","Toyota"), ("6758.T","6758","Sony"), ("6861.T","6861","Keyence"),
        ("9984.T","9984","SoftBank Group"), ("8306.T","8306","MUFG"), ("6902.T","6902","Denso"),
        ("9433.T","9433","KDDI"), ("4063.T","4063","Shin-Etsu Chemical"),
        ("6098.T","6098","Recruit"), ("8035.T","8035","Tokyo Electron"),
        ("7741.T","7741","HOYA"), ("4568.T","4568","Daiichi Sankyo"),
        ("6501.T","6501","Hitachi"), ("7267.T","7267","Honda"),
        ("4502.T","4502","Takeda"), ("6367.T","6367","Daikin"),
        ("8316.T","8316","SMFG"), ("9432.T","9432","NTT"),
        ("6594.T","6594","Nidec"), ("7974.T","7974","Nintendo"),
        ("4519.T","4519","Chugai Pharma"), ("6762.T","6762","TDK"),
        ("6981.T","6981","Murata Mfg"), ("3382.T","3382","Seven & i"),
        ("8058.T","8058","Mitsubishi Corp"), ("8031.T","8031","Mitsui & Co"),
        ("2914.T","2914","Japan Tobacco"), ("8001.T","8001","ITOCHU"),
        ("4661.T","4661","Oriental Land"), ("9983.T","9983","Fast Retailing"),
        ("6954.T","6954","Fanuc"), ("6857.T","6857","Advantest"),
        ("4503.T","4503","Astellas Pharma"), ("6752.T","6752","Panasonic"),
        ("7751.T","7751","Canon"), ("6301.T","6301","Komatsu"),
        ("4507.T","4507","Shionogi"), ("8411.T","8411","Mizuho FG"),
        ("6305.T","6305","Hitachi Construction"), ("2801.T","2801","Kikkoman"),
    ]
    return [{"ticker_yf": t, "ticker_display": d, "name": n, "market": "Japan"} for t, d, n in major[:limit]]


def get_eurostoxx50(limit: int = 30) -> list[dict]:
    """Euro Stoxx 50 주요 종목 (유럽 대형주)."""
    major = [
        ("ASML.AS","ASML","ASML Holding"), ("MC.PA","MC","LVMH"),
        ("SAP.DE","SAP","SAP"), ("SIE.DE","SIE","Siemens"),
        ("TTE.PA","TTE","TotalEnergies"), ("OR.PA","OR","L'Oréal"),
        ("AIR.PA","AIR","Airbus"), ("SAN.PA","SAN","Sanofi"),
        ("ALV.DE","ALV","Allianz"), ("DTE.DE","DTE","Deutsche Telekom"),
        ("BNP.PA","BNP","BNP Paribas"), ("CS.PA","CS","AXA"),
        ("RMS.PA","RMS","Hermès"), ("CDI.PA","CDI","Christian Dior"),
        ("SU.PA","SU","Schneider Electric"), ("AI.PA","AI","Air Liquide"),
        ("EL.PA","EL","EssilorLuxottica"), ("BAS.DE","BAS","BASF"),
        ("ENEL.MI","ENEL","Enel"), ("ISP.MI","ISP","Intesa Sanpaolo"),
        ("IBE.MC","IBE","Iberdrola"), ("INGA.AS","INGA","ING Group"),
        ("MBG.DE","MBG","Mercedes-Benz"), ("BMW.DE","BMW","BMW"),
        ("ABI.BR","ABI","AB InBev"), ("AD.AS","AD","Ahold Delhaize"),
        ("MUV2.DE","MUV2","Munich Re"), ("DPW.DE","DPW","DHL Group"),
        ("PHIA.AS","PHIA","Philips"), ("VOW3.DE","VOW3","Volkswagen"),
    ]
    return [{"ticker_yf": t, "ticker_display": d, "name": n, "market": "Europe"} for t, d, n in major[:limit]]


# ============================================================
# 2. 데이터 수집 (속도 최적화)
# ============================================================

def fetch_stock_data(
    stock_list: list[dict],
    progress_callback=None,
    delay: float = 0.3,
) -> pd.DataFrame:
    """
    종목 리스트를 받아 yfinance로 재무 데이터를 수집.
    1단계: yf.download() 배치로 가격/시총 가져오기 (빠름)
    2단계: 개별 Ticker()로 현금흐름 데이터 보완 (느리지만 필수)
    """
    if not stock_list:
        return pd.DataFrame()

    tickers_yf = [s["ticker_yf"] for s in stock_list]
    ticker_map = {s["ticker_yf"]: s for s in stock_list}
    total = len(stock_list)

    # ---- 1단계: 배치 다운로드 (가격/시총 빠르게) ----
    total_start = time.time()
    if progress_callback:
        est_seconds = total * 1.2  # 종목당 약 1.2초 추정
        if est_seconds >= 60:
            est_str = f"예상 총 소요시간: 약 {est_seconds/60:.0f}분"
        else:
            est_str = f"예상 총 소요시간: 약 {est_seconds:.0f}초"
        progress_callback(0.05, f"기본정보 배치 다운로드 중... ({total}종목, {est_str})")

    batch_info = {}
    try:
        batch_tickers = " ".join(tickers_yf)
        tickers_obj = yf.Tickers(batch_tickers)
        for ticker_yf in tickers_yf:
            try:
                info = tickers_obj.tickers[ticker_yf].info
                if info and info.get("marketCap", 0) > 0:
                    batch_info[ticker_yf] = info
            except Exception:
                pass
    except Exception:
        pass

    if progress_callback:
        elapsed = time.time() - total_start
        progress_callback(0.3, f"기본정보 {len(batch_info)}/{total}개 확보 ({elapsed:.0f}초 경과). 현금흐름 수집 시작...")

    # ---- 2단계: 개별 현금흐름 데이터 ----
    records = []
    fetched = 0
    stage2_start = time.time()
    for i, ticker_yf in enumerate(tickers_yf):
        stock = ticker_map[ticker_yf]
        pct = 0.3 + 0.65 * (i / total)
        if progress_callback and i % 2 == 0:
            # ETA 계산
            elapsed = time.time() - stage2_start
            if i > 0:
                per_stock = elapsed / i
                remaining = per_stock * (total - i)
                if remaining >= 60:
                    eta_str = f"약 {remaining/60:.0f}분 {remaining%60:.0f}초 남음"
                else:
                    eta_str = f"약 {remaining:.0f}초 남음"
            else:
                eta_str = "계산 중..."
            progress_callback(pct, f"({i+1}/{total}) {stock['name']} 수집 중... ⏱️ {eta_str}")

        try:
            info = batch_info.get(ticker_yf)

            # 배치에서 못 가져온 경우 개별 시도
            if not info:
                try:
                    tk = yf.Ticker(ticker_yf)
                    info = tk.info or {}
                except Exception:
                    info = {}

            price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose", 0)
            market_cap = info.get("marketCap", 0)
            sector = info.get("sector", "Unknown")
            currency = info.get("currency", "")

            if not market_cap or market_cap <= 0:
                time.sleep(delay * 0.5)
                continue

            # ---- 현금흐름 추출 ----
            ttm_ocf, ttm_net_income, ttm_depreciation = np.nan, np.nan, np.nan
            revenue_history, cf_history = {}, {}

            try:
                tk = yf.Ticker(ticker_yf)

                # 분기별 CF (TTM)
                cf_q = tk.quarterly_cashflow
                if cf_q is not None and not cf_q.empty:
                    cf_q = cf_q.iloc[:, :4]  # 최근 4분기

                    # OCF: 여러 가능한 라벨명
                    for label in ["Operating Cash Flow", "Free Cash Flow",
                                  "Total Cash From Operating Activities",
                                  "Cash Flow From Continuing Operating Activities"]:
                        if label in cf_q.index:
                            val = cf_q.loc[label].dropna()
                            if len(val) > 0:
                                ttm_ocf = val.sum()
                                break

                    # Net Income
                    for label in ["Net Income From Continuing Operations",
                                  "Net Income", "Net Income From Continuing Operation Net Minority Interest"]:
                        if label in cf_q.index:
                            val = cf_q.loc[label].dropna()
                            if len(val) > 0:
                                ttm_net_income = val.sum()
                                break

                    # Depreciation
                    for label in ["Depreciation And Amortization", "Depreciation & Amortization",
                                  "Depreciation Amortization Depletion"]:
                        if label in cf_q.index:
                            val = cf_q.loc[label].dropna()
                            if len(val) > 0:
                                ttm_depreciation = abs(val.sum())
                                break

                # 연간 매출/CF (5년 추세용)
                try:
                    fins = tk.financials
                    if fins is not None and not fins.empty:
                        for label in ["Total Revenue", "Revenue"]:
                            if label in fins.index:
                                for col in fins.columns[:5]:
                                    year = col.year if hasattr(col, "year") else str(col)[:4]
                                    val = fins.loc[label, col]
                                    if pd.notna(val):
                                        revenue_history[int(year)] = float(val)
                                break

                    cf_annual = tk.cashflow
                    if cf_annual is not None and not cf_annual.empty:
                        for label in ["Operating Cash Flow", "Free Cash Flow",
                                      "Total Cash From Operating Activities",
                                      "Cash Flow From Continuing Operating Activities"]:
                            if label in cf_annual.index:
                                for col in cf_annual.columns[:5]:
                                    year = col.year if hasattr(col, "year") else str(col)[:4]
                                    val = cf_annual.loc[label, col]
                                    if pd.notna(val):
                                        cf_history[int(year)] = float(val)
                                break
                except Exception:
                    pass

            except Exception:
                pass

            # FFO proxy
            ttm_ffo_proxy = np.nan
            if not np.isnan(ttm_net_income) and not np.isnan(ttm_depreciation):
                ttm_ffo_proxy = ttm_net_income + ttm_depreciation

            records.append({
                "ticker_yf": ticker_yf,
                "ticker_display": stock["ticker_display"],
                "name": stock["name"],
                "market": stock["market"],
                "sector": sector,
                "price": price,
                "market_cap": market_cap,
                "currency": currency,
                "ttm_ocf": ttm_ocf,
                "ttm_net_income": ttm_net_income,
                "ttm_depreciation": ttm_depreciation,
                "ttm_ffo_proxy": ttm_ffo_proxy,
                "revenue_history": revenue_history,
                "cf_history": cf_history,
            })
            fetched += 1

        except Exception:
            pass

        time.sleep(delay)

    if progress_callback:
        progress_callback(1.0, f"✅ {fetched}/{total} 종목 수집 완료!")

    return pd.DataFrame(records)
