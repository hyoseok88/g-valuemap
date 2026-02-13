"""
valuation.py — 밸류에이션 계산 및 추세 분석 모듈
P/CF 비율 계산, FFO 우선 처리, 5년 추세 회귀분석
"""

import numpy as np
import pandas as pd
from scipy import stats


def calculate_pcf(row: pd.Series) -> float | None:
    """
    P/CF (Price-to-Cash-Flow) 비율 계산.

    규칙:
      1. 섹터가 'Real Estate'이면 FFO(순이익+감가상각) 우선 사용
      2. 그 외 또는 FFO 실패 → 영업활동현금흐름(OCF) 사용
      3. 현금흐름 ≤ 0 → None (회색 처리)
      4. 정상 → 시가총액 / 현금흐름
    """
    # 강제 형변환 함수
    def safe_float(v):
        try:
            return float(v) if pd.notna(v) else 0.0
        except:
            return 0.0

    market_cap = safe_float(row.get("market_cap", 0))
    if market_cap <= 0:
        return None

    sector = str(row.get("sector", "")).lower()
    ttm_ocf = safe_float(row.get("ttm_ocf", np.nan))
    ttm_ffo_proxy = safe_float(row.get("ttm_ffo_proxy", np.nan))

    # 1) 리츠/부동산 → FFO 우선
    cash_flow = np.nan
    cf_method = "OCF"
    if "real estate" in sector or "reit" in sector:
        if ttm_ffo_proxy > 0:
            cash_flow = ttm_ffo_proxy
            cf_method = "FFO"

    # 2) FFO 못 구하면 OCF
    if np.isnan(cash_flow):
        if ttm_ocf != 0: # safe_float returns 0.0 for NaN. But if original was 0?
                         # Wait, if original was 0, it is 0.
                         # If original was NaN, it is 0.
                         # OCF can be negative.
                         # If OCF is 0, we can't divide.
            cash_flow = ttm_ocf
            cf_method = "OCF"

    # 3) 현금흐름이 없거나 음수
    # safe_float(nan) is 0.
    if cash_flow <= 0:
        return None

    # 4) P/CF 계산
    return market_cap / cash_flow


def get_cf_method(row: pd.Series) -> str:
    """해당 종목의 현금흐름 계산 방식 반환."""
    sector = str(row.get("sector", "")).lower()
    ttm_ffo_proxy = row.get("ttm_ffo_proxy", np.nan)

    if ("real estate" in sector or "reit" in sector) and pd.notna(ttm_ffo_proxy) and ttm_ffo_proxy > 0:
        return "FFO"
    return "OCF"


def calculate_trend(history: dict, years: int = 5) -> str:
    """
    연간 데이터의 선형 회귀 기울기로 추세 판단.

    Args:
        history: {year: value} 딕셔너리
        years: 분석 연수

    Returns:
        "Uptrend ↗" / "Downtrend ↘" / "Flat ➡" / "N/A"
    """
    if not history or not isinstance(history, dict):
        return "N/A"

    # 연도순 정렬, 최근 N년만
    sorted_items = sorted(history.items(), key=lambda x: x[0])
    if len(sorted_items) < 2:
        return "N/A"

    sorted_items = sorted_items[-years:]

    x = np.array([i for i in range(len(sorted_items))], dtype=float)
    y = np.array([v for _, v in sorted_items], dtype=float)

    # NaN 제거
    mask = ~np.isnan(y)
    x, y = x[mask], y[mask]
    if len(x) < 2:
        return "N/A"

    try:
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    except Exception:
        return "N/A"

    # 평균 대비 기울기 비율
    mean_val = np.mean(np.abs(y))
    if mean_val == 0:
        return "Flat ➡"

    slope_pct = slope / mean_val

    if slope_pct > 0.05:
        return "Uptrend ↗"
    elif slope_pct < -0.05:
        return "Downtrend ↘"
    else:
        return "Flat ➡"


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame에 밸류에이션 및 추세 컬럼 추가.

    추가 컬럼:
      - pcf: P/CF 비율 (None이면 N/A)
      - cf_method: "OCF" 또는 "FFO"
      - revenue_trend: 5년 매출 추세
      - cf_trend: 5년 현금흐름 추세
      - pcf_display: 표시용 문자열
      - market_cap_b: 시총 (10억 단위)
    """
    if df.empty:
        return df

    df = df.copy()

    # P/CF 계산
    df["pcf"] = df.apply(calculate_pcf, axis=1)

    # CF 방법
    df["cf_method"] = df.apply(get_cf_method, axis=1)

    # 추세 분석 (History 우선, 없으면 YoY Growth 사용)
    def determine_trend(history, growth_rate):
        # 1. History 기반 추세
        trend = calculate_trend(history) if isinstance(history, dict) and history else "N/A"
        if trend != "N/A":
            return trend
        
        # 2. Growth Rate 기반 추세 (History 없는 경우)
        # 5% 이상 성장 시 Uptrend
        if pd.isna(growth_rate):
            return "N/A"
        
        if growth_rate > 0.05:
            return "Uptrend ↗"
        elif growth_rate < -0.05:
            return "Downtrend ↘"
        else:
            return "Flat ➡"

    df["revenue_trend"] = df.apply(
        lambda r: determine_trend(r.get("revenue_history"), r.get("revenue_growth")), 
        axis=1
    )
    df["cf_trend"] = df.apply(
        lambda r: determine_trend(r.get("cf_history"), r.get("earnings_growth")), 
        axis=1
    )

    # 표시용 컬럼
    df["pcf_display"] = df["pcf"].apply(
        lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A"
    )
    df["market_cap_b"] = df["market_cap"] / 1e9

    # 색상용 P/CF (None → NaN)
    df["pcf_color"] = df["pcf"].astype(float)

    return df
