"""
visualization.py — Plotly 트리맵 시각화 모듈 (v2)
단일 treemap trace + 음수 CF 필터링 지원
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go


# 커스텀 색상 스케일: Green(저평가) → Blue(중립) → Red(고평가)
CUSTOM_COLORSCALE = [
    [0.0, "#1a9641"],    # 진한 녹색
    [0.2, "#66bd63"],    # 녹색
    [0.35, "#a6d96a"],   # 연녹색
    [0.45, "#74a9cf"],   # 연파랑
    [0.5, "#2166ac"],    # 진파랑 (중립)
    [0.55, "#9970ab"],   # 연보라
    [0.7, "#e08070"],    # 연빨강
    [0.85, "#d73027"],   # 빨강
    [1.0, "#a50026"],    # 진빨강 (고평가)
]

GREY_COLOR = "#b0b0b0"


def build_treemap(
    df: pd.DataFrame,
    title: str = "",
    hide_negative_cf: bool = True,
    size_by_undervalue: bool = False,
) -> go.Figure:
    """
    밸류에이션 트리맵 생성.

    Args:
        df: process_dataframe()을 거친 DataFrame
        title: 트리맵 제목
        hide_negative_cf: True면 음수 CF 종목 제외
        size_by_undervalue: True면 저평가(1/P×CF) 기준 크기, False면 시총 기준
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="📭 데이터 없음", x=0.5, y=0.5, showarrow=False, font_size=28)
        fig.update_layout(height=500, paper_bgcolor="#1a1a2e")
        return fig

    df = df[df["market_cap"] > 0].copy()

    # 음수 CF 필터링
    if hide_negative_cf:
        df_show = df[df["pcf"].notna()].copy()
    else:
        df_show = df.copy()

    if df_show.empty:
        fig = go.Figure()
        fig.add_annotation(text="📭 유효한 종목 없음 (모두 음수 CF)", x=0.5, y=0.5, showarrow=False, font_size=20)
        fig.update_layout(height=500, paper_bgcolor="#1a1a2e")
        return fig

    # ---- 색상 값 준비 ----
    # ---- 색상 값 준비 ----
    df_valid = df_show.copy()
    
    all_labels = []
    all_parents = []
    all_values = []
    all_colors = []
    all_hovers = []

    # ---- 고정 P/CF 구간 색상 매핑 ----
    PCF_MIN = 0
    PCF_MAX = 30 
    
    # 색상 추출을 위한 Valid Value 수집
    valid_indices = []
    valid_norms = []
    
    from plotly.colors import sample_colorscale
    
    for idx, r in df_valid.iterrows():
        pcf = r["pcf"]
        
        # 1. PCF 상태별 라벨/색상/값 결정
        if pd.isna(pcf) or pcf <= 0:
            # N/A 또는 음수 (적자/데이터없음) -> 회색
            grade = "음수/N/A"
            color = GREY_COLOR
            # 크기: 저평가모드면 작게(1000), 아니면 시총
            val = r["market_cap"] if not size_by_undervalue else 1000
            
            all_labels.append(f"<b>{r['ticker_display']}</b><br>N/A")
            all_parents.append("")
            all_values.append(val)
            all_colors.append(color)
            all_hovers.append(_make_hover(r, is_na=True))
            
        else:
            # Valid Positive PCF
            if pcf <= 10: grade = "🟢저평가"
            elif pcf <= 15: grade = "🔵중립"
            elif pcf <= 20: grade = "🟠약간고평가"
            else: grade = "🔴고평가"
            
            all_labels.append(f"<b>{r['ticker_display']}</b><br>{r['pcf_display']} {grade}")
            all_parents.append("")
            
            # 크기: 시총 vs 저평가
            if size_by_undervalue:
                all_values.append(1.0 / pcf * 1e6)
            else:
                all_values.append(r["market_cap"])
            
            all_hovers.append(_make_hover(r))
            
            # 색상 계산을 위해 인덱스 저장 (나중에 한꺼번에 변환)
            norm = (pcf - PCF_MIN) / (PCF_MAX - PCF_MIN)
            norm = max(0, min(1, norm))
            valid_norms.append(norm)
            # Placeholder for color (will be filled later)
            all_colors.append(None) 
            valid_indices.append(len(all_colors) - 1)

    # 2. Valid Norms -> Colors 변환 (Batch)
    if valid_norms:
        # CUSTOM_COLORSCALE 포맷에 맞는 샘플링
        sampled_colors = sample_colorscale(CUSTOM_COLORSCALE, valid_norms)
        for i, idx in enumerate(valid_indices):
            all_colors[idx] = sampled_colors[i]

    # ---- 단일 Treemap ----
    fig = go.Figure(go.Treemap(
        labels=all_labels,
        parents=all_parents,
        values=all_values,
        marker=dict(
            colors=all_colors,
            line=dict(width=2, color="#1a1a2e"),
        ),
        text=all_hovers,
        hoverinfo="text",
        textposition="middle center",
        textfont=dict(size=13, color="white", family="Arial Black"),
        pathbar=dict(visible=False),
    ))

    # 수동 colorbar (고정 범위)
    fig.add_trace(go.Scatter(
        x=[None], y=[None], mode="markers",
        marker=dict(
            colorscale=CUSTOM_COLORSCALE,
            cmin=PCF_MIN,
            cmax=PCF_MAX,
            colorbar=dict(
                title=dict(text="P/CF", font=dict(size=14, color="#ccc")),
                tickvals=[5, 10, 15, 20, 25],
                ticktext=["5x\n저평가", "10x", "15x\n중립", "20x", "25x\n고평가"],
                tickfont=dict(size=10, color="#ccc"),
                len=0.75, thickness=18, x=1.01,
                bgcolor="rgba(26,26,46,0.8)",
                bordercolor="#444",
            ),
            showscale=True,
        ),
        hoverinfo="none",
        showlegend=False,
    ))

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=18, color="#e8e8ff", family="Arial Black"),
            x=0.5,
        ),
        margin=dict(t=50, l=8, r=8, b=8),
        height=650,
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font=dict(family="Arial", color="#ccc"),
    )

    return fig


def _make_hover(r, is_na=False) -> str:
    """호버 툴팁 생성."""
    price_str = _format_price(r.get("price", 0), r.get("currency", ""))
    mcap_str = _format_market_cap(r.get("market_cap", 0))

    if is_na:
        pcf_line = "⚠️ P/CF: N/A 또는 음수 (적자/데이터부족)"
    else:
        pcf_line = f"📈 P/CF: {r.get('pcf_display', 'N/A')} ({r.get('cf_method', 'OCF')})"

    return (
        f"<b>{r.get('name','')}</b> ({r.get('ticker_display','')})<br>"
        f"─────────────────<br>"
        f"💰 현재가: {price_str}<br>"
        f"📊 시가총액: {mcap_str}<br>"
        f"─────────────────<br>"
        f"{pcf_line}<br>"
        f"─────────────────<br>"
        f"📉 5Y 매출: {r.get('revenue_trend', 'N/A')}<br>"
        f"💵 5Y CF: {r.get('cf_trend', 'N/A')}"
    )


def _format_price(price, currency: str = "") -> str:
    if not price or price == 0:
        return "N/A"
    c = str(currency).upper()
    if c in ("KRW", "JPY", "CNY"):
        return f"{c} {price:,.0f}"
    return f"{c} {price:,.2f}" if c else f"{price:,.2f}"


def _format_market_cap(mc) -> str:
    if not mc or mc <= 0:
        return "N/A"
    if mc >= 1e12: return f"${mc/1e12:,.1f}T"
    if mc >= 1e9:  return f"${mc/1e9:,.1f}B"
    if mc >= 1e6:  return f"${mc/1e6:,.1f}M"
    return f"${mc:,.0f}"


def get_summary_stats(df: pd.DataFrame) -> dict:
    total = len(df)
    # Valid: P/CF > 0
    valid_count = len(df[ (df["pcf"].notna()) & (df["pcf"] > 0) ])
    # Negative/Null
    neg = total - valid_count
    
    # Stats for Valid only
    valid_df = df[ (df["pcf"].notna()) & (df["pcf"] > 0) ]
    med = valid_df["pcf"].median() if not valid_df.empty else None
    avg = valid_df["pcf"].mean() if not valid_df.empty else None
    
    return {
        "total": total, "valid": valid_count, "negative_cf": neg,
        "negative_cf_pct": f"{neg/total*100:.1f}%" if total else "0%",
        "median_pcf": f"{med:.1f}x" if med else "N/A",
        "mean_pcf": f"{avg:.1f}x" if avg else "N/A",
    }


def plot_weekly_chart(hist: pd.DataFrame, title: str = "") -> go.Figure:
    """
    주가 데이터를 주봉(Weekly)으로 변환하여 캔들차트 그리기.
    Args:
        hist: Daily OHLC DataFrame (Index=Date, Columns=[Open, High, Low, Close, Volume])
    """
    if hist.empty:
        return go.Figure()

    # 1. 주봉 리샘플링 (금요일 기준)
    # yfinance history returns index as timezone-aware datetime usually.
    # We need to ensure logic handles it.
    
    # Resample logic
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }
    # Ensure columns exist
    avail_cols = {k: v for k, v in ohlc_dict.items() if k in hist.columns}
    
    if not avail_cols:
        return go.Figure()
        
    df_weekly = hist.resample('W-FRI').agg(avail_cols).dropna()

    if df_weekly.empty:
        return go.Figure()

    # 2. 캔들차트 생성
    fig = go.Figure(data=[go.Candlestick(
        x=df_weekly.index,
        open=df_weekly['Open'],
        high=df_weekly['High'],
        low=df_weekly['Low'],
        close=df_weekly['Close'],
        increasing_line_color='#26a69a', # Green
        decreasing_line_color='#ef5350' # Red
    )])

    # 3. 레이아웃 설정
    fig.update_layout(
        title=dict(
            text=title,
            y=0.9,
            x=0.5,
            xanchor='center',
            yanchor='top',
            font=dict(size=15, color="#ccc")
        ),
        height=400,
        margin=dict(t=30, b=10, l=10, r=10),
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font=dict(color="#ccc"),
        xaxis_rangeslider_visible=False, # Slider off for cleaner view
        xaxis=dict(
            showgrid=True, gridcolor='rgba(128,128,128,0.2)',
            title=""
        ),
        yaxis=dict(
            showgrid=True, gridcolor='rgba(128,128,128,0.2)',
            title=""
        )
    )

    return fig
