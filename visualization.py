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
    df_valid = df_show[df_show["pcf"].notna()].copy()
    df_na = df_show[df_show["pcf"].isna()].copy()

    all_labels = []
    all_parents = []
    all_values = []
    all_colors = []
    all_hovers = []

    # ---- 고정 P/CF 구간 색상 매핑 ----
    # ≤10: 저평가(Green), 10~15: 중립(Blue), 15~20: 약간 고평가(Orange), 20+: 고평가(Red)
    PCF_MIN = 0
    PCF_MAX = 30  # colorbar 표시 범위

    if not df_valid.empty:
        for _, r in df_valid.iterrows():
            pcf = r["pcf"]
            # 밸류에이션 등급 라벨
            if pcf <= 10:
                grade = "🟢저평가"
            elif pcf <= 15:
                grade = "🔵중립"
            elif pcf <= 20:
                grade = "🟠약간고평가"
            else:
                grade = "🔴고평가"
            all_labels.append(f"<b>{r['ticker_display']}</b><br>{r['pcf_display']} {grade}")
            all_parents.append("")
            # 크기 결정: 시총 vs 저평가순
            if size_by_undervalue and pd.notna(pcf) and pcf > 0:
                all_values.append(1.0 / pcf * 1e6)
            else:
                all_values.append(r["market_cap"])
            # 고정 구간 정규화 (0~30 → 0~1)
            norm = (pcf - PCF_MIN) / (PCF_MAX - PCF_MIN)
            norm = max(0, min(1, norm))
            all_colors.append(_interpolate_color(norm))
            all_hovers.append(_make_hover(r))

    # 음수 CF (hide_negative_cf=False일 때만)
    if not df_na.empty:
        for _, r in df_na.iterrows():
            all_labels.append(f"<b>{r['ticker_display']}</b><br>N/A")
            all_parents.append("")
            all_values.append(r["market_cap"] if not size_by_undervalue else 1000)
            all_colors.append(GREY_COLOR)
            all_hovers.append(_make_hover(r, is_na=True))

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


def _interpolate_color(t: float) -> str:
    """
    0~1 값을 고정 P/CF 구간 기반 색상으로 변환.
    0~0.33 (P/CF 0~10): Green (저평가)
    0.33~0.50 (P/CF 10~15): Blue (중립)
    0.50~0.67 (P/CF 15~20): Orange (약간 고평가)
    0.67~1.0 (P/CF 20~30): Red (고평가)
    """
    # 고정 구간에 맞춘 색상 그라데이션
    colors = [
        (0.00, (26, 150, 65)),     # 진한 녹색 (P/CF ~0)
        (0.17, (102, 189, 99)),    # 밝은 녹색 (P/CF ~5)
        (0.33, (166, 217, 106)),   # 연녹색 (P/CF 10 경계)
        (0.40, (116, 169, 207)),   # 연파랑
        (0.50, (33, 102, 172)),    # 진파랑 (P/CF 15 중립)
        (0.57, (153, 112, 171)),   # 보라
        (0.67, (230, 160, 60)),    # 오렌지 (P/CF 20 경계)
        (0.80, (215, 48, 39)),     # 빨강
        (1.00, (165, 0, 38)),      # 진빨강 (P/CF 30+)
    ]
    t = max(0, min(1, t))
    for i in range(len(colors) - 1):
        t0, c0 = colors[i]
        t1, c1 = colors[i + 1]
        if t0 <= t <= t1:
            f = (t - t0) / (t1 - t0) if t1 > t0 else 0
            r = int(c0[0] + f * (c1[0] - c0[0]))
            g = int(c0[1] + f * (c1[1] - c0[1]))
            b = int(c0[2] + f * (c1[2] - c0[2]))
            return f"rgb({r},{g},{b})"
    return f"rgb({colors[-1][1][0]},{colors[-1][1][1]},{colors[-1][1][2]})"


def _make_hover(r, is_na=False) -> str:
    """호버 툴팁 생성."""
    price_str = _format_price(r.get("price", 0), r.get("currency", ""))
    mcap_str = _format_market_cap(r.get("market_cap", 0))

    if is_na:
        pcf_line = "⚠️ P/CF: N/A (음수 현금흐름)"
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
    valid = int(df["pcf"].notna().sum())
    neg = total - valid
    med = df["pcf"].median() if valid > 0 else None
    avg = df["pcf"].mean() if valid > 0 else None
    return {
        "total": total, "valid": valid, "negative_cf": neg,
        "negative_cf_pct": f"{neg/total*100:.1f}%" if total else "0%",
        "median_pcf": f"{med:.1f}x" if med else "N/A",
        "mean_pcf": f"{avg:.1f}x" if avg else "N/A",
    }
