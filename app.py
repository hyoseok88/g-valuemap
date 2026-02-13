"""
G-Valuemap: Global Market Valuation TreeMap (v2.1)
================================================
글로벌 지수의 P/CF 밸류에이션 트리맵 대시보드.
수정사항: 중국 삭제, 종목수 200개 고정, 접속자 간 상태 공유(영속성).
"""

import streamlit as st
import pandas as pd
import os
import ast
import time

from data_fetcher import (
    get_kospi200, get_sp500, get_nasdaq100, get_nikkei225, get_eurostoxx50,
    fetch_stock_data, fetch_single_stock, get_history
)
from valuation import process_dataframe
from visualization import build_treemap, get_summary_stats, plot_weekly_chart
from disk_cache import load_cached, save_cache, is_stale, get_cache_age_str
from persistence import save_app_state, load_app_state

# ============================================================
# 페이지 설정
# ============================================================
st.set_page_config(
    page_title="G-Valuemap | Global Valuation",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 초기 영속 상태 로드
# ============================================================
shared_state = load_app_state()

# 세션 상태 초기화 (영속 데이터가 있으면 그것을 사용)
if "current_market" not in st.session_state:
    st.session_state.current_market = shared_state.get("current_market", "🇰🇷 한국 (KOSPI 200)")
if "last_search" not in st.session_state:
    st.session_state.last_search = shared_state.get("last_search", "")

# ============================================================
# CSS
# ============================================================
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(145deg, #0d0d1a 0%, #1a1a2e 40%, #16213e 100%);
        color: #e0e0e0;
    }
    .block-container { padding-top: 1rem; max-width: 1400px; }

    .header-box {
        background: linear-gradient(135deg, rgba(33,102,172,0.15), rgba(26,150,65,0.08));
        border: 1px solid rgba(100,140,255,0.2);
        border-radius: 14px;
        padding: 22px 30px;
        margin-bottom: 18px;
    }
    .header-box h1 {
        margin: 0 0 4px 0;
        font-size: 1.9rem;
        font-weight: 800;
        background: linear-gradient(90deg, #66bd63, #74a9cf, #d73027);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .header-box .sub { color: #8899bb; font-size: 0.9rem; margin: 0; }
    .header-box .method {
        display: inline-block;
        margin-top: 10px;
        padding: 6px 14px;
        background: rgba(33,102,172,0.2);
        border: 1px solid rgba(33,102,172,0.3);
        border-radius: 8px;
        color: #74a9cf;
        font-size: 0.8rem;
    }

    .stat-row { display: flex; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; }
    .stat-card {
        flex: 1; min-width: 110px;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 10px;
        padding: 12px 16px;
        text-align: center;
    }
    .stat-card .val { font-size: 1.4rem; font-weight: 700; color: #e8e8ff; }
    .stat-card .lbl { font-size: 0.72rem; color: #777; margin-top: 2px; }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #16213e, #0d0d1a);
    }
    [data-testid="stSidebar"] h3, [data-testid="stSidebar"] label { color: #bbb !important; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# 헤더
# ============================================================
st.markdown("""
<div class="header-box">
    <h1>🗺️ G-Valuemap</h1>
    <p class="sub">Global Market Valuation TreeMap — P/CF 기반 밸류에이션 대시보드 (v2.1)</p>
    <div class="method">
        📐 <b>계산 방식:</b> P/CF = 시가총액 ÷ TTM 현금흐름 &nbsp;|&nbsp;
        부동산·리츠: FFO 우선 &nbsp;|&nbsp;
        🟢 저평가 → 🔵 중립 → 🔴 고평가 &nbsp;|&nbsp;
        📢 <b>화면 공유:</b> 마지막 조회 상태가 모두에게 공개됩니다.
    </div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# 사이드바
# ============================================================
with st.sidebar:
    st.markdown("### ⚙️ 설정")
    
    # 검색 기능
    def on_search_change():
        st.session_state.last_search = st.session_state.search_input
        save_app_state({
            "current_market": st.session_state.current_market,
            "last_search": st.session_state.last_search
        })

    search_query = st.text_input(
        "🔍 종목 검색 (실시간 공유)",
        value=st.session_state.last_search,
        placeholder="티커/코드 (예: 005930, AAPL)",
        key="search_input",
        on_change=on_search_change,
        help="검색어는 다른 접속자에게도 실시간으로 공유됩니다."
    )
    
    if st.button("🗑️ 검색 초기화"):
        st.session_state.last_search = ""
        save_app_state({"current_market": st.session_state.current_market, "last_search": ""})
        st.rerun()

    st.markdown("---")
    
    # 종목 수 고정 (내부 변수 처리)
    limit = 200

    size_mode = st.radio(
        "📐 타일 크기 기준",
        ["시가총액 (Market Cap)", "저평가순 (1/P×CF)"],
        index=1,
        help="시총: 시총 큰 기업이 크게. 저평가순: P/CF 낮은(저평가) 기업이 크게 보임."
    )

    hide_neg = st.checkbox("음수 CF 종목 숨기기", value=True,
                           help="현금흐름이 마이너스인 종목을 트리맵에서 제외")

    st.markdown("---")
    
    if st.button("🔄 실시간 데이터 새로고침 (Live)"):
         st.cache_data.clear()
         st.rerun()
         
    st.markdown("### 🎨 P/CF 밸류에이션 기준")
    st.markdown("""
    <div style="padding:8px 4px;">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">
            <div style="width:14px;height:14px;background:#1a9641;border-radius:3px;"></div>
            <span style="color:#aaa;font-size:0.82rem;">🟢 저평가 (P/CF ≤ 10x)</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">
            <div style="width:14px;height:14px;background:#2166ac;border-radius:3px;"></div>
            <span style="color:#aaa;font-size:0.82rem;">🔵 중립 (P/CF 10~15x)</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">
            <div style="width:14px;height:14px;background:#e6a03c;border-radius:3px;"></div>
            <span style="color:#aaa;font-size:0.82rem;">🟠 약간 고평가 (P/CF 15~20x)</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">
            <div style="width:14px;height:14px;background:#a50026;border-radius:3px;"></div>
            <span style="color:#aaa;font-size:0.82rem;">🔴 고평가 (P/CF > 20x)</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
            <div style="width:14px;height:14px;background:#b0b0b0;border-radius:3px;"></div>
            <span style="color:#aaa;font-size:0.82rem;">⚪ 해당없음 (음수 현금흐름)</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# 데이터 수집 (디스크 캐시 + 실시간 갱신)
# ============================================================
FETCHERS = {
    "Korea": get_kospi200,
    "USA_SP500": get_sp500,
    "USA_NASDAQ": get_nasdaq100,
    "Japan": get_nikkei225,
    "Europe": get_eurostoxx50,
}

def _fetch_fresh(market: str, lim: int, progress_callback=None) -> pd.DataFrame:
    get_fn = FETCHERS.get(market)
    if not get_fn: return pd.DataFrame()
    stock_list = get_fn(lim)
    if not stock_list: return pd.DataFrame()
    df = fetch_stock_data(stock_list, progress_callback=progress_callback)
    if df.empty: return df
    df = process_dataframe(df)
    save_cache(market, lim, df)
    return df

def load_with_progress(market_key: str, label: str, emoji: str, lim: int):
    # 1. 디스크 캐시 확인
    df = load_cached(market_key, lim)
    if df is not None and not is_stale(market_key, lim):
        st.caption(f"✅ {label} 캐시 데이터 로드됨 ({get_cache_age_str(market_key, lim)} 전 갱신)")
        return df

    # 2. 실시간 수집 (진행바 표시)
    status_text = st.empty()
    status_text.info(f"📡 {emoji} {label} 실시간 데이터 수집 중 (최대 200종목)...")
    bar = st.progress(0.0)
    
    def update_progress(p, msg):
        bar.progress(p, text=f"{emoji} {msg}")

    try:
        df = _fetch_fresh(market_key, lim, update_progress)
        bar.empty(); status_text.empty()
        if not df.empty: return df
    except Exception:
        pass
        
    bar.empty(); status_text.empty()
    return pd.DataFrame()

# ============================================================
# UI 컴포넌트
# ============================================================

def render_search_result(df: pd.DataFrame):
    if df.empty: return
    row = df.iloc[0]
    st.markdown(f"### 🎯 [{row['ticker_display']}] {row['name']}")
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("현재가", f"{row['price']:,.2f} {row['currency']}")
    with c2: st.metric("시가총액", f"{row['market_cap_b']:,.1f} B$")
    with c3:
        pcf_val = row['pcf']
        pcf_str = f"{pcf_val:.1f}x" if pcf_val and pcf_val > 0 else "N/A"
        st.metric("P/CF", pcf_str)
    with c4: st.metric("섹터", row['sector'])
    
    st.markdown("---")
    st.markdown("#### 📅 주봉 차트 (최근 2년)")
    hist = get_history(row['ticker_yf'])
    if not hist.empty:
        fig = plot_weekly_chart(hist, row['name'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("차트 데이터를 불러올 수 없습니다.")

def render_ranking_table(df: pd.DataFrame, label: str):
    if df.empty or "pcf" not in df.columns: return
    st.markdown(f"#### 🏆 {label} 저평가 랭킹 (Top 50)")
    valid_df = df[ (df["pcf"] > 0) ].copy()
    valid_df["score"] = valid_df["pcf"]
    
    # 보너스 점수 (추세가 좋으면 P/CF가 낮아보이도록 가중치 부여)
    if "revenue_trend" in valid_df.columns:
        valid_df.loc[valid_df["revenue_trend"].str.contains("Uptrend", na=False), "score"] -= 1.0
    if "cf_trend" in valid_df.columns:
        valid_df.loc[valid_df["cf_trend"].str.contains("Uptrend", na=False), "score"] -= 1.0
        
    valid_df = valid_df.sort_values("score", ascending=True).head(50)
    
    if valid_df.empty:
        st.caption("데이터가 없습니다."); return
        
    valid_df.reset_index(drop=True, inplace=True)
    valid_df.index = valid_df.index + 1
    
    cols_map = {
        "ticker_display": "티커", "name": "종목명", "sector": "섹터",
        "pcf_display": "P/CF", "price": "현재가", 
        "revenue_trend": "매출추세", "cf_trend": "CF추세"
    }
    avail = [c for c in cols_map.keys() if c in valid_df.columns]
    view = valid_df[avail].rename(columns=cols_map)
    st.dataframe(view, use_container_width=True)
    st.caption("※ 랭킹 산정: P/CF 기준 (매출/CF 우상향 시 가산점)")

def render_tab_content(market_key: str, label: str, emoji: str):
    df = load_with_progress(market_key, label, emoji, limit)
    if df.empty:
        st.warning(f"⚠️ {label}: 데이터 로드 실패"); return

    st.info(f"💡 **P/CF(Price to Cash Flow)**: 주가가 현금흐름의 몇 배인지 나타냅니다. 낮을수록 저평가 상태입니다. (10이하: 저평가, 20이상: 고평가)")

    # 요약 지표
    stats = get_summary_stats(df)
    st.markdown(f"""
    <div class="stat-row">
        <div class="stat-card"><div class="val">{stats['total']}</div><div class="lbl">분석 종목</div></div>
        <div class="stat-card"><div class="val">{stats['median_pcf']:.1f}x</div><div class="lbl">중앙값 P/CF</div></div>
        <div class="stat-card"><div class="val">{stats['undervalued']}</div><div class="lbl">저평가(10이하)</div></div>
        <div class="stat-card"><div class="val">{stats['neg_cf_pct']:.0f}%</div><div class="lbl">현금흐름 적자</div></div>
    </div>
    """, unsafe_allow_html=True)

    # 트리맵 시각화
    fig = build_treemap(df, f"{emoji} {label} Real-time Valuation (P/CF)", size_mode=size_mode, hide_neg=hide_neg)
    st.plotly_chart(fig, use_container_width=True, theme=None)

    # 종목 선택기 (차트 보기)
    st.markdown(f"#### 📈 {label} 개별 종목 차트")
    ticker_options = df['ticker_display'].tolist()
    selected_ticker = st.selectbox("차트를 볼 종목을 선택하세요", ["선택 안 함"] + ticker_options, key=f"sel_{market_key}")
    if selected_ticker != "선택 안 함":
        sel_row = df[df['ticker_display'] == selected_ticker]
        if not sel_row.empty: render_search_result(sel_row)

    # 랭킹 테이블
    render_ranking_table(df, label)

# ============================================================
# 메인 영역 - 공유 화면 (영속 탭)
# ============================================================

# 검색어가 있으면 최우선 표시
if st.session_state.last_search:
    st.markdown("### 🔎 검색 결과 (실시간 공유)")
    with st.spinner(f"'{st.session_state.last_search}' 데이터 분석 중..."):
        search_df = fetch_single_stock(st.session_state.last_search)
        if not search_df.empty:
            search_df = process_dataframe(search_df)
            render_search_result(search_df)
        else:
            st.error(f"❌ '{st.session_state.last_search}' 종목을 찾을 수 없습니다.")
    st.markdown("---")

market_list = [
    "🇰🇷 한국 (KOSPI 200)",
    "🇺🇸 미국 (S&P 500 + Nasdaq)",
    "🇯🇵 일본 (Nikkei 225)",
    "🇪🇺 유럽 (Euro Stoxx 50)"
]

# 탭 선택 (영속성 연동)
def on_market_change():
    save_app_state({
        "current_market": st.session_state.market_radio,
        "last_search": st.session_state.last_search
    })

default_idx = market_list.index(st.session_state.current_market) if st.session_state.current_market in market_list else 0

selected_market = st.radio(
    "🌍 시장 선택 (현재 활성 탭이 다른 접속자에게도 기본으로 보입니다)",
    market_list,
    index=default_idx,
    horizontal=True,
    key="market_radio",
    on_change=on_market_change
)

# 탭별 렌더링
if "한국" in selected_market:
    render_tab_content("Korea", "KOSPI 200", "🇰🇷")
elif "미국" in selected_market:
    df_sp = load_with_progress("USA_SP500", "S&P 500", "🇺🇸", limit)
    df_nq = load_with_progress("USA_NASDAQ", "Nasdaq 100", "💻", limit)
    frames = [f for f in [df_sp, df_nq] if not f.empty]
    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker_yf"], keep="first")
        render_tab_content("USA", "S&P 500 + Nasdaq 100", "🇺🇸") # Simplified for combined
    else:
        st.warning("데이터 없음")
elif "일본" in selected_market:
    render_tab_content("Japan", "Nikkei 225", "🇯🇵")
elif "유럽" in selected_market:
    render_tab_content("Europe", "Euro Stoxx 50", "🇪🇺")

# ============================================================
# 푸터
# ============================================================
st.markdown("---")
st.markdown(f"""
<div style="text-align:center;color:#555;font-size:0.7rem;padding:8px;">
    G-Valuemap v2.1 | 실시간 화면 공유 모드 활성화 | 종목 수 200개 고정<br>
    P/CF = Market Cap ÷ TTM OCF (리츠: FFO) | 🟢 저평가 → 🔴 고평가 | ⚪ N/A
</div>
""", unsafe_allow_html=True)
