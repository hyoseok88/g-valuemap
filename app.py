"""
G-Valuemap: Global Market Valuation TreeMap (v2)
================================================
5대 글로벌 지수의 P/CF 밸류에이션 트리맵 대시보드.

실행: streamlit run app.py
"""

import streamlit as st
import pandas as pd

from data_fetcher import (
    get_kospi200, get_sp500, get_nasdaq100, get_nikkei225, get_eurostoxx50,
    fetch_stock_data, fetch_single_stock
)
from valuation import process_dataframe
from visualization import build_treemap, get_summary_stats


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

    .stTabs [data-baseweb="tab-list"] { gap: 2px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(255,255,255,0.04);
        border-radius: 10px 10px 0 0;
        color: #888; padding: 10px 22px; font-weight: 600;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: rgba(33,102,172,0.15);
        color: #fff;
        border-bottom: 2px solid #2166ac;
    }

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
    <p class="sub">Global Market Valuation TreeMap — P/CF 기반 밸류에이션 대시보드</p>
    <div class="method">
        📐 <b>계산 방식:</b> P/CF = 시가총액 ÷ TTM 현금흐름 &nbsp;|&nbsp;
        부동산·리츠: FFO 우선 &nbsp;|&nbsp;
        기타: 영업활동현금흐름(OCF) &nbsp;|&nbsp;
        🟢 저평가 → 🔵 중립 → 🔴 고평가
    </div>
</div>
""", unsafe_allow_html=True)


with st.expander("📖 처음 오셨나요? 사용 가이드 보기", expanded=True):
    st.markdown("""
    #### 👋 환영합니다! 이렇게 활용하세요:
    
    1. **시장 선택**: 상단 탭에서 🇰🇷한국, 🇺🇸미국, 🇯🇵일본, 🇪🇺유럽을 선택하세요.
    2. **색상 의미**: 
        - 🟢 **초록색**: 돈 잘 버는데 주가가 싼 기업 (**저평가**)
        - 🔴 **빨간색**: 이익 대비 주가가 비싼 기업 (**고평가**)
        - ⬜ **회색**: 적자 기업 (현금흐름 마이너스)
    3. **크기 조절**: 왼쪽 사이드바 **'타일 크기 기준'**에서:
        - **'저평가순'**을 선택하면 **알짜배기 기업**이 큼지막하게 보입니다!
    4. **검색**: 특정 종목이 궁금하면 왼쪽 사이드바 **'🔍 종목 검색'**을 이용하세요.
    """)


# ============================================================
# 사이드바
# ============================================================
with st.sidebar:
    st.markdown("### ⚙️ 설정")
    
    # 검색 기능
    search_query = st.text_input("🔍 종목 검색", placeholder="티커/코드 (예: 005930, AAPL)", help="한국(6자리), 일본(4자리), 미국(티커)")
    if search_query:
        st.write("") # Spacer

    st.markdown("---")
    
    limit = st.slider("지수당 종목 수", 10, 300, 30, 10,
                       help="각 지수에서 시총 상위 N개 종목. 높을수록 로딩 느림.")

    size_mode = st.radio(
        "📐 타일 크기 기준",
        ["시가총액 (Market Cap)", "저평가순 (1/P×CF)"],
        index=1,
        help="시총: 시총 큰 기업이 크게. 저평가순: P/CF 낮은(저평가) 기업이 크게 보임."
    )

    hide_neg = st.checkbox("음수 CF 종목 숨기기", value=True,
                           help="현금흐름이 마이너스인 종목을 트리맵에서 제외")

    st.markdown("---")
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

    st.markdown("---")
    st.markdown("""
    <div style="color:#666;font-size:0.72rem;line-height:1.5;">
        📡 데이터: yfinance + FinanceDataReader<br>
        🔄 갱신: 24시간 캐시 (하루 1회)<br>
        📐 P/CF = 시가총액 ÷ TTM CF<br>
        📈 추세: 5년 선형회귀 (±5%)
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔄 데이터 새로고침", use_container_width=True):
        # 디스크 캐시 삭제
        import shutil, os
        cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_cache")
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        st.cache_data.clear()
        st.rerun()


from disk_cache import load_cached, save_cache, is_stale, get_cache_age_str


# ============================================================
# 데이터 로드 (디스크 캐시 + 실시간 갱신)
# 접속 시 이전 데이터 즉시 표시 → 만료시 갱신 → 완료 후 rerun
# ============================================================
FETCHERS = {
    "Korea": get_kospi200,
    "USA_SP500": get_sp500,
    "USA_NASDAQ": get_nasdaq100,
    "Japan": get_nikkei225,
    "Europe": get_eurostoxx50,
}


def _fetch_fresh(market: str, lim: int, progress_callback=None) -> pd.DataFrame:
    """신규 데이터 수집 + 가공 + 디스크 저장."""
    get_fn = FETCHERS.get(market)
    if not get_fn:
        return pd.DataFrame()
    stock_list = get_fn(lim)
    if not stock_list:
        return pd.DataFrame()
    df = fetch_stock_data(stock_list, progress_callback=progress_callback)
    if df.empty:
        return df
    df = process_dataframe(df)
    save_cache(market, lim, df)
    return df


def load_with_progress(market: str, label: str, emoji: str, lim: int):
    """
    디스크 캐시 기반 로딩:
    1. 캐시 있으면 즉시 반환 (이전 데이터 표시)
    2. 캐시 만료(24h) → 갱신 진행 → 완료 후 rerun
    3. 캐시 없으면 신규 수집 (프로그레스 표시)
    """
    cached_df, cached_ts = load_cached(market, lim)

    # ---- 캐시 있고 아직 유효하면 즉시 반환 ----
    if cached_df is not None and not is_stale(market, lim):
        age_str = get_cache_age_str(cached_ts)
        st.caption(f"📦 캐시 데이터 ({age_str}) — 다음 갱신까지 유효")
        return cached_df

    # ---- 캐시 있지만 만료 → 이전 데이터 먼저 표시, 갱신 알림 ----
    if cached_df is not None and is_stale(market, lim):
        # session_state로 갱신 중복 방지
        refresh_key = f"refreshing_{market}_{lim}"
        done_key = f"refresh_done_{market}_{lim}"

        if st.session_state.get(done_key):
            # 갱신 완료됨 → 새 데이터 반환
            st.session_state.pop(done_key, None)
            new_df, new_ts = load_cached(market, lim)
            if new_df is not None:
                age_str = get_cache_age_str(new_ts)
                st.caption(f"✅ 데이터 갱신 완료 ({age_str})")
                return new_df
            return cached_df

        if not st.session_state.get(refresh_key):
            # 이전 데이터 보여주면서 갱신 시작
            age_str = get_cache_age_str(cached_ts)
            st.caption(f"📦 이전 데이터 표시 중 ({age_str}) — 백그라운드 갱신 대기")

            # 갱신 버튼
            if st.button(f"🔄 {label} 최신 데이터로 갱신", key=f"refresh_btn_{market}"):
                st.session_state[refresh_key] = True
                st.rerun()

            return cached_df
        else:
            # 갱신 진행 중
            progress = st.progress(0, text=f"{emoji} {label} 데이터 갱신 중...")

            def update_progress(pct, msg):
                progress.progress(min(int(pct * 100), 100), text=f"{emoji} {msg}")

            try:
                df = _fetch_fresh(market, lim, progress_callback=update_progress)
                progress.progress(100, text=f"✅ {label} 갱신 완료!")
                st.session_state.pop(refresh_key, None)
                st.session_state[done_key] = True
                import time as _t; _t.sleep(0.5)
                st.rerun()
            except Exception as e:
                progress.empty()
                st.error(f"❌ 갱신 실패: {e}")
                st.session_state.pop(refresh_key, None)
                return cached_df

    # ---- 캐시 없음 → 첫 수집 (프로그레스 표시) ----
    progress = st.progress(0, text=f"{emoji} {label} 데이터 최초 수집 중...")

    def update_progress(pct, msg):
        progress.progress(min(int(pct * 100), 100), text=f"{emoji} {msg}")

    try:
        df = _fetch_fresh(market, lim, progress_callback=update_progress)
        progress.progress(100, text=f"✅ {label} 수집 완료!")
        import time as _t; _t.sleep(0.3)
        progress.empty()
        return df
    except Exception as e:
        progress.empty()
        st.error(f"❌ {label} 로드 실패: {e}")
        return pd.DataFrame()


def render_strong_picks(df: pd.DataFrame):
    """P/CF ≤10 & 매출 우상향 & CF 우상향 종목을 강력추천으로 표시."""
    if df.empty or "pcf" not in df.columns:
        return

    picks = df[
        (df["pcf"].notna()) &
        (df["pcf"] <= 10) &
        (df["revenue_trend"].str.contains("Uptrend", na=False)) &
        (df["cf_trend"].str.contains("Uptrend", na=False))
    ].copy()

    if picks.empty:
        return

    picks = picks.sort_values("pcf", ascending=True)

    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, rgba(26,150,65,0.15), rgba(102,189,99,0.08));
        border: 1px solid rgba(26,150,65,0.4);
        border-radius: 12px; padding: 16px 20px; margin: 14px 0;
    ">
        <h4 style="margin:0 0 10px 0; color:#66bd63;">
            ⭐ 강력추천 종목 ({len(picks)}개)
            <span style="font-size:0.7rem; color:#888; font-weight:400;">
                — P/CF ≤ 10x & 매출↑ & 현금흐름↑
            </span>
        </h4>
    """, unsafe_allow_html=True)

    for _, r in picks.iterrows():
        pcf_val = r.get("pcf_display", "N/A")
        mcap = r.get("market_cap_b", "N/A")
        rev_t = r.get("revenue_trend", "N/A")
        cf_t = r.get("cf_trend", "N/A")
        st.markdown(f"""
        <div style="
            display: inline-block; margin: 4px 6px; padding: 8px 14px;
            background: rgba(26,150,65,0.12); border: 1px solid rgba(26,150,65,0.3);
            border-radius: 8px; min-width: 180px;
        ">
            <div style="font-weight:700; color:#a6d96a; font-size:1rem;">
                {r.get('ticker_display','')} <span style="color:#ccc;font-weight:400;font-size:0.8rem;">{r.get('name','')}</span>
            </div>
            <div style="color:#888; font-size:0.78rem; margin-top:3px;">
                P/CF: <b style="color:#66bd63;">{pcf_val}</b> &nbsp;|&nbsp;
                시총: {mcap} &nbsp;|&nbsp;
                매출: 📈{rev_t} &nbsp;|&nbsp;
                CF: 📈{cf_t}
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_search_result(df: pd.DataFrame):
    """검색 결과 단일 종목 표시."""
    if df.empty:
        st.warning("❌ 검색 결과가 없습니다. (티커 확인: 005930, AAPL 등)")
        return

    r = df.iloc[0]
    pcf = r.get("pcf", None)
    
    # 등급 판정
    grade = "⚪분석불가"
    color = "#888"
    if pd.notna(pcf) and pcf > 0:
        if pcf <= 10: 
            grade = "🟢저평가 (Strong Buy)"
            color = "#1a9641"
        elif pcf <= 15: 
            grade = "🔵중립 (Hold)"
            color = "#2166ac"
        elif pcf <= 20: 
            grade = "🟠약간고평가"
            color = "#e6a03c"
        else: 
            grade = "🔴고평가"
            color = "#a50026"

    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
        border: 1px solid {color}88;
        border-radius: 12px; padding: 20px; margin-bottom: 20px;
    ">
        <h3 style="margin:0; color:{color}; display:flex; align-items:center; gap:10px;">
            🔍 {r.get('ticker_display')} {r.get('name')}
            <span style="font-size:1rem; background:{color}33; padding:4px 10px; border-radius:8px;">{grade}</span>
        </h3>
        <div style="display:flex; gap:20px; margin-top:15px; flex-wrap:wrap;">
            <div style="background:#ffffff08; padding:10px 15px; border-radius:8px;">
                <div style="font-size:0.8rem; color:#888;">주가 (Price)</div>
                <div style="font-size:1.2rem; font-weight:bold;">{r.get('price', 0):,.0f} {r.get('currency','')}</div>
            </div>
            <div style="background:#ffffff08; padding:10px 15px; border-radius:8px;">
                <div style="font-size:0.8rem; color:#888;">P/CF 비율</div>
                <div style="font-size:1.2rem; font-weight:bold; color:{color};">{r.get('pcf_display','N/A')}</div>
            </div>
            <div style="background:#ffffff08; padding:10px 15px; border-radius:8px;">
                <div style="font-size:0.8rem; color:#888;">매출 성장성</div>
                <div style="font-size:1.1rem;">📈 {r.get('revenue_trend','N/A')}</div>
            </div>
            <div style="background:#ffffff08; padding:10px 15px; border-radius:8px;">
                <div style="font-size:0.8rem; color:#888;">현금흐름 추세</div>
                <div style="font-size:1.1rem;">📈 {r.get('cf_trend','N/A')}</div>
            </div>
        </div>
        <div style="margin-top:10px; font-size:0.8rem; color:#666;">
            *성장성은 최근 5년 또는 YoY 기준
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_portfolio_proposal(df: pd.DataFrame, label: str):
    """추천 포트폴리오 제안 섹션."""
    if st.button(f"💼 {label} 추천 포트폴리오 생성", key=f"port_btn_{label}"):
        picks = df[
            (df["pcf"].notna()) & (df["pcf"] > 0) & (df["pcf"] <= 12) &  # 조금 더 넓은 범위
            (df["revenue_trend"].str.contains("Uptrend", na=False)) &
            (df["cf_trend"].str.contains("Uptrend", na=False))
        ].sort_values("pcf").head(5)

        if picks.empty:
            st.warning("조건에 맞는 더 엄격한 우량주가 없습니다. (P/CF≤12, 매출/CF 우상향)")
            return

        st.markdown(f"""
        <div style="background:#1e1e2f; border:1px solid #444; border-radius:10px; padding:20px; margin-top:10px;">
            <h3 style="margin-top:0; color:#ffd700;">💼 {label} AI 추천 포트폴리오</h3>
            <p style="color:#aaa; font-size:0.9rem;">
                저평가(P/CF 낮은 순) + 성장성(매출/현금흐름 우상향) 우량주 TOP 5<br>
                투자 권유 아님. 참고용으로만 활용하세요.
            </p>
        </div>
        """, unsafe_allow_html=True)

        cols = st.columns(len(picks))
        for i, (idx, row) in enumerate(picks.iterrows()):
            with cols[i]:
                st.markdown(f"""
                <div style="background:rgba(255,215,0,0.1); border:1px solid rgba(255,215,0,0.3); border-radius:8px; padding:12px; text-align:center;">
                    <div style="font-size:1.1rem; font-weight:bold; color:#fff;">{row['ticker_display']}</div>
                    <div style="font-size:0.8rem; color:#ddd; margin-bottom:5px;">{row['name']}</div>
                    <div style="font-size:0.9rem; color:#ffd700;">P/CF: {row['pcf_display']}</div>
                </div>
                """, unsafe_allow_html=True)


def render_tab(market_key: str, label: str, emoji: str):
    """단일 시장 탭 렌더링."""
    df = load_with_progress(market_key, label, emoji, limit)

    if df.empty:
        st.warning(f"⚠️ {label}: 종목 데이터 없음")
        return

    # 요약 지표
    stats = get_summary_stats(df)
    st.markdown(f"""
    <div class="stat-row">
        <div class="stat-card"><div class="val">{stats['total']}</div><div class="lbl">총 종목</div></div>
        <div class="stat-card"><div class="val">{stats['valid']}</div><div class="lbl">유효 P/CF</div></div>
        <div class="stat-card"><div class="val">{stats['median_pcf']}</div><div class="lbl">중앙값 P/CF</div></div>
        <div class="stat-card"><div class="val">{stats['mean_pcf']}</div><div class="lbl">평균 P/CF</div></div>
        <div class="stat-card"><div class="val" style="color:#ff6b6b;">{stats['negative_cf']}</div><div class="lbl">음수 CF</div></div>
    </div>
    """, unsafe_allow_html=True)

    # 강력추천 종목
    render_strong_picks(df)
    
    # 추천 포트폴리오 버튼
    _render_portfolio_proposal(df, label)

    # 트리맵
    use_underval = "저평가" in size_mode
    fig = build_treemap(df, title=f"{emoji} {label} — P/CF Valuation Map", hide_negative_cf=hide_neg, size_by_undervalue=use_underval)
    st.plotly_chart(fig, use_container_width=True)

    # 테이블
    with st.expander(f"📊 {label} 상세 데이터", expanded=False):
        cols_map = {
            "ticker_display": "티커", "name": "종목명", "sector": "섹터",
            "price": "현재가", "market_cap_b": "시총(B$)",
            "pcf_display": "P/CF", "cf_method": "CF방식",
            "revenue_trend": "매출추세", "cf_trend": "CF추세",
        }
        avail = [c for c in cols_map if c in df.columns]
        dfd = df[avail].copy()
        dfd.columns = [cols_map[c] for c in avail]
        if "P/CF" in dfd.columns:
            dfd = dfd.sort_values("P/CF", key=lambda x: x.str.replace("x","").str.replace("N/A","999").astype(float))
        st.dataframe(dfd, use_container_width=True, hide_index=True)


def render_usa_tab():
    """USA 탭: S&P 500 + Nasdaq 100 통합."""
    df_sp = load_with_progress("USA_SP500", "S&P 500", "🇺🇸", limit)
    df_nq = load_with_progress("USA_NASDAQ", "Nasdaq 100", "💻", limit)

    frames = [f for f in [df_sp, df_nq] if not f.empty]
    if not frames:
        st.warning("⚠️ 미국 데이터 없음"); return

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker_yf"], keep="first")

    stats = get_summary_stats(df)
    st.markdown(f"""
    <div class="stat-row">
        <div class="stat-card"><div class="val">{stats['total']}</div><div class="lbl">총 종목</div></div>
        <div class="stat-card"><div class="val">{stats['valid']}</div><div class="lbl">유효 P/CF</div></div>
        <div class="stat-card"><div class="val">{stats['median_pcf']}</div><div class="lbl">중앙값 P/CF</div></div>
        <div class="stat-card"><div class="val">{stats['mean_pcf']}</div><div class="lbl">평균 P/CF</div></div>
        <div class="stat-card"><div class="val" style="color:#ff6b6b;">{stats['negative_cf']}</div><div class="lbl">음수 CF</div></div>
    </div>
    """, unsafe_allow_html=True)

    # 강력추천 종목
    render_strong_picks(df)

    # 추천 포트폴리오 버튼
    _render_portfolio_proposal(df, "USA")

    use_underval = "저평가" in size_mode
    fig = build_treemap(df, title="🇺🇸 USA (S&P 500 + Nasdaq 100) — P/CF Valuation Map", hide_negative_cf=hide_neg, size_by_undervalue=use_underval)
    st.plotly_chart(fig, use_container_width=True, config={'scrollZoom': True, 'displayModeBar': True})

    with st.expander("📊 미국 상세 데이터", expanded=False):
        cols_map = {
            "ticker_display": "티커", "name": "종목명", "sector": "섹터",
            "price": "현재가", "market_cap_b": "시총(B$)",
            "pcf_display": "P/CF", "cf_method": "CF방식",
            "revenue_trend": "매출추세", "cf_trend": "CF추세",
        }
        avail = [c for c in cols_map if c in df.columns]
        dfd = df[avail].copy()
        dfd.columns = [cols_map[c] for c in avail]
        if "P/CF" in dfd.columns:
            dfd = dfd.sort_values("P/CF", key=lambda x: x.str.replace("x","").str.replace("N/A","999").astype(float))
        st.dataframe(dfd, use_container_width=True, hide_index=True)


# ============================================================
# 탭 구성
# ============================================================

# 검색 결과가 있으면 맨 위에 표시
if search_query:
    st.markdown("### 🔎 검색 결과")
    with st.spinner(f"'{search_query}' 데이터 수집 및 분석 중..."):
        search_df = fetch_single_stock(search_query)
        if not search_df.empty:
            search_df = process_dataframe(search_df)
            render_search_result(search_df)
        else:
            st.error(f"❌ '{search_query}' 종목을 찾을 수 없습니다. (티커를 확인해주세요)")
    st.markdown("---")


tab_kr, tab_us, tab_jp, tab_eu = st.tabs([
    "🇰🇷 한국 (KOSPI 200)",
    "🇺🇸 미국 (S&P 500 + Nasdaq)",
    "🇯🇵 일본 (Nikkei 225)",
    "🇪🇺 유럽 (Euro Stoxx 50)",
])

with tab_kr:
    render_tab("Korea", "KOSPI 200", "🇰🇷")

with tab_us:
    render_usa_tab()

with tab_jp:
    render_tab("Japan", "Nikkei 225", "🇯🇵")

with tab_eu:
    render_tab("Europe", "Euro Stoxx 50", "🇪🇺")

# ============================================================
# 푸터
# ============================================================
st.markdown("---")
st.markdown("""
<div style="text-align:center;color:#555;font-size:0.7rem;padding:8px;">
    G-Valuemap v2.0 | Yahoo Finance + FinanceDataReader | 투자 참고용 (투자 권유 아님)<br>
    P/CF = Market Cap ÷ TTM Cash Flow | 🟢 저평가 → 🔵 중립 → 🔴 고평가 | ⚪ 음수 CF = N/A
</div>
""", unsafe_allow_html=True)
