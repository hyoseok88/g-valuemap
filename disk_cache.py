"""
disk_cache.py — 디스크 기반 데이터 캐시
접속 즉시 이전 데이터를 보여주고, 백그라운드에서 갱신.
"""

import os
import time
import pickle
import pandas as pd

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_cache")
CACHE_TTL = 86400  # 24시간 (초)


def _ensure_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(market: str, limit: int) -> str:
    return os.path.join(CACHE_DIR, f"{market}_{limit}.pkl")


def _meta_path(market: str, limit: int) -> str:
    return os.path.join(CACHE_DIR, f"{market}_{limit}_meta.pkl")


def load_cached(market: str, limit: int) -> tuple[pd.DataFrame | None, float | None]:
    """
    디스크에서 캐시된 데이터 로드.
    Returns: (DataFrame or None, timestamp or None)
    """
    _ensure_dir()
    cp = _cache_path(market, limit)
    mp = _meta_path(market, limit)

    if not os.path.exists(cp):
        return None, None

    try:
        df = pd.read_pickle(cp)
        ts = None
        if os.path.exists(mp):
            with open(mp, "rb") as f:
                meta = pickle.load(f)
                ts = meta.get("timestamp")
        return df, ts
    except Exception:
        return None, None


def save_cache(market: str, limit: int, df: pd.DataFrame):
    """데이터를 디스크에 저장."""
    _ensure_dir()
    cp = _cache_path(market, limit)
    mp = _meta_path(market, limit)

    df.to_pickle(cp)
    with open(mp, "wb") as f:
        pickle.dump({"timestamp": time.time()}, f)


def is_stale(market: str, limit: int) -> bool:
    """캐시가 만료됐는지 확인 (24시간 기준)."""
    _, ts = load_cached(market, limit)
    if ts is None:
        return True
    return (time.time() - ts) > CACHE_TTL


def get_cache_age_str(ts: float | None) -> str:
    """타임스탬프를 '~시간 전' 문자열로."""
    if ts is None:
        return "캐시 없음"
    age = time.time() - ts
    if age < 60:
        return "방금 갱신"
    if age < 3600:
        return f"{age/60:.0f}분 전"
    if age < 86400:
        return f"{age/3600:.0f}시간 전"
    return f"{age/86400:.0f}일 전"
