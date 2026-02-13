import json
import os
import threading

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_cache", "app_state.json")
_lock = threading.Lock()

def save_app_state(state_dict: dict):
    """지속성 데이터를 파일에 저장 (접속자 간 공유)."""
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with _lock:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state_dict, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving app state: {e}")

def load_app_state() -> dict:
    """저장된 접속자 공유 상태를 로드."""
    try:
        if os.path.exists(STATE_FILE):
            with _lock:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
    except Exception as e:
        print(f"Error loading app state: {e}")
    return {}
