"""Phase 1 - Project Foundation: central configuration.

Doc tat ca secret/cau hinh tu `.env` qua python-dotenv.
Khong hard-code API key trong source code.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

# --- Secret (Phase 2 se dung, Phase 1 chi doc, khong goi API) ---
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")

# --- Duong dan du lieu (theo PROJECT_CONTEXT.md) ---
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = BASE_DIR / "processed"
ROADS_CSV = DATA_DIR / "roads.csv"
TRAFFIC_DATA_CSV = DATA_DIR / "traffic_data.csv"
WEATHER_DATA_CSV = DATA_DIR / "weather_data.csv"

ROAD_SUMMARY_CSV = PROCESSED_DIR / "road_summary.csv"
HOURLY_SUMMARY_CSV = PROCESSED_DIR / "hourly_summary.csv"
WEATHER_SUMMARY_CSV = PROCESSED_DIR / "weather_summary.csv"

# --- Spark (Local Mode, Phase 0 da verify) ---
SPARK_APP_NAME = "HanoiTrafficAnalytics"
SPARK_MASTER = "local[*]"

# --- Historical collection cadence + quota policy (Phase 4.3, design contract) ---
# Chay thu cong theo khung gio Ha Noi; start inclusive, end exclusive
# (vd 06:00–10:00 cho cac cycle 06:00, 06:15, ..., 09:45; khong chay 10:00).
COLLECTION_INTERVAL_MINUTES = 15
COLLECTION_TIMEZONE = "Asia/Ho_Chi_Minh"
COLLECTION_WINDOWS = (
    ("06:00", "10:00"),
    ("16:00", "20:00"),
)

# Project-configured budget (khong phai dynamic provider discovery).
# Safety limit = 80% budget; reserve cho manual tests/demo/dev.
TOMTOM_MONTHLY_BUDGET = 20_000
TOMTOM_MONTHLY_SAFETY_LIMIT = 16_000


def _window_minutes(start, end):
    """Do dai window (phut); start/end dang 'HH:MM'. Start phai < end.

    Khong support overnight windows o MVP.
    """
    from datetime import datetime

    try:
        s = datetime.strptime(start, "%H:%M")
        e = datetime.strptime(end, "%H:%M")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Window khong hop le: {(start, end)!r}.") from exc
    delta = (e - s).total_seconds() / 60
    if delta <= 0:
        raise ValueError(f"Window start phai < end: {(start, end)!r}.")
    return delta


def estimate_collection_budget(road_count, interval_minutes, windows, days=30):
    """Pure helper (KHONG network): uoc tinh request theo cadence contract.

    Moi road = 1 request/source/cycle. Window duration phai chia het cho
    interval (exact cadence, tranh off-by-one). Tra ve summary dict
    deterministic. KHONG import collector.
    """
    if isinstance(road_count, bool) or not isinstance(road_count, int):
        raise ValueError(f"road_count phai la int >= 0: {road_count!r}.")
    if road_count < 0:
        raise ValueError(f"road_count khong duoc am: {road_count!r}.")
    if isinstance(interval_minutes, bool) or not isinstance(
        interval_minutes, (int, float)
    ):
        raise ValueError(f"interval_minutes phai > 0: {interval_minutes!r}.")
    if interval_minutes <= 0:
        raise ValueError(f"interval_minutes phai > 0: {interval_minutes!r}.")
    if isinstance(days, bool) or not isinstance(days, (int, float)):
        raise ValueError(f"days phai > 0: {days!r}.")
    if days <= 0:
        raise ValueError(f"days phai > 0: {days!r}.")
    if not windows:
        raise ValueError("windows khong duoc rong.")

    cycles_per_day = 0
    for window in windows:
        start, end = window
        duration = _window_minutes(start, end)
        cycles, remainder = divmod(duration, interval_minutes)
        if remainder != 0:
            raise ValueError(
                f"Window {(start, end)!r} ({duration:g} min) khong chia het "
                f"cho interval {interval_minutes}."
            )
        cycles_per_day += int(cycles)

    active_hours = cycles_per_day * interval_minutes / 60
    per_day = road_count * cycles_per_day
    return {
        "road_count": road_count,
        "interval_minutes": interval_minutes,
        "active_hours_per_day": active_hours,
        "cycles_per_day": cycles_per_day,
        "tomtom_requests_per_day": per_day,
        "weather_requests_per_day": per_day,
        "tomtom_requests_30_days": per_day * days,
        "weather_requests_30_days": per_day * days,
    }
