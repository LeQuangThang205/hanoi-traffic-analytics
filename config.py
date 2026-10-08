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

# --- Local Data Lake (Phase 10.2B, Windows-safe PyArrow fallback) ---
# Raw zone: ROADS_CSV / TRAFFIC_DATA_CSV / WEATHER_DATA_CSV (CSV, giu nguyen).
# Curated zone: data/processed/*.csv (dashboard compat) + Parquet analytical
# storage duoi day (Spark ETL + PyArrow driver-side serializer).
# ML zone: reserved cho phase sau (chua tao).
DATA_LAKE_DIR = DATA_DIR / "lake"
CURATED_DIR = DATA_LAKE_DIR / "curated"
COMBINED_PARQUET_PATH = CURATED_DIR / "combined" / "combined_data.parquet"

# --- Spark (Local Mode, Phase 0 da verify) ---
SPARK_APP_NAME = "HanoiTrafficAnalytics"
SPARK_MASTER = "local[*]"

# --- Historical collection cadence + quota policy (Phase 4.3, Phase 10.4 campaign) ---
# Campaign 10.4: 06:00-22:00 lien tuc moi 5 phut (Asia/Ho_Chi_Minh).
# Start inclusive, end exclusive (cycles 06:00, 06:05, ..., 21:55; khong chay 22:00).
# Ly do chon (thay vi multi-window 10.3 cu 06-10 + 16-20): single window don gian
# nhat (1 tuple), tu dong phu trua + toi, khong gap logic, de giai thich.
# 6 roads -> 192 cycles/ngay -> ~1,152 combined observations/ngay (100% success).
# Internal API demand (KHONG ket luan quota/budget vi chua verify quota chinh
# thuc hien tai): TomTom ~1,152 + Open-Meteo ~1,152 = ~2,304 requests/ngay;
# TomTom ~34,560/30 ngay. Laptop runtime ~16h/ngay nhung co the stop/restart
# bat ky luc nao; missed slots bi skip, khong backfill.
COLLECTION_INTERVAL_MINUTES = 5
COLLECTION_TIMEZONE = "Asia/Ho_Chi_Minh"
COLLECTION_WINDOWS = (
    ("06:00", "22:00"),
)

# --- Real data collection campaign targets (Phase 10.4, planning constants) ---
# Practical target cho student ML prototype (khong phai universal ML minimum).
# Uu tien diversity (nhieu ngay, weekday+weekend, sang/trua/chieu/toi,
# mua/khong mua, nhieu congestion levels) hon raw row count.
# Processing: per-cycle (moi collection cycle -> 1 Spark process-only ngay
# sau khi raw persistence hoan tat; near-realtime batch 5 phut, KHONG streaming).
CAMPAIGN_TARGET_MIN_OBS = 3_000
CAMPAIGN_TARGET_MAX_OBS = 5_000
CAMPAIGN_PROCESSING_CADENCE = "per-cycle"

# Project-configured budget (khong phai dynamic provider discovery).
# Safety limit = 80% budget; reserve cho manual tests/demo/dev.
TOMTOM_MONTHLY_BUDGET = 20_000
TOMTOM_MONTHLY_SAFETY_LIMIT = 16_000

# --- Auto-process during collection campaign (near-realtime, per-cycle) ---
# Campaign thu API moi 5 phut VA Spark process ngay sau moi collection cycle:
# sau moi AUTO_PROCESS_EVERY_CYCLES cycle attempts da hoan tat, campaign tu
# chay data_update.run_process_only() mot lan (tuan tu, khong song song).
# Voi AUTO_PROCESS_EVERY_CYCLES = 1: moi cycle trong valid window deu
# collection -> raw persistence hoan tat -> Spark ETL + analytics ->
# processed snapshot moi (near-realtime batch 5 phut, KHONG streaming).
# Chi 1 Spark job tai 1 thoi diem; khong multiprocessing/queue/backfill.
# So 1 chi dinh nghia 1 lan o day, KHONG hard-code rai rac trong scheduler.
# Tat bang CLI --no-auto-process.
AUTO_PROCESS_ENABLED = True
AUTO_PROCESS_EVERY_CYCLES = 1

# --- Hanoi road catalog (Phase 8.2, UX-only static data) ---
# Danh muc tuyen Ha Noi co toa do dai dien de UI "Them tuyen" dung dropdown
# thay vi nhap tay lat/lon. Moi entry chi co road_name/lat/lon (schema
# roads.csv khong doi). Quy tac:
# - 5 production roads GIU CHINH XAC toa do data/roads.csv hien tai.
# - Tuyen moi: diem dai dien tren truc duong, tra cuu tu OpenStreetMap
#   (Nominatim way lookup, lam tron 4 decimals nhu production precision).
# - Chua Boc / Nguyen Van Cu KHONG dua vao vi khong lay duoc road point
#   dang tin (accuracy > quantity).
# - KHONG geocoding runtime, KHONG API call, KHONG database.
HANOI_ROAD_CATALOG = (
    {"road_name": "Nguyen Trai", "lat": 20.9983, "lon": 105.7929},
    {"road_name": "Truong Chinh", "lat": 20.9975, "lon": 105.8348},
    {"road_name": "Giai Phong", "lat": 20.9919, "lon": 105.8355},
    {"road_name": "Cau Giay", "lat": 21.0285, "lon": 105.8003},
    {"road_name": "Xuan Thuy", "lat": 21.0327, "lon": 105.7829},
    {"road_name": "Dai Co Viet", "lat": 21.0082, "lon": 105.8469},
    {"road_name": "Kim Ma", "lat": 21.0324, "lon": 105.8279},
    {"road_name": "Lang Ha", "lat": 21.0156, "lon": 105.8144},
    {"road_name": "Le Van Luong", "lat": 21.0092, "lon": 105.8099},
    {"road_name": "Minh Khai", "lat": 21.0057, "lon": 105.8689},
    {"road_name": "Nguyen Chi Thanh", "lat": 21.0246, "lon": 105.8112},
    {"road_name": "Pham Van Dong", "lat": 21.0671, "lon": 105.7853},
    {"road_name": "Tay Son", "lat": 21.0105, "lon": 105.8251},
)


def find_catalog_entry(road_name):
    """Tim entry trong HANOI_ROAD_CATALOG theo ten (pure, khong IO).

    So sanh trim + case-insensitive (tuong thich road_manager).
    Tra ve dict entry COPY (chong mutate catalog) hoac None.
    """
    if not isinstance(road_name, str) or not road_name.strip():
        return None
    target = road_name.strip().lower()
    for entry in HANOI_ROAD_CATALOG:
        if entry["road_name"].strip().lower() == target:
            return dict(entry)
    return None


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
