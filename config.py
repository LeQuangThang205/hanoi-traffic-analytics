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
