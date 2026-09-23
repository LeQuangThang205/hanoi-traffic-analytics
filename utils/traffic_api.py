"""Phase 2.1 - TomTom Traffic API helper (muc toi thieu cho Phase 2).

Chi chuan bi cau hinh + ham goi API don gian.
Khong goi API that o import time. Khong hard-code API key.
"""

import os

import requests

try:
    from config import TOMTOM_API_KEY as _CONFIG_KEY
except ImportError:
    _CONFIG_KEY = ""

TOMTOM_FLOW_URL = (
    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
)


def resolve_api_key(api_key=None):
    """Lay TomTom API key uu tien: tham so truyen vao > config/.env."""
    key = (api_key if api_key is not None else _CONFIG_KEY) or os.getenv(
        "TOMTOM_API_KEY", ""
    )
    key = str(key).strip().strip("'\"")
    if not key:
        raise RuntimeError(
            "Thieu cau hinh TOMTOM_API_KEY. "
            "Hay tao file `.env` tu `.env.example` va dien TOMTOM_API_KEY that, "
            "vi du: TOMTOM_API_KEY=abc123. Khong goi API khi key rong."
        )
    return key


def fetch_traffic_flow(lat, lon, api_key=None, timeout=15):
    """Goi TomTom flowSegmentData cho 1 toa do (lat, lon).

    Tra ve dict toi thieu Phase 2 can (Buoc 2.2 xac minh du 5 field):
    {"currentSpeed": ..., "freeFlowSpeed": ..., "currentTravelTime": ...,
     "freeFlowTravelTime": ..., "confidence": ...}
    """
    key = resolve_api_key(api_key)

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        raise ValueError(f"Toa do khong hop le: lat={lat!r}, lon={lon!r}.")

    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
        raise ValueError(f"Toa do ngoai pham vi: lat={lat_f}, lon={lon_f}.")

    params = {"key": key, "point": f"{lat_f},{lon_f}"}

    try:
        resp = requests.get(TOMTOM_FLOW_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Loi ket noi TomTom API: {exc}") from exc

    segment = (data or {}).get("flowSegmentData")
    if not isinstance(segment, dict):
        raise RuntimeError(f"TomTom API tra ve thieu 'flowSegmentData': {data!r}")

    try:
        return {
            "currentSpeed": segment["currentSpeed"],
            "freeFlowSpeed": segment["freeFlowSpeed"],
            "currentTravelTime": segment["currentTravelTime"],
            "freeFlowTravelTime": segment["freeFlowTravelTime"],
            "confidence": segment["confidence"],
        }
    except KeyError as exc:
        raise RuntimeError(
            f"TomTom API thieu truong {exc} trong 'flowSegmentData': {segment!r}"
        ) from exc
