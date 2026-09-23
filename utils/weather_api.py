"""Phase 3.1-3.2 + 3.5 - Open-Meteo Weather API helper.

- fetch_weather(): chi lo API communication (free/open-access, khong can key).
  Khong goi API o import time. Khong fake data, khong retry/backoff phuc tap.
- build_weather_record(): chuan hoa weather record 12 fields, timestamp
  convert ve UTC ISO 8601 bang timezone-aware datetime (zoneinfo).
- Buoc 3.5: temporal join contract (reference semantics cho Spark ETL Phase 5).
  Helper find_nearest_weather_record() chi phuc vu test/contract, KHONG phai
  production Pandas join va KHONG thay Spark ETL.
"""

import requests

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

TIMEZONE = "Asia/Ho_Chi_Minh"

CURRENT_VARIABLES = (
    "temperature_2m,relative_humidity_2m,precipitation,rain,"
    "wind_speed_10m,weather_code"
)

REQUIRED_CURRENT_FIELDS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "wind_speed_10m",
    "weather_code",
)


def fetch_weather(lat, lon, timeout=15):
    """Goi Open-Meteo Forecast API cho 1 toa do (lat, lon).

    Tra ve dict toi thieu Phase 3 can (Buoc 3.1 xac minh):
    {"latitude": ..., "longitude": ..., "timezone": ...,
     "time": ..., "temperature_2m": ..., "relative_humidity_2m": ...,
     "precipitation": ..., "rain": ..., "wind_speed_10m": ...,
     "weather_code": ..., "current_units": {...}}
    """
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        raise ValueError(f"Toa do khong hop le: lat={lat!r}, lon={lon!r}.")

    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
        raise ValueError(f"Toa do ngoai pham vi: lat={lat_f}, lon={lon_f}.")

    params = {
        "latitude": lat_f,
        "longitude": lon_f,
        "current": CURRENT_VARIABLES,
        "timezone": TIMEZONE,
    }

    try:
        resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Loi ket noi Open-Meteo API: {exc}") from exc
    except ValueError as exc:
        raise RuntimeError(f"Open-Meteo API tra ve JSON khong hop le: {exc}") from exc

    if not isinstance(data, dict):
        raise RuntimeError(f"Open-Meteo API tra ve cau truc khong hop le: {data!r}")

    current = data.get("current")
    if not isinstance(current, dict):
        raise RuntimeError(f"Open-Meteo API tra ve thieu 'current': {data!r}")

    try:
        result = {
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "timezone": data.get("timezone", ""),
            "time": current["time"],
        }
        for field in REQUIRED_CURRENT_FIELDS:
            result[field] = current[field]
        result["current_units"] = (data.get("current_units") or {})
    except KeyError as exc:
        raise RuntimeError(
            f"Open-Meteo API thieu truong {exc}: {data!r}"
        ) from exc

    return result


WEATHER_RECORD_FIELDS = [
    "timestamp",
    "road_name",
    "lat",
    "lon",
    "weather_lat",
    "weather_lon",
    "temperature",
    "humidity",
    "precipitation",
    "rain",
    "wind_speed",
    "weather_code",
]


def _weather_time_to_utc_iso8601(time_str, tz_name):
    """Parse current.time theo timezone Open-Meteo tra ve, convert ve UTC.

    Khong coi local time la UTC, khong cong/tru gio bang tay.
    Timezone invalid/missing -> ValueError (khong silently assume UTC).
    Tra ve ISO 8601 co offset +00:00, seconds precision.
    """
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    if not isinstance(time_str, str) or not time_str.strip():
        raise ValueError(f"Weather time khong hop le: {time_str!r}.")
    if not isinstance(tz_name, str) or not tz_name.strip():
        raise ValueError(f"Weather timezone missing/invalid: {tz_name!r}.")

    try:
        tz = ZoneInfo(tz_name.strip())
    except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
        raise ValueError(f"Weather timezone invalid: {tz_name!r}.") from exc

    try:
        dt = datetime.fromisoformat(time_str.strip())
    except ValueError as exc:
        raise ValueError(f"Weather time khong parse duoc: {time_str!r}.") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _require_number(value, field):
    """Validate weather value la numeric that (khong fake/default)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Weather field {field!r} phai numeric: {value!r}.")
    return value


def build_weather_record(road, weather_data):
    """Tao 1 normalized weather record tu road input + fetch_weather() output.

    Args:
        road: dict {"road_name", "lat", "lon"} (toa do request tu roads.csv).
        weather_data: dict tu fetch_weather() voi latitude/longitude (grid
            thuc te), timezone, time va 6 current fields.

    Returns:
        dict schema 12 fields: timestamp (UTC ISO 8601), road_name, lat, lon
        (toa do request), weather_lat/weather_lon (grid Open-Meteo),
        temperature, humidity, precipitation, rain, wind_speed, weather_code.
    """
    if not isinstance(road, dict):
        raise ValueError(f"road phai la dict: {road!r}.")
    road_name = road.get("road_name")
    if not isinstance(road_name, str) or not road_name.strip():
        raise ValueError(f"road_name khong hop le: {road_name!r}.")

    try:
        lat_f = float(road.get("lat"))
        lon_f = float(road.get("lon"))
    except (TypeError, ValueError):
        raise ValueError(
            f"Toa do road khong hop le: lat={road.get('lat')!r}, "
            f"lon={road.get('lon')!r}."
        )
    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
        raise ValueError(f"Toa do road ngoai pham vi: lat={lat_f}, lon={lon_f}.")

    if not isinstance(weather_data, dict):
        raise ValueError(f"weather_data phai la dict tu fetch_weather: {weather_data!r}.")

    for field in ("latitude", "longitude", "timezone", "time"):
        if weather_data.get(field) is None or (
            isinstance(weather_data.get(field), str)
            and not weather_data[field].strip()
        ):
            raise ValueError(f"weather_data thieu field {field!r}: {weather_data!r}.")

    try:
        wlat = float(weather_data["latitude"])
        wlon = float(weather_data["longitude"])
    except (TypeError, ValueError):
        raise ValueError(
            f"Weather grid coordinate khong hop le: {weather_data!r}."
        )

    timestamp = _weather_time_to_utc_iso8601(
        weather_data["time"], weather_data["timezone"]
    )

    mapping = {
        "temperature": "temperature_2m",
        "humidity": "relative_humidity_2m",
        "precipitation": "precipitation",
        "rain": "rain",
        "wind_speed": "wind_speed_10m",
        "weather_code": "weather_code",
    }
    values = {}
    for out_key, in_key in mapping.items():
        if in_key not in weather_data or weather_data[in_key] is None:
            raise ValueError(
                f"weather_data thieu field {in_key!r}: {weather_data!r}."
            )
        values[out_key] = _require_number(weather_data[in_key], in_key)

    return {
        "timestamp": timestamp,
        "road_name": road_name.strip(),
        "lat": lat_f,
        "lon": lon_f,
        "weather_lat": wlat,
        "weather_lon": wlon,
        "temperature": values["temperature"],
        "humidity": values["humidity"],
        "precipitation": values["precipitation"],
        "rain": values["rain"],
        "wind_speed": values["wind_speed"],
        "weather_code": values["weather_code"],
    }


# --- Buoc 3.5: temporal join contract (reference semantics) ---
#
# Weather observations may be aligned to provider time steps,
# so traffic and weather timestamps are not expected to be exactly equal.
# Logical join key: road_name + nearest timestamp within tolerance.
# Khong join bang lat/lon equality (road coords vs grid coords khac nhau).
# Helper duoi day chi phuc vu test/contract; Phase 5 phai implement
# equivalent semantics bang Spark, KHONG dung Python/Pandas join thay the.

WEATHER_JOIN_TOLERANCE_MINUTES = 60


def _parse_aware_timestamp(value, label):
    """Parse ISO 8601 timestamp, yeu cau timezone-aware."""
    from datetime import datetime

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} khong hop le: {value!r}.")
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"{label} khong parse duoc: {value!r}.") from exc
    if dt.tzinfo is None:
        raise ValueError(f"{label} phai timezone-aware: {value!r}.")
    return dt


def find_nearest_weather_record(traffic_record, weather_records,
                                tolerance_minutes=WEATHER_JOIN_TOLERANCE_MINUTES):
    """Tim weather record gan nhat cung road_name trong tolerance.

    - Chi xet weather records cung road_name voi traffic observation.
    - Chon record co abs(traffic_ts - weather_ts) nho nhat.
    - Chi match neu time_difference <= tolerance (60 phut).
    - Tie (2 phia cach deu): prefer past (weather_ts <= traffic_ts).
    - Duplicate cung road + timestamp: chon deterministically (on dinh
      theo thu tu timestamp, khong phu thuoc thu tu input).
    - Khong match -> tra ve None (weather unavailable, khong fake data).

    Day la reference semantics cho Spark ETL Phase 5, khong phai
    production join implementation.
    """
    if not isinstance(traffic_record, dict):
        raise ValueError(f"traffic_record phai la dict: {traffic_record!r}.")
    road = traffic_record.get("road_name")
    if not isinstance(road, str) or not road.strip():
        raise ValueError(f"traffic road_name khong hop le: {road!r}.")
    t_ts = _parse_aware_timestamp(traffic_record.get("timestamp"),
                                  "traffic timestamp")

    tolerance_s = tolerance_minutes * 60

    def _canon(w):
        # Thu tu sap xep hoan toan xac dinh theo NOI DUNG record
        # (khong phu thuoc thu tu input), ke ca khi duplicate road+timestamp.
        items = sorted((str(k), str(v)) for k, v in w.items())
        return (str(w.get("timestamp")), str(w.get("road_name")), repr(items))

    ordered = sorted(
        [w for w in (weather_records or []) if isinstance(w, dict)],
        key=_canon,
    )
    best = None
    best_key = None
    for pos, w in enumerate(ordered):
        if w.get("road_name") != road:
            continue
        w_ts = _parse_aware_timestamp(w.get("timestamp"), "weather timestamp")
        diff = abs((t_ts - w_ts).total_seconds())
        if diff > tolerance_s:
            continue
        past_flag = 0 if w_ts <= t_ts else 1
        key = (diff, past_flag, pos)
        if best_key is None or key < best_key:
            best_key = key
            best = w
    return best


if __name__ == "__main__":
    # Demo toi thieu Buoc 3.1: test dung 1 toa do dau tien trong roads.csv.
    # Khong loop 5 tuyen, khong ghi file, khong merge traffic/weather.
    import csv

    from config import ROADS_CSV

    with open(ROADS_CSV, newline="", encoding="utf-8") as f:
        row = next(csv.DictReader(f))

    w = fetch_weather(row["lat"], row["lon"])
    units = w.get("current_units") or {}
    print("Weather API test")
    print("----------------")
    print(f"Road: {row['road_name']}")
    print(f"Requested coordinate: {row['lat']},{row['lon']}")
    print(f"Returned coordinate: {w['latitude']},{w['longitude']}")
    print(f"Timezone: {w['timezone']}")
    print(f"Weather time: {w['time']}")
    print(f"Temperature: {w['temperature_2m']} {units.get('temperature_2m', '')}")
    print(f"Humidity: {w['relative_humidity_2m']} {units.get('relative_humidity_2m', '')}")
    print(f"Precipitation: {w['precipitation']} {units.get('precipitation', '')}")
    print(f"Rain: {w['rain']} {units.get('rain', '')}")
    print(f"Wind speed: {w['wind_speed_10m']} {units.get('wind_speed_10m', '')}")
    print(f"Weather code: {w['weather_code']} {units.get('weather_code', '')}")
