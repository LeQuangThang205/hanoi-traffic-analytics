"""Phase 2.3-2.6 - Data Collector: chuan hoa + luu traffic record.

Pham vi Buoc 2.3-2.6 (toi thieu):
- Giu `fetch_traffic_flow()` trong `utils/traffic_api.py` chi lo giao tiep TomTom.
- Logic chuan hoa record nam o day: ghep road input + timestamp + TomTom response.
- Buoc 2.4: doc `data/roads.csv`, goi TomTom dung 1 lan/tuyen, tra ve list
  records trong memory.
- Buoc 2.5: append records vao `data/traffic_data.csv` (tao file + header
  neu chua co; khong overwrite du lieu cu).
- Buoc 2.6: hardening/error handling (validate input, API failure, response
  thieu field, schema CSV). Khong dedup, khong retry/backoff phuc tap.
- KHONG tinh congestion (Spark se tinh).
- KHONG scheduler / weather / Spark / Pandas / Streamlit.
"""

from datetime import datetime, timezone

try:
    from config import ROADS_CSV as _DEFAULT_ROADS_CSV
    from config import TRAFFIC_DATA_CSV as _DEFAULT_TRAFFIC_CSV
    from config import WEATHER_DATA_CSV as _DEFAULT_WEATHER_CSV
except ImportError:
    _DEFAULT_ROADS_CSV = None
    _DEFAULT_TRAFFIC_CSV = None
    _DEFAULT_WEATHER_CSV = None

try:
    from utils.traffic_api import fetch_traffic_flow
except ImportError:  # cho phep import truc tiep khi chay tu thu muc goc
    fetch_traffic_flow = None

try:
    from utils.weather_api import (
        WEATHER_RECORD_FIELDS,
        build_weather_record,
        fetch_weather,
    )
except ImportError:
    fetch_weather = None
    build_weather_record = None
    WEATHER_RECORD_FIELDS = None


def _utc_now_iso8601():
    """Timestamp tai thoi diem thu thap, ISO 8601, Spark parse duoc."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_traffic_record(road_name, lat, lon, flow_data, timestamp=None):
    """Tao 1 normalized traffic record tu road input + TomTom response.

    Args:
        road_name: ten tuyen lay tu `data/roads.csv`.
        lat, lon: toa do lay tu `data/roads.csv`.
        flow_data: dict tu `fetch_traffic_flow()` voi cac key TomTom goc:
            currentSpeed, freeFlowSpeed, currentTravelTime,
            freeFlowTravelTime, confidence.
        timestamp: ISO 8601 str (optional, de test). Mac dinh tao tai
            thoi diem thu thap.

    Returns:
        dict voi schema Phase 2 (9 field):
        timestamp, road_name, lat, lon, current_speed, free_flow_speed,
        current_travel_time, free_flow_travel_time, confidence.
    """
    if not isinstance(road_name, str) or not road_name.strip():
        raise ValueError(f"road_name khong hop le: {road_name!r}.")

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        raise ValueError(f"Toa do khong hop le: lat={lat!r}, lon={lon!r}.")

    if not isinstance(flow_data, dict):
        raise ValueError(f"flow_data phai la dict tu fetch_traffic_flow: {flow_data!r}.")

    try:
        current_speed = flow_data["currentSpeed"]
        free_flow_speed = flow_data["freeFlowSpeed"]
        current_travel_time = flow_data["currentTravelTime"]
        free_flow_travel_time = flow_data["freeFlowTravelTime"]
        confidence = flow_data["confidence"]
    except KeyError as exc:
        raise ValueError(f"flow_data thieu truong TomTom {exc}: {flow_data!r}.") from exc

    ts = timestamp if timestamp is not None else _utc_now_iso8601()
    if not isinstance(ts, str) or not ts.strip():
        raise ValueError(f"timestamp phai la str ISO 8601: {ts!r}.")

    return {
        "timestamp": ts,
        "road_name": road_name.strip(),
        "lat": lat_f,
        "lon": lon_f,
        "current_speed": current_speed,
        "free_flow_speed": free_flow_speed,
        "current_travel_time": current_travel_time,
        "free_flow_travel_time": free_flow_travel_time,
        "confidence": confidence,
    }


def load_roads(roads_csv=None):
    """Doc danh sach tuyen tu `data/roads.csv`.

    Bo qua row khong hop le (thieu road_name/lat/lon hoac toa do sai)
    va bao ro row bi bo qua. Khong goi API o day.
    """
    import csv

    path = roads_csv if roads_csv is not None else _DEFAULT_ROADS_CSV
    if path is None:
        raise RuntimeError("Khong xac dinh duoc duong dan roads.csv.")

    roads = []
    try:
        f = open(path, newline="", encoding="utf-8")
    except FileNotFoundError:
        raise RuntimeError(f"Khong tim thay file roads.csv: {path}.")
    with f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            name = (row.get("road_name") or "").strip()
            lat = row.get("lat")
            lon = row.get("lon")
            if not name:
                print(f"Bo qua dong {i}: thieu road_name.")
                continue
            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except (TypeError, ValueError):
                print(f"Bo qua tuyen {name!r} (dong {i}): toa do khong hop le.")
                continue
            if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
                print(f"Bo qua tuyen {name!r} (dong {i}): toa do ngoai pham vi.")
                continue
            roads.append({"road_name": name, "lat": lat_f, "lon": lon_f})
    return roads


def collect_all_roads(roads=None, roads_csv=None):
    """Thu thap traffic record cho tat ca tuyen hop le (trong memory).

    Moi tuyen hop le goi TomTom dung 1 lan:
    roads.csv -> fetch_traffic_flow() -> build_traffic_record().
    Tuyen loi API: bao road_name, bo qua (khong fake record), tiep tuc
    cac tuyen con lai. KHONG ghi file CSV.
    """
    if fetch_traffic_flow is None:
        raise RuntimeError("Khong import duoc fetch_traffic_flow tu utils.traffic_api.")

    road_list = list(roads) if roads is not None else load_roads(roads_csv)

    records = []
    for road in road_list:
        name = road.get("road_name")
        try:
            flow = fetch_traffic_flow(road.get("lat"), road.get("lon"))
            records.append(
                build_traffic_record(name, road.get("lat"), road.get("lon"), flow)
            )
        except Exception as exc:
            # Bao gom loi API/network, response thieu field, record khong hop le:
            # bo qua road nay (khong fake record), tiep tuc cac road con lai.
            print(f"Loi thu thap tuyen {name!r}: {exc}. Bo qua, tiep tuc.")
            continue
    return records


def collect_weather_for_roads(roads=None, roads_csv=None):
    """Thu thap weather record cho tat ca tuyen hop le (trong memory).

    Moi tuyen hop le goi Open-Meteo dung 1 lan:
    roads.csv -> fetch_weather() -> build_weather_record().
    Tuyen loi: bao road_name, bo qua (khong fake record), tiep tuc
    cac tuyen con lai. KHONG persistence o buoc nay.
    """
    if fetch_weather is None or build_weather_record is None:
        raise RuntimeError("Khong import duoc fetch_weather tu utils.weather_api.")

    road_list = list(roads) if roads is not None else load_roads(roads_csv)

    records = []
    for road in road_list:
        name = road.get("road_name")
        try:
            weather = fetch_weather(road.get("lat"), road.get("lon"))
            records.append(build_weather_record(road, weather))
        except Exception as exc:
            print(f"Loi thu thap weather tuyen {name!r}: {exc}. Bo qua, tiep tuc.")
            continue
    return records


TRAFFIC_CSV_FIELDS = [
    "timestamp",
    "road_name",
    "lat",
    "lon",
    "current_speed",
    "free_flow_speed",
    "current_travel_time",
    "free_flow_travel_time",
    "confidence",
]


def _validate_traffic_record(record):
    """Kiem tra 1 record dung khop schema truoc khi append.

    Thieu field hoac thua field ngoai schema -> reject (ValueError)
    de tranh ghi record khong hoan chinh / schema drift lam hong CSV.
    """
    if not isinstance(record, dict):
        raise ValueError(f"Record phai la dict, nhan: {record!r}.")
    keys = set(record.keys())
    expected = set(TRAFFIC_CSV_FIELDS)
    missing = expected - keys
    extra = keys - expected
    if missing:
        raise ValueError(f"Record thieu field {sorted(missing)}: {record!r}.")
    if extra:
        raise ValueError(f"Record thua field ngoai schema {sorted(extra)}: {record!r}.")
    return True


def save_traffic_records(records, output_path=None):
    """Append traffic records vao `data/traffic_data.csv` (csv chuan, UTF-8).

    - records rong: bao 0 records saved, khong tao file/fake data/row rong.
    - Validate schema moi record TRUOC khi mo file: record loi -> reject,
      khong ghi bat ky row nao, khong lam hong CSV.
    - File chua ton tai: tao file, ghi header dung 1 lan, append records.
    - File da ton tai: kiem tra header khop expected schema; neu lech
      -> KHONG append mu, raise loi de bao ve file hien tai.
    - Tra ve so records da luu.
    """
    records = list(records) if records else []
    if not records:
        print("0 records saved (danh sach rong).")
        return 0

    import csv
    from pathlib import Path

    for r in records:
        _validate_traffic_record(r)

    path = Path(output_path) if output_path is not None else _DEFAULT_TRAFFIC_CSV
    if path is None:
        raise RuntimeError("Khong xac dinh duoc duong dan traffic_data.csv.")
    path = Path(path)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists() and path.stat().st_size > 0
    if file_exists:
        with open(path, newline="", encoding="utf-8") as f:
            existing_header = next(csv.reader(f), None)
        if existing_header != list(TRAFFIC_CSV_FIELDS):
            raise ValueError(
                f"Header CSV hien tai khong khop schema (bao ve file, khong append): "
                f"{existing_header!r} vs {list(TRAFFIC_CSV_FIELDS)!r}."
            )

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRAFFIC_CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for r in records:
            writer.writerow({k: r[k] for k in TRAFFIC_CSV_FIELDS})

    print(f"{len(records)} records saved to {path}.")
    return len(records)


def _validate_weather_record(record):
    """Kiem tra 1 weather record dung khop schema truoc khi append.

    Thieu/thua field -> reject (ValueError), khong silently drop.
    Timestamp phai parse duoc va timezone-aware; cac field con lai
    validate type o muc toi thieu (khong meteorological range phuc tap).
    """
    from datetime import datetime

    if WEATHER_RECORD_FIELDS is None:
        raise RuntimeError("Khong load duoc WEATHER_RECORD_FIELDS.")
    if not isinstance(record, dict):
        raise ValueError(f"Record phai la dict, nhan: {record!r}.")
    keys = set(record.keys())
    expected = set(WEATHER_RECORD_FIELDS)
    missing = expected - keys
    extra = keys - expected
    if missing:
        raise ValueError(f"Record thieu field {sorted(missing)}: {record!r}.")
    if extra:
        raise ValueError(f"Record thua field ngoai schema {sorted(extra)}: {record!r}.")

    try:
        ts = datetime.fromisoformat(record["timestamp"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"timestamp khong parse duoc: {record['timestamp']!r}.") from exc
    if ts.tzinfo is None:
        raise ValueError(f"timestamp phai timezone-aware: {record['timestamp']!r}.")

    if not isinstance(record["road_name"], str) or not record["road_name"].strip():
        raise ValueError(f"road_name khong hop le: {record['road_name']!r}.")

    for field in ("lat", "lon", "weather_lat", "weather_lon", "temperature",
                  "humidity", "precipitation", "rain", "wind_speed",
                  "weather_code"):
        v = record[field]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ValueError(f"Field {field!r} phai numeric: {v!r}.")
    return True


def save_weather_records(records, output_path=None):
    """Append weather records vao `data/weather_data.csv` (csv chuan, UTF-8).

    Append-only raw observation log, khong dedup.
    - records rong: bao 0, khong tao file/fake data.
    - Validate TOAN BO batch TRUOC khi mo file: 1 record loi -> reject
      ca batch, khong partial append, khong lam hong CSV.
    - File chua ton tai: tao parent dir/file, ghi header dung 1 lan.
    - File da ton tai: header phai khop WEATHER_RECORD_FIELDS, neu lech
      -> raise, KHONG append/overwrite. Tra ve so records da luu.
    """
    records = list(records) if records else []
    if not records:
        print("0 weather records saved (danh sach rong).")
        return 0

    import csv
    from pathlib import Path

    if WEATHER_RECORD_FIELDS is None:
        raise RuntimeError("Khong load duoc WEATHER_RECORD_FIELDS.")

    for r in records:
        _validate_weather_record(r)

    path = Path(output_path) if output_path is not None else _DEFAULT_WEATHER_CSV
    if path is None:
        raise RuntimeError("Khong xac dinh duoc duong dan weather_data.csv.")
    path = Path(path)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists() and path.stat().st_size > 0
    if file_exists:
        with open(path, newline="", encoding="utf-8") as f:
            existing_header = next(csv.reader(f), None)
        if existing_header != list(WEATHER_RECORD_FIELDS):
            raise ValueError(
                f"Header CSV hien tai khong khop schema (bao ve file, khong append): "
                f"{existing_header!r} vs {list(WEATHER_RECORD_FIELDS)!r}."
            )

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(WEATHER_RECORD_FIELDS))
        if not file_exists:
            writer.writeheader()
        for r in records:
            writer.writerow({k: r[k] for k in WEATHER_RECORD_FIELDS})

    print(f"{len(records)} weather records saved to {path}.")
    return len(records)


def run_collection_cycle(roads_csv=None, traffic_output=None, weather_output=None):
    """Chay dung 1 historical collection cycle (Phase 4.1).

    - Load roads.csv dung 1 lan; dung cung road snapshot cho ca 2 nguon.
    - Collect traffic (best-effort per road) -> append traffic_data.csv.
    - Collect weather (best-effort per road) -> append weather_data.csv.
    - Hai nguon doc lap: mot ben fail khong rollback ben kia, khong fake.
    - Persistence error duoc propagate (khong report saved success gia).
    - Tra ve summary dict deterministic:
      roads_loaded, traffic_collected, traffic_failed, traffic_saved,
      weather_collected, weather_failed, weather_saved.
    - KHONG scheduler/loop/sleep; caller goi 1 lan = 1 cycle.
    """
    roads = load_roads(roads_csv)

    traffic_records = collect_all_roads(roads)
    weather_records = collect_weather_for_roads(roads)

    traffic_saved = save_traffic_records(traffic_records, traffic_output)
    weather_saved = save_weather_records(weather_records, weather_output)

    return {
        "roads_loaded": len(roads),
        "traffic_collected": len(traffic_records),
        "traffic_failed": len(roads) - len(traffic_records),
        "traffic_saved": traffic_saved,
        "weather_collected": len(weather_records),
        "weather_failed": len(roads) - len(weather_records),
        "weather_saved": weather_saved,
    }


def main():
    """Manual one-shot CLI (Phase 4.2): chay CHINH XAC 1 collection cycle.

    - Partial per-road API failures KHONG phai fatal: in summary, exit 0.
    - Fatal error (missing roads.csv, persistence failure, header mismatch):
      in 1 dong error ngan (khong traceback dai, khong secret), exit 1.
    """
    try:
        summary = run_collection_cycle()
    except Exception as exc:
        print(f"Collection cycle failed: {exc}")
        return 1

    print("=== Hanoi Traffic Analytics - Collection Cycle ===")
    print()
    print(f"Roads loaded:       {summary['roads_loaded']}")
    print()
    print("Traffic:")
    print(f"  Collected:        {summary['traffic_collected']}")
    print(f"  Failed:           {summary['traffic_failed']}")
    print(f"  Saved:            {summary['traffic_saved']}")
    print()
    print("Weather:")
    print(f"  Collected:        {summary['weather_collected']}")
    print(f"  Failed:           {summary['weather_failed']}")
    print(f"  Saved:            {summary['weather_saved']}")
    print()
    print("Collection cycle completed.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
