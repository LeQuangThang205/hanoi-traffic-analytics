"""Phase 9.1 - Data Update Orchestrator (synchronous backend, KHONG UI).

Dieu phoi 1 full update cycle:

  roads.csv
    -> collector.run_collection_cycle() (TomTom + Open-Meteo, best-effort)
    -> run_spark_etl() -> run_road/hourly/weather_analytics()
    -> processed/*.csv + structured result dict.

- TAI SU DUNG collector + spark_analysis; KHONG duplicate TomTom/Open-Meteo
  request logic, validation, congestion, join, analytics, export.
- Synchronous, KHONG background job/queue/Celery. Streamlit Phase 9.2 se goi
  run_data_update() truc tiep (hien spinner trong luc cho).
- Concurrency guard: process-local threading.Lock (non-blocking); neu update
  dang chay, call thu hai tra ve success=False ngay, khong start collection
  thu hai song song. KHONG Redis/DB/distributed lock.
- Spark gate (bao thu, dua tren collector best-effort contract): Spark chi
  chay khi collection step khong fatal VA co >= 1 saved observation moi
  (traffic_saved + weather_saved > 0). Per-road API failure khong chan Spark
  neu van co du lieu hop le; collection hoan toan khong thu duoc gi thi skip
  Spark thay vi regenerate processed giong het tu raw cu.
- Failure boundary (khong transaction phuc tap):
  - Collector fatal (exception: mat roads.csv, persistence loi, header lech)
    -> success=False, Spark 0 lan, khong gia vo thanh cong.
  - Spark exception -> success=False, spark_success=False; raw data vua thu
    duoc GIU NGUYEN (khong rollback, khong xoa lich su).
- collect_fn / spark_fn inject duoc cho tests (mock, KHONG quota that).
  Mac dinh dung implementation that. KHONG CLI main() o 9.1 de tranh chay
  production vo y tu terminal; real first update chay rieng co kiem soat.
- KHONG log secret (collector da sanitize error message, khong key).
"""

import threading
import time
from datetime import datetime, timezone

from collector import run_collection_cycle
from spark_analysis import (
    run_hourly_analytics,
    run_road_analytics,
    run_spark_etl,
    run_weather_analytics,
)

# Process-local guard chong double-run (VD double-click Streamlit 9.2).
_UPDATE_LOCK = threading.Lock()


def _utc_now_iso():
    """Timestamp ISO 8601 UTC (khong microsecond)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_spark_pipeline():
    """Chay ETL + 3 analytics theo thu tu phu thuoc (ETL truoc).

    Tra ve (metas, processed_files): metas la dict stage -> meta dict cua
    spark_analysis; processed_files la list output_path theo thu tu chay.
    Bat ky stage nao raise -> propagate (caller bao failure, raw giu).
    """
    metas = {}
    metas["etl"] = run_spark_etl()
    metas["road"] = run_road_analytics()
    metas["hourly"] = run_hourly_analytics()
    metas["weather"] = run_weather_analytics()
    files = [
        m.get("output_path") for m in metas.values()
        if isinstance(m, dict) and m.get("output_path")
    ]
    return metas, files


def _base_result(started_at):
    """Result dict khoi tao (success=False cho den khi full pipeline xong)."""
    return {
        "success": False,
        "started_at": started_at,
        "finished_at": None,
        "duration_seconds": None,
        "roads_loaded": 0,
        "traffic_collected": 0,
        "traffic_failed": 0,
        "traffic_saved": 0,
        "weather_collected": 0,
        "weather_failed": 0,
        "weather_saved": 0,
        "spark_success": False,
        "spark_stages": [],
        "processed_files": [],
        "error": None,
    }


def run_data_update(collect_fn=None, spark_fn=None):
    """Chay 1 full data update cycle (synchronous), tra ve result dict.

    Args:
        collect_fn: callable () -> collector summary dict
            (roads_loaded, traffic_collected/failed/saved,
            weather_collected/failed/saved). Default
            collector.run_collection_cycle (THAT: goi TomTom + Open-Meteo,
            append raw CSV). Test truyen mock.
        spark_fn: callable () -> (metas, processed_files). Default
            _default_spark_pipeline (THAT: 4 Spark jobs + export).
            Test truyen mock.

    Returns:
        dict voi success, started_at/finished_at (UTC ISO),
        duration_seconds, collection counters, spark_success,
        spark_stages, processed_files, error (None khi success).
    """
    collect = collect_fn or run_collection_cycle
    spark = spark_fn or _default_spark_pipeline

    started = _utc_now_iso()
    result = _base_result(started)

    if not _UPDATE_LOCK.acquire(blocking=False):
        result["finished_at"] = _utc_now_iso()
        result["duration_seconds"] = 0.0
        result["error"] = "Another data update is already running."
        return result

    t0 = time.monotonic()
    try:
        try:
            summary = collect()
            result["roads_loaded"] = summary["roads_loaded"]
            result["traffic_collected"] = summary["traffic_collected"]
            result["traffic_failed"] = summary["traffic_failed"]
            result["traffic_saved"] = summary["traffic_saved"]
            result["weather_collected"] = summary["weather_collected"]
            result["weather_failed"] = summary["weather_failed"]
            result["weather_saved"] = summary["weather_saved"]
        except Exception as exc:
            result["error"] = f"Collection failed: {exc}"
            return result

        if result["traffic_saved"] + result["weather_saved"] <= 0:
            result["error"] = (
                "Collection finished but no new observations were saved; "
                "Spark skipped."
            )
            return result

        try:
            metas, processed_files = spark()
        except Exception as exc:
            result["error"] = (
                f"Spark pipeline failed: {exc} "
                "(newly collected raw data kept, no rollback)"
            )
            return result

        result["spark_success"] = True
        result["spark_stages"] = list(metas.keys()) if isinstance(
            metas, dict) else []
        result["processed_files"] = list(processed_files or [])
        result["success"] = True
        return result
    finally:
        result["finished_at"] = _utc_now_iso()
        result["duration_seconds"] = round(time.monotonic() - t0, 3)
        _UPDATE_LOCK.release()
