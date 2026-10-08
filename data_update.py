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

# Versioned processed snapshot architecture (fix mixed-version window):
# pipeline build snapshot moi trong data/processed/snapshots/<version>/ rieng
# biet; trong luc build, current_snapshot.json van tro snapshot cu (A);
# dashboard chi doc A. Sau khi B hoan tat + validate PASS, pointer duoc
# replace atomically (temp sibling + os.replace) A -> B. Khong co
# mixed-version window, khong phu thuoc RAM fallback sau restart.
#
# Pham vi atomicity: 4 dashboard CSVs trong versioned snapshot
# (combined_data.csv + road/hourly/weather_summary.csv). Canonical Parquet
# (data/lake/curated/combined/combined_data.parquet) GIU NGUYEN ngoai versioned
# scope: van overwrite atomically per-file trong ETL (temp+os.replace) nhung
# khong tham gia versioned pointer (dashboard khong doc Parquet; ML doc
# Parquet co the thay parquet moi trong luc summaries chua xong — chap nhan
# cho MVP, da document).
#
# Legacy snapshot_ready.json + data/processed/*.csv giu lai lam compat/bootstrap
# (khong phai authoritative): dashboard dung khi chua co versioned pointer;
# sau publish versioned thanh cong, compat copies duoc refresh best-effort.
SNAPSHOT_MARKER_FILENAME = "snapshot_ready.json"

# Versioned snapshot pointer (authoritative cho dashboard).
SNAPSHOTS_DIRNAME = "snapshots"
CURRENT_SNAPSHOT_FILENAME = "current_snapshot.json"
SNAPSHOT_RETAIN_COUNT = 3


def default_snapshot_marker_path(processed_dir=None):
    """Default marker path data/processed/snapshot_ready.json (portable).

    Neu truyen processed_dir (str/Path) thi marker nam trong do (cho tests
    temp). Khong hard-code drive/path may.
    """
    from pathlib import Path

    if processed_dir is not None:
        return Path(processed_dir) / SNAPSHOT_MARKER_FILENAME
    try:
        from config import DATA_DIR as _DATA_DIR
        return Path(_DATA_DIR) / "processed" / SNAPSHOT_MARKER_FILENAME
    except ImportError:
        from pathlib import Path as _P
        return _P(__file__).resolve().parent / "data" / "processed" / SNAPSHOT_MARKER_FILENAME


def publish_snapshot_marker(metas, marker_path=None):
    """Publish snapshot marker SAU KHI full pipeline thanh cong (atomic).

    - Chi goi khi etl + road + hourly + weather deu xong (caller dam bao);
      khong bao gio publish giua chung -> dashboard khong thay mixed-version.
    - marker_path mac dinh: cung thu muc voi etl combined output (meta
      etl.output_path). Ghi temp sibling + os.replace; loi -> raise (khong
      publish nua voi, marker cu giu nguyen, cycle sau publish lai).
    - Tra ve marker dict (version, published_at, row counts). Khong API/Spark.
    """
    import json
    import os
    import uuid
    from pathlib import Path

    if not isinstance(metas, dict):
        raise ValueError(f"publish_snapshot_marker can metas dict, nhan: {type(metas)}.")
    etl = metas.get("etl", {})
    road = metas.get("road", {})
    hourly = metas.get("hourly", {})
    weather = metas.get("weather", {})
    for stage in ("etl", "road", "hourly", "weather"):
        if not isinstance(metas.get(stage), dict):
            raise ValueError(f"publish_snapshot_marker thieu meta stage: {stage}.")

    now_full = datetime.now(timezone.utc).isoformat()
    marker = {
        "version": now_full,
        "published_at": now_full,
        "combined_rows": int(etl.get("output_rows", 0) or 0),
        "road_summary_rows": int(road.get("output_rows", 0) or 0),
        "road_observation_sum": int(road.get("sum_observation_count", 0) or 0),
        "hourly_summary_rows": int(hourly.get("output_rows", 0) or 0),
        "hourly_observation_sum": int(hourly.get("sum_observation_count", 0) or 0),
        "weather_summary_rows": int(weather.get("output_rows", 0) or 0),
        "weather_observation_sum": int(weather.get("analyzed_rows", 0) or 0),
        "parquet_rows": int(etl.get("parquet_rows", 0) or 0),
        "traffic_input_rows": int(etl.get("traffic_rows", 0) or 0),
        "weather_input_rows": int(etl.get("weather_rows", 0) or 0),
    }
    if marker_path is not None:
        final = Path(marker_path)
    else:
        etl_out = etl.get("output_path")
        if not etl_out:
            raise ValueError("publish_snapshot_marker: thieu etl output_path.")
        final = Path(etl_out).parent / SNAPSHOT_MARKER_FILENAME
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.parent / f"{final.name}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(marker, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, final)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
    marker["path"] = str(final)
    return marker


def read_snapshot_marker(marker_path=None):
    """Doc snapshot marker; tra ve dict hoac None (pure IO, khong UI).

    Missing/corrupt/thieu truong -> None (caller tu fallback legacy +
    diagnostic ra terminal). Khong bao gio raise ve dashboard.
    """
    import json
    from pathlib import Path

    target = Path(marker_path) if marker_path is not None else default_snapshot_marker_path()
    try:
        with open(target, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    required_ints = (
        "combined_rows", "road_summary_rows", "road_observation_sum",
        "hourly_summary_rows", "hourly_observation_sum",
        "weather_summary_rows", "weather_observation_sum",
    )
    version = data.get("version")
    if not isinstance(version, str) or not version.strip():
        return None
    for key in required_ints:
        value = data.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
    return data


def check_snapshot_coherence(counts, marker):
    """So khop counts thuc te voi marker (pure, khong UI).

    counts: dict {combined_rows, road_observation_sum, hourly_observation_sum,
    weather_observation_sum, matched_rows}. marker: dict tu
    read_snapshot_marker (None -> ["no marker"]).
    Tra ve list diagnostics (rong = coherent). Caller tu print ra terminal,
    KHONG dua len dashboard UI.
    """
    if not isinstance(marker, dict):
        return ["snapshot marker unavailable (bootstrap/legacy mode)."]
    diag = []
    for key in ("combined_rows", "road_observation_sum",
                "hourly_observation_sum", "weather_observation_sum"):
        expected = marker.get(key)
        actual = (counts or {}).get(key)
        if expected != actual:
            diag.append(f"{key}: files={actual} != marker={expected} (transient mixed-version?).")
    return diag


def default_processed_dir():
    """Default data/processed dir (portable, pathlib)."""
    from pathlib import Path

    try:
        from config import DATA_DIR as _DATA_DIR
        return Path(_DATA_DIR) / "processed"
    except ImportError:
        from pathlib import Path as _P
        return _P(__file__).resolve().parent / "data" / "processed"


def default_snapshots_dir(processed_dir=None):
    """Default data/processed/snapshots dir (portable)."""
    from pathlib import Path

    base = Path(processed_dir) if processed_dir is not None else default_processed_dir()
    return base / SNAPSHOTS_DIRNAME


def default_current_snapshot_path(processed_dir=None):
    """Default data/processed/current_snapshot.json pointer (portable)."""
    from pathlib import Path

    base = Path(processed_dir) if processed_dir is not None else default_processed_dir()
    return base / CURRENT_SNAPSHOT_FILENAME


def generate_snapshot_version(now=None):
    """Sinh version unique + sortable + filesystem-safe (UTC, microseconds).

    Format: YYYYMMDDTHHMMSS_microsecondsZ (vd 20261004T115637_479767Z).
    Khong chua ":" (Windows-safe), sortable lexicographically, unique theo
    microsecond (2 builds cung microsecond la khong the trong pipeline
    tuan tu). Khong database/UUID framework.
    """
    ts = now if now is not None else datetime.now(timezone.utc)
    if ts.tzinfo is None:
        raise ValueError(f"generate_snapshot_version can aware datetime, nhan: {ts!r}.")
    utc = ts.astimezone(timezone.utc)
    return utc.strftime("%Y%m%dT%H%M%S_%fZ")


def snapshot_dir_for_version(version, processed_dir=None):
    """Thu muc snapshots/<version>/ (khong tao dir o day)."""
    from pathlib import Path

    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"snapshot version khong hop le: {version!r}.")
    if any(sep in version for sep in ("/", "\\", ":")):
        raise ValueError(f"snapshot version chua ky tu path nguy hiem: {version!r}.")
    base = Path(processed_dir) if processed_dir is not None else default_processed_dir()
    return base / SNAPSHOTS_DIRNAME / version


def read_current_snapshot(pointer_path=None):
    """Doc versioned pointer; tra ve dict hoac None (pure IO, khong UI).

    Missing/corrupt/thieu truong -> None (caller fallback legacy + diagnostic
    ra terminal). Khong bao gio raise ve dashboard. Khong absolute Windows
    path: snapshot phai la relative "snapshots/<version>".
    """
    import json
    from pathlib import Path

    target = Path(pointer_path) if pointer_path is not None else default_current_snapshot_path()
    try:
        with open(target, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    version = data.get("version")
    if not isinstance(version, str) or not version.strip():
        return None
    snapshot = data.get("snapshot")
    if not isinstance(snapshot, str) or not snapshot.strip():
        return None
    # Chi chap nhan relative snapshots/<version> (khong absolute/drive/traverse).
    if snapshot.startswith(("snapshots/", "snapshots\\")) is False:
        # Chap nhan "snapshots\\<v>" tren Windows nhung chuan hoa ve "/".
        return None
    rel_parts = snapshot.replace("\\", "/").split("/")
    if len(rel_parts) != 2 or rel_parts[0] != SNAPSHOTS_DIRNAME or not rel_parts[1].strip():
        return None
    if any(sep in rel_parts[1] for sep in ("/", "\\", ":", "..")):
        return None
    for key in ("combined_rows", "road_observation_sum",
                "hourly_observation_sum", "weather_observation_sum"):
        value = data.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
    return data


def check_versioned_coherence(counts, pointer):
    """So khop counts thuc te voi versioned pointer (pure, khong UI).

    Tra ve list diagnostics (rong = coherent). Giu marker check nghiem ngat,
    khong noi validation, khong hide error.
    """
    if not isinstance(pointer, dict):
        return ["versioned pointer unavailable."]
    diag = []
    for key in ("combined_rows", "road_observation_sum",
                "hourly_observation_sum", "weather_observation_sum"):
        expected = pointer.get(key)
        actual = (counts or {}).get(key)
        if expected != actual:
            diag.append(f"{key}: files={actual} != pointer={expected}.")
    return diag


def _write_json_atomic(final_path, payload):
    """Ghi JSON atomically (temp sibling + os.replace, UTF-8)."""
    import json
    import os
    import uuid
    from pathlib import Path

    final = Path(final_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.parent / f"{final.name}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, final)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
    return final


def publish_current_snapshot(pointer, pointer_path=None):
    """Publish versioned pointer atomically (temp + os.replace).

    pointer: dict da co version/snapshot/counts. Ghi xong tra ve pointer
    (kem path). Loi -> raise (pointer cu giu nguyen).
    """
    from pathlib import Path

    if not isinstance(pointer, dict):
        raise ValueError(f"publish_current_snapshot can pointer dict, nhan: {type(pointer)}.")
    final = Path(pointer_path) if pointer_path is not None else default_current_snapshot_path()
    _write_json_atomic(final, pointer)
    out = dict(pointer)
    out["path"] = str(final)
    return out


def cleanup_old_snapshots(processed_dir=None, keep=None, pointer_path=None):
    """Giu latest N snapshots, khong bao gio xoa current pointer target.

    Chi cleanup SAU successful publish. Loi cleanup KHONG fail pipeline
    (tra ve [] + print warning). Tra ve list version da xoa.
    """
    import shutil
    from pathlib import Path

    limit = SNAPSHOT_RETAIN_COUNT if keep is None else int(keep)
    base = Path(processed_dir) if processed_dir is not None else default_processed_dir()
    snaps = base / SNAPSHOTS_DIRNAME
    if not snaps.is_dir():
        return []
    current = read_current_snapshot(pointer_path if pointer_path is not None
                                    else base / CURRENT_SNAPSHOT_FILENAME)
    protected = None
    if isinstance(current, dict):
        try:
            protected = (base / str(current.get("snapshot")).replace("\\", "/")).resolve()
        except (OSError, ValueError):
            protected = None
    try:
        candidates = sorted(p.name for p in snaps.iterdir() if p.is_dir())
    except OSError as exc:
        print(f"[pipeline] snapshot retention list failed (ignored): {exc}")
        return []
    # Giu N moi nhat (version sortable nen sort ten = sort thoi gian).
    to_delete = candidates[:-limit] if len(candidates) > limit else []
    deleted = []
    for name in to_delete:
        target = snaps / name
        try:
            if protected is not None and target.resolve() == protected:
                continue
        except OSError:
            continue
        try:
            shutil.rmtree(target, ignore_errors=False)
            deleted.append(name)
        except OSError as exc:
            print(f"[pipeline] snapshot retention skip {name} (ignored): {exc}")
    return deleted


def _utc_now_iso():
    """Timestamp ISO 8601 UTC (khong microsecond)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_spark_pipeline(processed_dir=None, version=None):
    """Chay ETL + 3 analytics vao versioned staging roi publish pointer.

    - Build snapshot moi trong snapshots/<new-version>/ rieng biet; trong
      luc build, current_snapshot.json KHONG doi (dashboard van doc A).
    - ETL + road/hourly/weather ghi thang vao staging (khong overwrite
      current snapshot dir). Analytics doc staging combined (combined_path),
      khong doc legacy.
    - Validate coherence truoc publish (combined == road_sum == hourly_sum;
      weather_sum == matched). Invalid -> raise, xoa staging incomplete,
      pointer giu A.
    - Publish pointer CUOI CUNG bang temp + os.replace (atomic A->B).
      Pointer publish fail -> raise, staging giu lai de debug, pointer cu
      giu nguyen.
    - Sau publish thanh cong: refresh legacy compat copies best-effort
      (data/processed/*.csv + snapshot_ready.json, KHONG authoritative) +
      retention latest 3 (khong bao gio xoa current; cleanup fail khong
      fail pipeline).
    - Parquet canonical (data/lake/...) van overwrite trong ETL, ngoai
      versioned atomic scope (dashboard khong doc Parquet; da document).
    - Tra ve (metas, processed_files) voi processed_files la 4 staging paths.
      Bat ky stage nao raise -> propagate (raw giu, pointer giu).
    """
    import shutil
    from pathlib import Path

    base = Path(processed_dir) if processed_dir is not None else default_processed_dir()
    snaps = base / SNAPSHOTS_DIRNAME
    snaps.mkdir(parents=True, exist_ok=True)
    new_version = version if version is not None else generate_snapshot_version()
    if not isinstance(new_version, str) or not new_version.strip():
        raise ValueError(f"snapshot version khong hop le: {new_version!r}.")
    staging = snaps / new_version
    staging.mkdir(parents=True, exist_ok=True)

    combined_p = staging / "combined_data.csv"
    road_p = staging / "road_summary.csv"
    hourly_p = staging / "hourly_summary.csv"
    weather_p = staging / "weather_summary.csv"

    try:
        metas = {}
        metas["etl"] = run_spark_etl(output_path=str(combined_p))
        metas["road"] = run_road_analytics(
            output_path=str(road_p), combined_path=str(combined_p))
        metas["hourly"] = run_hourly_analytics(
            output_path=str(hourly_p), combined_path=str(combined_p))
        metas["weather"] = run_weather_analytics(
            output_path=str(weather_p), combined_path=str(combined_p))
    except Exception:
        # Build fail: xoa staging incomplete (best-effort), pointer giu A.
        try:
            shutil.rmtree(staging, ignore_errors=True)
        except OSError:
            pass
        raise

    try:
        etl = metas.get("etl", {})
        road = metas.get("road", {})
        hourly = metas.get("hourly", {})
        weather = metas.get("weather", {})
        combined_rows = int(etl.get("output_rows", 0) or 0)
        road_sum = int(road.get("sum_observation_count", 0) or 0)
        hourly_sum = int(hourly.get("sum_observation_count", 0) or 0)
        weather_sum = int(weather.get("analyzed_rows", 0) or 0)
        matched = int(etl.get("matched_rows", 0) or 0)
        # Validation nghiem ngat truoc publish (khong noi, khong hide).
        problems = []
        if combined_rows <= 0:
            problems.append(f"combined_rows={combined_rows} (empty).")
        if combined_rows != road_sum:
            problems.append(f"combined {combined_rows} != road_sum {road_sum}.")
        if combined_rows != hourly_sum:
            problems.append(f"combined {combined_rows} != hourly_sum {hourly_sum}.")
        if weather_sum != matched:
            problems.append(f"weather_sum {weather_sum} != matched {matched}.")
        if problems:
            raise ValueError("versioned snapshot invalid counts: " + "; ".join(problems))
        # Dam bao 4 files staging ton tai truoc publish.
        for p in (combined_p, road_p, hourly_p, weather_p):
            if not p.is_file():
                raise ValueError(f"versioned snapshot thieu file: {p}.")

        published_at = datetime.now(timezone.utc).isoformat()
        pointer = {
            "version": new_version,
            "published_at": published_at,
            "snapshot": f"{SNAPSHOTS_DIRNAME}/{new_version}",
            "combined_rows": combined_rows,
            "road_summary_rows": int(road.get("output_rows", 0) or 0),
            "road_observation_sum": road_sum,
            "hourly_summary_rows": int(hourly.get("output_rows", 0) or 0),
            "hourly_observation_sum": hourly_sum,
            "weather_summary_rows": int(weather.get("output_rows", 0) or 0),
            "weather_observation_sum": weather_sum,
            "parquet_rows": int(etl.get("parquet_rows", 0) or 0),
            "traffic_input_rows": int(etl.get("traffic_rows", 0) or 0),
            "weather_input_rows": int(etl.get("weather_rows", 0) or 0),
        }
        publish_current_snapshot(pointer, pointer_path=base / CURRENT_SNAPSHOT_FILENAME)
    except Exception:
        # Khong publish khi invalid; xoa staging de khong rac (pointer giu A).
        try:
            shutil.rmtree(staging, ignore_errors=True)
        except OSError:
            pass
        raise

    # Compat best-effort (khong authoritative, fail khong fail pipeline).
    try:
        _refresh_legacy_compat(base, staging, metas)
    except Exception as exc:
        print(f"[pipeline] legacy compat refresh skipped (ignored): {exc}")
    try:
        cleanup_old_snapshots(
            processed_dir=str(base), keep=SNAPSHOT_RETAIN_COUNT,
            pointer_path=str(base / CURRENT_SNAPSHOT_FILENAME))
    except Exception as exc:
        print(f"[pipeline] snapshot retention skipped (ignored): {exc}")

    files = [str(combined_p), str(road_p), str(hourly_p), str(weather_p)]
    return metas, files


def _refresh_legacy_compat(base_dir, staging_dir, metas):
    """Refresh legacy data/processed/*.csv + snapshot_ready.json (best-effort).

    Chi goi SAU khi versioned pointer da publish thanh cong. Copy 4 staging
    CSVs ra legacy paths bang temp + os.replace tung file (per-file atomic;
    legacy readers co the thay transient mixed — chap nhan vi dashboard
    KHONG doc legacy nua). Legacy marker publish best-effort. Loi -> raise
    de caller log + bo qua (khong fail pipeline chinh).
    """
    import shutil
    from pathlib import Path

    base = Path(base_dir)
    staging = Path(staging_dir)
    pairs = (
        ("combined_data.csv", "combined_data.csv"),
        ("road_summary.csv", "road_summary.csv"),
        ("hourly_summary.csv", "hourly_summary.csv"),
        ("weather_summary.csv", "weather_summary.csv"),
    )
    for src_name, _dst_name in pairs:
        src = staging / src_name
        dst = base / src_name
        tmp = dst.parent / f"{dst.name}.compat-tmp"
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, tmp)
            import os as _os
            _os.replace(tmp, dst)
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
    # Legacy marker cho tools cu (authoritative hien tai la versioned pointer).
    try:
        publish_snapshot_marker(metas, marker_path=base / SNAPSHOT_MARKER_FILENAME)
    except Exception as exc:
        print(f"[pipeline] legacy marker refresh skipped (ignored): {exc}")


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
        "snapshot_marker": None,
        "current_snapshot": None,
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
        result["snapshot_marker"] = read_snapshot_marker()
        result["current_snapshot"] = read_current_snapshot()
        result["success"] = True
        return result
    finally:
        result["finished_at"] = _utc_now_iso()
        result["duration_seconds"] = round(time.monotonic() - t0, 3)
        _UPDATE_LOCK.release()


def run_process_only(spark_fn=None):
    """Chay Spark-only pipeline tu raw hien co, KHONG collection/API.

    Reuse _default_spark_pipeline (run_spark_etl + road/hourly/weather
    analytics) — khong duplicate ETL. TUYET DOI khong goi collect_fn,
    run_collection_cycle, TomTom, Open-Meteo, requests.get.

    Args:
        spark_fn: callable () -> (metas, processed_files). Default
            _default_spark_pipeline (THAT: 4 Spark jobs + export).
            Test truyen mock.

    Returns:
        dict voi success, started_at/finished_at (UTC ISO),
        duration_seconds, traffic_input_rows, weather_input_rows,
        combined_output_rows, matched/unmatched rows, road/hourly/
        weather summary rows, parquet_rows/parquet_path, spark_stages,
        processed_files, error (None khi success).
    """
    spark = spark_fn or _default_spark_pipeline

    started = _utc_now_iso()
    result = {
        "success": False,
        "mode": "process-only",
        "started_at": started,
        "finished_at": None,
        "duration_seconds": None,
        "traffic_input_rows": 0,
        "weather_input_rows": 0,
        "combined_output_rows": 0,
        "matched_rows": 0,
        "unmatched_rows": 0,
        "road_summary_rows": 0,
        "hourly_summary_rows": 0,
        "weather_summary_rows": 0,
        "parquet_rows": 0,
        "parquet_path": None,
        "spark_stages": [],
        "processed_files": [],
        "snapshot_marker": None,
        "current_snapshot": None,
        "error": None,
    }

    if not _UPDATE_LOCK.acquire(blocking=False):
        result["finished_at"] = _utc_now_iso()
        result["duration_seconds"] = 0.0
        result["error"] = "Another data update is already running."
        return result

    t0 = time.monotonic()
    try:
        try:
            metas, processed_files = spark()
        except Exception as exc:
            result["error"] = f"Spark pipeline failed: {exc}"
            return result

        etl = metas.get("etl", {}) if isinstance(metas, dict) else {}
        road = metas.get("road", {}) if isinstance(metas, dict) else {}
        hourly = metas.get("hourly", {}) if isinstance(metas, dict) else {}
        weather = metas.get("weather", {}) if isinstance(metas, dict) else {}

        result["traffic_input_rows"] = etl.get("traffic_rows", 0)
        result["weather_input_rows"] = etl.get("weather_rows", 0)
        result["combined_output_rows"] = etl.get("output_rows", 0)
        result["matched_rows"] = etl.get("matched_rows", 0)
        result["unmatched_rows"] = etl.get("unmatched_rows", 0)
        result["road_summary_rows"] = road.get("output_rows", 0)
        result["hourly_summary_rows"] = hourly.get("output_rows", 0)
        result["weather_summary_rows"] = weather.get("output_rows", 0)
        result["parquet_rows"] = etl.get("parquet_rows", 0)
        result["parquet_path"] = etl.get("parquet_path")
        result["spark_stages"] = list(metas.keys()) if isinstance(
            metas, dict) else []
        result["processed_files"] = list(processed_files or [])
        result["snapshot_marker"] = read_snapshot_marker()
        result["current_snapshot"] = read_current_snapshot()
        result["success"] = True
        return result
    finally:
        result["finished_at"] = _utc_now_iso()
        result["duration_seconds"] = round(time.monotonic() - t0, 3)
        _UPDATE_LOCK.release()


def parse_args(argv=None):
    """Parse CLI args (pure, khong side-effect; import-safe)."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Hanoi Traffic Analytics data update (Spark ETL, no auto-run)."
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="Chi chay Spark ETL + analytics tu raw hien co; khong goi API.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """CLI entry (chi chay duoi __main__; import khong tu chay)."""
    args = parse_args(argv)
    if not args.process_only:
        print("Chua chon tac vu. Dung: python data_update.py --process-only")
        return 2

    t0 = time.monotonic()
    result = run_process_only()
    duration = round(time.monotonic() - t0, 3)

    print(f"mode: process-only (no API calls)")
    print(f"traffic input rows: {result['traffic_input_rows']}")
    print(f"weather input rows: {result['weather_input_rows']}")
    print(f"combined output rows: {result['combined_output_rows']}")
    print(f"matched rows: {result['matched_rows']}")
    print(f"unmatched rows: {result['unmatched_rows']}")
    print(f"road_summary rows: {result['road_summary_rows']}")
    print(f"hourly_summary rows: {result['hourly_summary_rows']}")
    print(f"weather_summary rows: {result['weather_summary_rows']}")
    print(f"parquet rows: {result['parquet_rows']}")
    print(f"parquet path: {result['parquet_path']}")
    print(f"processed files: {result['processed_files']}")
    print(f"spark stages: {result['spark_stages']}")
    marker = result.get("snapshot_marker") or {}
    print(f"snapshot marker: {marker.get('version') or 'not published'}")
    current = result.get("current_snapshot") or {}
    print(f"current snapshot: {current.get('version') or 'not published'}"
          f" ({current.get('snapshot') or '-'})")
    print(f"duration: {duration}s")
    if result["success"]:
        print("SUCCESS: processed snapshot regenerated without API calls.")
        return 0
    print(f"FAILED: {result['error']}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
