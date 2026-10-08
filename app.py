"""Phase 7.1-7.6 — Streamlit Dashboard (presentation layer only).

Doc versioned processed snapshot qua current_snapshot.json
(snapshots/<version>/) va hien thi thong tin co ban + KPI tong quan.
KHONG Spark/pyspark, KHONG rerun ETL/analytics, KHONG TomTom/Open-Meteo,
KHONG ghi/sua bat ky CSV nao (read-only). Pandas chi dung o presentation
layer cho du lieu nho (KPI don gian, khong thay Spark).

Near-realtime: fragment `_auto_refresh_tick()` tu chay moi 30s (native
st.fragment run_every); khi snapshot key doi thi full app rerun de doc du
lieu moi. Versioned publishing: pipeline build snapshot moi trong
snapshots/<version>/ rieng; dashboard chi doc snapshot ma
current_snapshot.json dang tro toi (pointer atomic A->B sau FULL process +
validate). Khong bao gio doc mixed-version (combined B + summary A).
Bootstrap khi chua co pointer: dung legacy coherent snapshot neu validate
duoc. Auto-refresh CHI doc + render, TUYET DOI KHONG goi collector/API/Spark
(pipeline chi chay tu historical_collector --continuous hoac nut manual khi
user click).
"""

from pathlib import Path

import folium
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from branca.element import MacroElement, Template
from streamlit_folium import st_folium

from config import HANOI_ROAD_CATALOG, find_catalog_entry
from utils import road_manager

import data_update

BASE_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

COMBINED_CSV = PROCESSED_DIR / "combined_data.csv"
ROAD_SUMMARY_CSV = PROCESSED_DIR / "road_summary.csv"
HOURLY_SUMMARY_CSV = PROCESSED_DIR / "hourly_summary.csv"
WEATHER_SUMMARY_CSV = PROCESSED_DIR / "weather_summary.csv"

REQUIRED_FILES = {
    "combined_data.csv": COMBINED_CSV,
    "road_summary.csv": ROAD_SUMMARY_CSV,
    "hourly_summary.csv": HOURLY_SUMMARY_CSV,
    "weather_summary.csv": WEATHER_SUMMARY_CSV,
}

# Minimum columns dashboard can cu de hien thi (thieu -> bao loi ro rang).
REQUIRED_COLUMNS = {
    "combined_data.csv": [
        "timestamp", "event_timestamp", "road_name", "lat", "lon",
        "current_speed", "free_flow_speed", "congestion_percent",
        "congestion_level", "local_hour", "weather_matched",
        "temperature", "rain",
    ],
    "road_summary.csv": [
        "road_name", "observation_count", "avg_speed", "avg_congestion_percent",
    ],
    "hourly_summary.csv": [
        "local_hour", "time_window", "observation_count",
        "avg_speed", "avg_congestion_percent",
    ],
    "weather_summary.csv": [
        "weather_condition", "observation_count",
        "avg_speed", "avg_congestion_percent",
    ],
}

HANOI_TZ = "Asia/Ho_Chi_Minh"

# Near-realtime dashboard auto-refresh (native Streamlit, read-only).
# Fragment nho `_auto_refresh_tick()` tu chay moi 30s; neu file signature
# cua processed snapshot doi (Spark vua replace) thi kich hoat 1 full app
# rerun de doc snapshot moi. Refresh KHONG goi API/Spark/collector.
AUTO_REFRESH_SECONDS = 30

# Marker color theo Phase 5 congestion_level (source of truth, khong suy
# tu congestion_percent lam tron, khong random).
CONGESTION_COLORS = {
    "Thông thoáng": "green",
    "Đông": "orange",
    "Ùn tắc": "red",
    "Ùn tắc nghiêm trọng": "darkred",
}

# Emoji cho legend ban do (presentation only; thu tu lay tu data/CONGESTION_COLORS).
STATUS_EMOJI = {
    "Thông thoáng": "🟢",
    "Đông": "🟡",
    "Ùn tắc": "🟠",
    "Ùn tắc nghiêm trọng": "🔴",
}

KPI_ICONS = {
    "n_roads": "🛣️",
    "n_obs": "📊",
    "avg_speed": "🚗",
    "avg_congestion": "🚦",
    "matched_pct": "🌦️",
    "latest_hanoi": "🕐",
}

# ---- Dashboard theme: assets/dashboard.css (khong framework/CDN) ----
# CSS nam rieng de app.py khoi phinh to; inject_css doc file + wrap <style>.
DASHBOARD_CSS_PATH = BASE_DIR / "assets" / "dashboard.css"


def inject_css(css_path=None):
    """Inject dashboard CSS tu assets/dashboard.css (scoped, dark theme)."""
    target = Path(css_path) if css_path is not None else DASHBOARD_CSS_PATH
    with open(target, "r", encoding="utf-8") as fh:
        css = fh.read()
    st.markdown(f"<style>\n{css}\n</style>", unsafe_allow_html=True)


def find_missing_files(paths=None):
    """Tra ve list ten file required nhung khong ton tai (pure, khong UI)."""
    targets = paths or REQUIRED_FILES
    return [name for name, path in targets.items() if not Path(path).is_file()]


def find_missing_columns(frames):
    """Kiem tra schema toi thieu; tra ve {file: [missing cols]} (pure).

    `frames` la dict ten-file -> DataFrame. Khong tu tao cot.
    """
    problems = {}
    for name, required in REQUIRED_COLUMNS.items():
        df = frames.get(name)
        if df is None:
            continue
        missing = [c for c in required if c not in df.columns]
        if missing:
            problems[name] = missing
    return problems


def parse_event_timestamps(combined_df):
    """Parse ban copy event_timestamp sang UTC (pure, khong sua source).

    Tra ve (parsed_series, invalid_count). Invalid -> NaT, caller tu bao.
    """
    parsed = pd.to_datetime(combined_df["event_timestamp"], utc=True, errors="coerce")
    return parsed, int(parsed.isna().sum())


def to_hanoi(series_utc):
    """Chuyen Series UTC sang Asia/Ho_Chi_Minh in-memory (pure)."""
    return series_utc.dt.tz_convert(HANOI_TZ)


def normalize_matched(series):
    """Chuan hoa weather_matched thanh boolean mask (pure, strict).

    Chap nhan: True/False (bool), 1/0, "true"/"false" (case-insensitive,
    bao gom dang CSV cua Phase 5). KHONG dung astype(bool) vi string
    "False" se thanh truthy. Tra ve (mask, invalid_count); gia tri la
    (nhu "yes", NULL/NaN, so la) dem vao invalid, caller tu bao loi.
    """
    def _convert(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            if value == 1:
                return True
            if value == 0:
                return False
            return None
        if isinstance(value, float):
            if pd.isna(value):
                return None
            if value == 1.0:
                return True
            if value == 0.0:
                return False
            return None
        text = str(value).strip().lower()
        if text == "true":
            return True
        if text == "false":
            return False
        return None

    mapped = series.map(_convert)
    invalid = mapped.isna()
    mask = mapped.eq(True)
    return mask, int(invalid.sum())


def matched_mask(combined_df):
    """Boolean mask cho weather_matched (giữ tương thích 7.1).

    Lỏng ở mức helper: giá trị lạ được coi là False tại đây; validation
    nghiêm ngặt nằm ở calculate_overview_metrics (báo lỗi rõ ràng).
    """
    mask, _ = normalize_matched(combined_df["weather_matched"])
    return mask


def _require_numeric(series, column):
    """Ep numeric strict: non-numeric/NULL -> ValueError (pure)."""
    parsed = pd.to_numeric(series, errors="coerce")
    n_bad = int(parsed.isna().sum())
    if n_bad:
        raise ValueError(
            f"Cột {column} có {n_bad} giá trị không phải số/hợp lệ — "
            "kiểm tra lại Phase 5 thay vì điền 0 hay bỏ qua ngầm."
        )
    return parsed


def calculate_overview_metrics(combined_df, road_df, hourly_df, weather_df, event_hanoi):
    """Tinh 6 KPI tong quan tu processed snapshots (pure, testable).

    Chi tong hop presentation-level don gian (len/nunique/mean/sum);
    KHONG recreate Spark analytics. Tra ve dict so lieu tho + warnings
    reconciliation (UI tu format/hien thi). Malformed -> ValueError.
    """
    speed = _require_numeric(combined_df["current_speed"], "current_speed")
    congestion = _require_numeric(combined_df["congestion_percent"], "congestion_percent")
    mask, n_invalid_matched = normalize_matched(combined_df["weather_matched"])
    if n_invalid_matched:
        raise ValueError(
            f"Cột weather_matched có {n_invalid_matched} giá trị lạ "
            "(chỉ chấp nhận true/false) — kiểm tra lại Phase 5."
        )

    n_obs = len(combined_df)
    n_roads = int(combined_df["road_name"].nunique())
    n_matched = int(mask.sum())

    warnings = []
    if not road_df.empty and len(road_df) != n_roads:
        warnings.append(
            f"Số tuyến trong road_summary ({len(road_df)}) khác "
            f"số tuyến quan sát được ({n_roads})."
        )
    for label, df in (("road_summary", road_df), ("hourly_summary", hourly_df)):
        if not df.empty and int(df["observation_count"].sum()) != n_obs:
            warnings.append(
                f"Tổng observation_count trong {label} "
                f"({int(df['observation_count'].sum())}) khác số quan trắc ({n_obs})."
            )
    if not weather_df.empty and int(weather_df["observation_count"].sum()) != n_matched:
        warnings.append(
            f"Tổng observation_count trong weather_summary "
            f"({int(weather_df['observation_count'].sum())}) khác "
            f"số quan trắc ghép thời tiết ({n_matched})."
        )

    top_road = None
    if not road_df.empty:
        ranked = road_df.sort_values(
            ["avg_congestion_percent", "road_name"], ascending=[False, True]
        ).iloc[0]
        top_road = {
            "road_name": str(ranked["road_name"]),
            "avg_congestion_percent": float(ranked["avg_congestion_percent"]),
        }

    weather_counts = {}
    if not weather_df.empty:
        for _, row in weather_df.iterrows():
            weather_counts[str(row["weather_condition"])] = int(row["observation_count"])

    return {
        "n_roads": n_roads,
        "n_obs": n_obs,
        "avg_speed": float(speed.mean()),
        "avg_congestion": float(congestion.mean()),
        "n_matched": n_matched,
        "matched_pct": (n_matched / n_obs * 100.0) if n_obs else 0.0,
        "latest_hanoi": event_hanoi.max(),
        "warnings": warnings,
        "top_road": top_road,
        "weather_counts": weather_counts,
    }


def snapshot_signature(combined=None, road=None, hourly=None, weather=None):
    """File version key cho 4 processed snapshots (pure, read-only).

    Tra ve tuple (path, mtime_ns, size) cho moi file; file thieu -> (path,
    None, None). Spark replace file bang temp sibling + os.replace nen
    mtime doi moi lan snapshot moi hoan tat. Dung lam cache key de
    dashboard khong giu stale data. Khong doc noi dung file o day.
    """
    targets = [
        str(combined or COMBINED_CSV),
        str(road or ROAD_SUMMARY_CSV),
        str(hourly or HOURLY_SUMMARY_CSV),
        str(weather or WEATHER_SUMMARY_CSV),
    ]
    sig = []
    for target in targets:
        try:
            info = Path(target).stat()
            sig.append((target, info.st_mtime_ns, info.st_size))
        except OSError:
            sig.append((target, None, None))
    return tuple(sig)


@st.cache_data
def _load_snapshots_cached(sig, paths):
    """Doc 4 processed CSV (cached theo file signature; read-only).

    sig thay doi -> cache miss -> doc lai file moi. Khong goi API/Spark,
    khong ghi file. sig/paths la tuple hashable (LUU Y: khong dat ten param
    bat dau bang "_" vi Streamlit se loai khoi cache key -> stale vinh vien).
    """
    combined_p, road_p, hourly_p, weather_p = paths
    return (
        pd.read_csv(combined_p),
        pd.read_csv(road_p),
        pd.read_csv(hourly_p),
        pd.read_csv(weather_p),
    )


def load_dashboard_data(combined=None, road=None, hourly=None, weather=None):
    """Doc 4 processed snapshots, tu bust cache khi file thay doi.

    Khong arg -> doc snapshot production (behavior cu giu nguyen).
    Truyen paths (str/Path) de test voi snapshot tam. Chi doc processed
    snapshot, KHONG doc raw CSV, KHONG goi API/Spark/collector.
    """
    paths = (
        str(combined or COMBINED_CSV),
        str(road or ROAD_SUMMARY_CSV),
        str(hourly or HOURLY_SUMMARY_CSV),
        str(weather or WEATHER_SUMMARY_CSV),
    )
    return _load_snapshots_cached(snapshot_signature(*paths), paths)


DASHBOARD_PREV_SNAPSHOT_KEY = "dashboard_prev_snapshot"
DASHBOARD_SNAPSHOT_SIG_KEY = "dashboard_snapshot_sig"
DASHBOARD_COHERENT_KEY = "dashboard_coherent_key"

# Versioned snapshot pointer (authoritative): pipeline build snapshot moi trong
# snapshots/<version>/ rieng; trong luc build, current_snapshot.json van tro A;
# dashboard chi doc A. Sau FULL process + validate, pointer replace atomically
# A -> B. Khong bao gio doc mixed-version (combined B + summary A).
# Legacy snapshot_ready.json + data/processed/*.csv chi dung bootstrap khi chua
# co pointer (validate coherent moi render); lan process tiep theo tao
# versioned snapshot dau tien. Khong copy legacy mixed vao versioned storage.
SNAPSHOT_MARKER = PROCESSED_DIR / "snapshot_ready.json"
CURRENT_SNAPSHOT_JSON = PROCESSED_DIR / "current_snapshot.json"
SNAPSHOTS_DIR = PROCESSED_DIR / "snapshots"


@st.fragment(run_every=AUTO_REFRESH_SECONDS)
def _auto_refresh_tick():
    """Tick nhe chay moi 30s (native fragment, read-only, khong UI).

    Fragment rieng nho nen khong vuong snapshot nhu ca dashboard. Watch
    snapshot key (marker version khi co, file-sig khi bootstrap) thay vi
    mtime tung file le. Key doi -> 1 full app rerun de main() load snapshot
    moi; khong doi -> khong rerun, khong ve lai, khong cham widget/filter.
    TUYET DOI KHONG goi TomTom/Open-Meteo/collector/run_data_update/
    run_process_only/Spark o day.
    """
    try:
        key, _ = current_snapshot_key()
    except Exception:
        return
    last = st.session_state.get(DASHBOARD_SNAPSHOT_SIG_KEY)
    if last is None:
        st.session_state[DASHBOARD_SNAPSHOT_SIG_KEY] = key
        return
    if key != last:
        st.session_state[DASHBOARD_SNAPSHOT_SIG_KEY] = key
        st.rerun(scope="app")


def _resolve_snapshot_paths(combined=None, road=None, hourly=None, weather=None):
    """4 processed paths dang str tuple (production default hoac temp test)."""
    return (
        str(combined or COMBINED_CSV),
        str(road or ROAD_SUMMARY_CSV),
        str(hourly or HOURLY_SUMMARY_CSV),
        str(weather or WEATHER_SUMMARY_CSV),
    )


def resolve_versioned_paths(pointer, processed_dir=None):
    """4 CSV paths trong snapshots/<version>/ tu pointer (str tuple).

    pointer: dict tu data_update.read_current_snapshot (da validate).
    Khong absolute path: snapshot la relative "snapshots/<version>".
    """
    from pathlib import Path as _P

    base = _P(processed_dir) if processed_dir is not None else PROCESSED_DIR
    rel = str(pointer.get("snapshot")).replace("\\", "/")
    snap_dir = base / rel
    return (
        str(snap_dir / "combined_data.csv"),
        str(snap_dir / "road_summary.csv"),
        str(snap_dir / "hourly_summary.csv"),
        str(snap_dir / "weather_summary.csv"),
    )


def current_snapshot_key(paths=None, marker_path=None, pointer_path=None):
    """Snapshot key dashboard watch (pure IO, khong UI).

    Thu tu uu tien (authoritative):
    1. Versioned pointer hop le -> ("versioned", version): chi doi khi FULL
       pipeline publish xong (atomic A->B) -> refresh khong bao gio thay
       transient mixed-version. Pointer khong doi -> khong doi snapshot.
    2. Legacy marker hop le (bootstrap, chua co pointer) -> ("marker", version).
    3. Bootstrap khong marker -> ("files", file-sig) nhu behavior cu.
    Tra ve (key, pointer_or_marker_or_None). Khong bao gio raise.
    """
    try:
        pointer = data_update.read_current_snapshot(
            pointer_path if pointer_path is not None else CURRENT_SNAPSHOT_JSON
        )
    except Exception:
        pointer = None
    if isinstance(pointer, dict) and str(pointer.get("version") or "").strip():
        return ("versioned", str(pointer["version"])), pointer
    resolved = tuple(paths) if paths is not None else _resolve_snapshot_paths()
    try:
        marker = data_update.read_snapshot_marker(
            marker_path if marker_path is not None else SNAPSHOT_MARKER
        )
    except Exception:
        marker = None
    if isinstance(marker, dict) and str(marker.get("version") or "").strip():
        return ("marker", str(marker["version"])), marker
    return ("files", snapshot_signature(*resolved)), None


def _safe_obs_sum(df):
    """Tong observation_count (so nguyen >= 0); lech kieu -> -1 (lech marker)."""
    try:
        return int(pd.to_numeric(
            df["observation_count"], errors="coerce").fillna(0).sum())
    except (KeyError, TypeError, ValueError):
        return -1


def snapshot_frame_counts(combined_df, road_df, hourly_df, weather_df):
    """Counts thuc te cua 4 frames de so voi marker (pure, khong UI)."""
    matched, _ = normalize_matched(combined_df["weather_matched"])
    return {
        "combined_rows": int(len(combined_df)),
        "road_observation_sum": _safe_obs_sum(road_df),
        "hourly_observation_sum": _safe_obs_sum(hourly_df),
        "weather_observation_sum": _safe_obs_sum(weather_df),
        "matched_rows": int(matched.sum()),
    }


def _legacy_reconciliation_diagnostics(counts):
    """Internal reconciliation khi chua co marker (pure, terminal-only).

    Bootstrap: van render files hien co; diagnostics chi de debug,
    KHONG dua len UI.
    """
    diag = []
    n = counts.get("combined_rows")
    for label, key in (("road_summary", "road_observation_sum"),
                       ("hourly_summary", "hourly_observation_sum")):
        if counts.get(key) != n:
            diag.append(
                f"{label} sum {counts.get(key)} != combined {n} (legacy, no marker).")
    if counts.get("weather_observation_sum") != counts.get("matched_rows"):
        diag.append(
            f"weather_summary sum {counts.get('weather_observation_sum')} != "
            f"matched {counts.get('matched_rows')} (legacy, no marker).")
    return diag


def _validate_frames(frames):
    """Validate schema + timestamps + tra ve counts (raise khi invalid)."""
    problems = find_missing_columns(dict(zip(REQUIRED_FILES, frames)))
    if problems:
        raise ValueError(f"schema problems: {problems}.")
    combined_df = frames[0]
    if combined_df.empty:
        raise ValueError("combined_data.csv empty.")
    _, n_invalid = parse_event_timestamps(combined_df)
    if n_invalid:
        raise ValueError(f"{n_invalid} bad event_timestamp.")
    return snapshot_frame_counts(*frames)


def load_coherent_snapshot(paths=None, marker_path=None, pointer_path=None,
                           processed_dir=None):
    """Load versioned coherent snapshot + RAM fallback + legacy bootstrap.

    - Versioned mode (authoritative): doc current_snapshot.json -> resolve
      snapshots/<version>/ -> load 4 CSV TRONG CUNG directory do -> validate
      vs pointer counts (marker check nghiem ngat). Lech -> giu
      last-known-good RAM + diagnostic ra terminal, KHONG warning banner,
      KHONG fallback sang legacy (tranh hide error).
    - Bootstrap (chua co pointer): dung legacy paths (paths hoac production
      data/processed/*.csv) + legacy marker neu co; validate coherent moi
      render (diagnostics terminal). Khong copy legacy mixed vao versioned.
    - RAM fallback van giu nhung correctness KHONG phu thuoc RAM: sau
      browser/Streamlit/process restart, disk pointer van cho snapshot cu
      (A) trong luc batch moi (B) dang build.
    - Tra ve dict {frames, marker, key, syncing, versioned} hoac None khi
      chua tung co snapshot usable (main hien loi ngan + stop).
    """
    key, ref = current_snapshot_key(paths, marker_path, pointer_path)
    kind = key[0] if isinstance(key, tuple) else None
    if kind == "versioned":
        resolved = resolve_versioned_paths(ref, processed_dir)
        try:
            frames = _load_snapshots_cached(key, resolved)
            counts = _validate_frames(frames)
            diag = data_update.check_versioned_coherence(counts, ref)
            if diag:
                raise ValueError("incoherent vs marker: " + "; ".join(diag))
        except Exception as exc:
            print(f"[dashboard] snapshot load/validation failed: "
                  f"{type(exc).__name__}: {exc}")
            prev = st.session_state.get(DASHBOARD_PREV_SNAPSHOT_KEY)
            if prev is not None:
                return {"frames": prev, "marker": None,
                        "key": st.session_state.get(DASHBOARD_COHERENT_KEY),
                        "syncing": True, "versioned": True}
            return None
        st.session_state[DASHBOARD_PREV_SNAPSHOT_KEY] = frames
        st.session_state[DASHBOARD_COHERENT_KEY] = key
        return {"frames": frames, "marker": ref, "key": key,
                "syncing": False, "versioned": True}
    # Legacy/bootstrap mode (khong pointer): behavior cu + validation.
    resolved = tuple(paths) if paths is not None else _resolve_snapshot_paths()
    try:
        frames = _load_snapshots_cached(key, resolved)
        counts = _validate_frames(frames)
        if ref is not None:
            diag = data_update.check_snapshot_coherence(counts, ref)
            if diag:
                raise ValueError("incoherent vs marker: " + "; ".join(diag))
        else:
            for line in _legacy_reconciliation_diagnostics(counts):
                print(f"[dashboard] snapshot diagnostic: {line}")
    except Exception as exc:
        print(f"[dashboard] snapshot load/validation failed: "
              f"{type(exc).__name__}: {exc}")
        prev = st.session_state.get(DASHBOARD_PREV_SNAPSHOT_KEY)
        if prev is not None:
            return {"frames": prev, "marker": None,
                    "key": st.session_state.get(DASHBOARD_COHERENT_KEY),
                    "syncing": True, "versioned": False}
        return None
    st.session_state[DASHBOARD_PREV_SNAPSHOT_KEY] = frames
    st.session_state[DASHBOARD_COHERENT_KEY] = key
    return {"frames": frames, "marker": ref, "key": key,
            "syncing": False, "versioned": False}


# Thu tu semantic cho filter trang thai (chi level quan sat duoc hien thi).
LEVEL_SEMANTIC_ORDER = [
    "Thông thoáng",
    "Đông",
    "Ùn tắc",
    "Ùn tắc nghiêm trọng",
]

DETAIL_TABLE_COLUMNS = [
    "Thời điểm",
    "Tuyến đường",
    "Tốc độ (km/h)",
    "Tốc độ thông thoáng (km/h)",
    "Ùn tắc (%)",
    "Trạng thái",
    "Nhiệt độ (°C)",
    "Mưa (mm)",
]


def available_filter_options(combined_df):
    """Options cho 2 filters tu du lieu (pure): roads alpha + levels hien
    co theo semantic order. Khong hard-code ten, khong bia level vang mat."""
    roads = sorted(combined_df["road_name"].dropna().astype(str).unique().tolist())
    present = set(combined_df["congestion_level"].dropna().astype(str).tolist())
    levels = [lv for lv in LEVEL_SEMANTIC_ORDER if lv in present]
    return roads, levels


def available_observation_dates(event_hanoi):
    """Distinct Hanoi calendar dates co du lieu, newest -> oldest (pure).

    Tra ve list[datetime.date]; input rong -> []. Chi ngay thuc su ton tai
    trong du lieu, khong tao ngay khong co quan sat. Khong mutate input.
    """
    dates = {d for d in event_hanoi.dt.date if pd.notna(d)}
    return sorted(dates, reverse=True)


def filter_observations_by_date(combined_df, event_hanoi, selected_date):
    """Loc rows thuoc 1 Hanoi calendar date, tra ve COPY (pure).

    selected_date la datetime.date (so theo ngay Hanoi nhu dashboard).
    Ngay khong co trong du lieu -> ValueError ro rang (khong tra empty
    am tham). Khong mutate input, khong ghi CSV.
    """
    label = (
        selected_date.strftime("%d/%m/%Y")
        if hasattr(selected_date, "strftime") else str(selected_date)
    )
    mask = event_hanoi.dt.date == selected_date
    day_df = combined_df.loc[mask.fillna(False)].copy()
    if day_df.empty:
        raise ValueError(
            f"Ngày {label} không có dữ liệu quan sát."
        )
    return day_df


def filter_detail_observations(combined_df, selected_roads, selected_levels):
    """Loc exact membership road AND level, tra ve COPY (pure).

    Empty selection -> 0 rows (khong hieu la "all"). Selection la so voi
    source -> ValueError ro rang (khong ignore ngầm). Khong mutate input.
    """
    known_roads = set(combined_df["road_name"].dropna().astype(str).tolist())
    known_levels = set(combined_df["congestion_level"].dropna().astype(str).tolist())
    bad_roads = [r for r in selected_roads if r not in known_roads]
    bad_levels = [lv for lv in selected_levels if lv not in known_levels]
    if bad_roads:
        raise ValueError(
            "Tuyến đường không có trong dữ liệu: " + ", ".join(map(str, bad_roads)) + "."
        )
    if bad_levels:
        raise ValueError(
            "Trạng thái không có trong dữ liệu: " + ", ".join(map(str, bad_levels)) + "."
        )
    mask = combined_df["road_name"].isin(selected_roads) & combined_df[
        "congestion_level"
    ].isin(selected_levels)
    return combined_df.loc[mask].copy()


def prepare_detail_table(filtered_df, event_hanoi):
    """Bang hien thi 8 cot tu filtered rows (pure, copy moi).

    Sort event_timestamp DESC + road ASC (dung instant truoc khi format).
    Gio Hanoi dd/mm/YYYY HH:MM; so 1 decimal; weather unmatched/thieu ->
    "Không có dữ liệu" (khong thanh 0, khong thanh khong mua).
    Khong mutate input, khong ghi CSV.
    """
    work = filtered_df.copy()
    work["_sort_ts"] = pd.to_datetime(event_hanoi.loc[work.index], utc=True)
    work = work.sort_values(
        ["_sort_ts", "road_name"], ascending=[False, True], kind="mergesort"
    )
    hanoi = event_hanoi.loc[work.index].dt.tz_convert(HANOI_TZ)
    matched, _ = normalize_matched(work["weather_matched"])
    rows = []
    for idx, row in work.iterrows():
        temp = _optional_number(row.get("temperature"))
        rain = _optional_number(row.get("rain"))
        if bool(matched.loc[idx]) and temp is not None and rain is not None:
            temp_txt, rain_txt = f"{temp:.1f}", f"{rain:.1f}"
        else:
            temp_txt, rain_txt = "Không có dữ liệu", "Không có dữ liệu"
        rows.append(
            {
                "Thời điểm": hanoi.loc[idx].strftime("%d/%m/%Y %H:%M"),
                "Tuyến đường": str(row["road_name"]),
                "Tốc độ (km/h)": f"{float(row['current_speed']):.1f}",
                "Tốc độ thông thoáng (km/h)": f"{float(row['free_flow_speed']):.1f}",
                "Ùn tắc (%)": f"{float(row['congestion_percent']):.1f}",
                "Trạng thái": str(row["congestion_level"]),
                "Nhiệt độ (°C)": temp_txt,
                "Mưa (mm)": rain_txt,
            }
        )
    return pd.DataFrame(rows, columns=DETAIL_TABLE_COLUMNS)


def style_dark(fig):
    """Ap dark navy styling cho 1 Plotly figure (pure layout, khong doi data).

    Dung template plotly_dark + nen trong suot de hop card dashboard.
    Tra ve chinh fig (cho phep chain).
    """
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6ebf2"},
        margin={"l": 8, "r": 8, "t": 48, "b": 8},
    )
    return fig


LATEST_TABLE_COLUMNS = [
    "Thời gian",
    "Tuyến đường",
    "Tốc độ hiện tại (km/h)",
    "Tốc độ thông thoáng (km/h)",
    "Ùn tắc (%)",
    "Trạng thái",
]


def prepare_latest_traffic_table(latest_df, event_hanoi):
    """Bang 1 observation moi nhat / tuyen (pure, copy moi).

    Sort road ASC (deterministic); gio Hanoi dd/mm/YYYY HH:MM; so 1 decimal.
    Khong mutate input, khong ghi CSV.
    """
    work = latest_df.sort_values(
        "road_name", ascending=True, kind="mergesort"
    ).copy()
    hanoi = event_hanoi.loc[work.index].dt.tz_convert(HANOI_TZ)
    rows = []
    for idx, row in work.iterrows():
        rows.append(
            {
                "Thời gian": hanoi.loc[idx].strftime("%d/%m/%Y %H:%M"),
                "Tuyến đường": str(row["road_name"]),
                "Tốc độ hiện tại (km/h)": f"{float(row['current_speed']):.1f}",
                "Tốc độ thông thoáng (km/h)": f"{float(row['free_flow_speed']):.1f}",
                "Ùn tắc (%)": f"{float(row['congestion_percent']):.1f}",
                "Trạng thái": str(row["congestion_level"]),
            }
        )
    return pd.DataFrame(rows, columns=LATEST_TABLE_COLUMNS)


def latest_matched_weather(combined_df, event_hanoi):
    """Weather tu matched observation moi nhat + dem mua (pure, descriptive).

    Tra ve dict (road_name, observed_hanoi, temperature, humidity, rain,
    wind_speed, weather_code, rain_count, matched_count) hoac None khi chua
    co observation nao ghep duoc thoi tiet. Chi mo ta du lieu, KHONG causal.
    Khong mutate input, khong ghi CSV, khong goi API.
    """
    mask, _ = normalize_matched(combined_df["weather_matched"])
    matched = combined_df.loc[mask].copy()
    if matched.empty:
        return None
    instants = pd.to_datetime(event_hanoi.loc[matched.index], utc=True)
    row = matched.loc[instants.idxmax()]
    observed = event_hanoi.loc[instants.idxmax()].tz_convert(HANOI_TZ)
    rain_n = int((pd.to_numeric(matched["rain"], errors="coerce") > 0).sum())
    code = row.get("weather_code")
    return {
        "road_name": str(row["road_name"]),
        "observed_hanoi": observed,
        "temperature": _optional_number(row.get("temperature")),
        "humidity": _optional_number(row.get("humidity")),
        "rain": _optional_number(row.get("rain")),
        "wind_speed": _optional_number(row.get("wind_speed")),
        "weather_code": None if pd.isna(code) else int(code),
        "rain_count": rain_n,
        "matched_count": len(matched),
    }


def _require_chart_frame(df, label, numeric_cols, int_cols=()):
    """Validate chart source: numeric non-null, obs>0, hour 0..23 (pure).

    Tra ve ban copy (khong mutate input). Vi pham -> ValueError ro rang
    (khong drop ngầm). Khong ap [0,100] cho congestion (Phase 5 unclamped).
    """
    work = df.copy()
    for col in numeric_cols:
        parsed = pd.to_numeric(work[col], errors="coerce")
        if int(parsed.isna().sum()):
            raise ValueError(
                f"File {label}: cột {col} có giá trị không phải số/trống — "
                "kiểm tra lại Phase 6."
            )
        work[col] = parsed
    for col in int_cols:
        if col == "observation_count" and bool((work[col] <= 0).any()):
            raise ValueError(f"File {label}: observation_count phải > 0.")
        if col == "local_hour" and bool(
            (work[col] != work[col].astype(int)).any()
            or (work[col] < 0).any() or (work[col] > 23).any()
        ):
            raise ValueError(f"File {label}: local_hour phải là giờ nguyên 0..23.")
    return work


def prepare_road_chart_data(road_df):
    """Ban copy sap xep cho 2 road charts (pure, khong mutate input).

    Tra ve (speed_df, congestion_df): avg_speed DESC/road ASC va
    avg_congestion DESC/road ASC, kem cot label hien thi 1 decimal
    (gia tri goc giu nguyen).
    """
    if road_df.empty:
        raise ValueError("File road_summary.csv hiện trống.")
    if bool(road_df["road_name"].isna().any()) or bool(
        (road_df["road_name"].astype(str).str.strip() == "").any()
    ):
        raise ValueError("File road_summary.csv có road_name trống.")
    work = _require_chart_numeric_only(
        road_df,
        "road_summary.csv",
        ["avg_speed", "avg_congestion_percent"],
    )
    if bool((work["avg_speed"] < 0).any()):
        raise ValueError("File road_summary.csv: avg_speed phải >= 0.")
    speed_df = work.sort_values(
        ["avg_speed", "road_name"], ascending=[False, True], kind="mergesort"
    ).copy()
    speed_df["avg_speed_label"] = speed_df["avg_speed"].map(lambda v: f"{v:.1f}")
    congestion_df = work.sort_values(
        ["avg_congestion_percent", "road_name"], ascending=[False, True], kind="mergesort"
    ).copy()
    congestion_df["avg_congestion_label"] = congestion_df["avg_congestion_percent"].map(
        lambda v: f"{v:.1f}%"
    )
    return speed_df, congestion_df


def _require_chart_numeric_only(df, label, numeric_cols):
    """Validate numeric + observation_count>0 tren ban copy (pure)."""
    work = _require_chart_frame(df, label, numeric_cols)
    obs = pd.to_numeric(work["observation_count"], errors="coerce")
    if int(obs.isna().sum()) or bool((obs <= 0).any()):
        raise ValueError(f"File {label}: observation_count phải là số nguyên > 0.")
    work["observation_count"] = obs.astype(int)
    return work


def prepare_hourly_chart_data(hourly_df):
    """Ban copy sap local_hour ASC cho hourly chart (pure, khong mutate).

    Khong tao gio 0-observation. time_window lay nguyen tu summary.
    """
    if hourly_df.empty:
        raise ValueError("File hourly_summary.csv hiện trống.")
    work = _require_chart_frame(
        hourly_df,
        "hourly_summary.csv",
        ["avg_speed", "avg_congestion_percent"],
        int_cols=("observation_count", "local_hour"),
    )
    if bool(work["time_window"].isna().any()) or bool(
        (work["time_window"].astype(str).str.strip() == "").any()
    ):
        raise ValueError("File hourly_summary.csv có time_window trống.")
    if bool((work["avg_speed"] < 0).any()):
        raise ValueError("File hourly_summary.csv: avg_speed phải >= 0.")
    work["local_hour"] = work["local_hour"].astype(int)
    out = work.sort_values("local_hour", ascending=True, kind="mergesort").copy()
    out["avg_congestion_label"] = out["avg_congestion_percent"].map(lambda v: f"{v:.1f}%")
    return out


def prepare_weather_chart_data(weather_df):
    """Ban copy order semantic Khong mua -> Mua (pure, khong mutate).

    Chi condition quan sat duoc; khong tao condition thieu.
    """
    if weather_df.empty:
        raise ValueError("File weather_summary.csv hiện trống.")
    allowed = ["Không mưa", "Mưa"]
    if bool(weather_df["weather_condition"].isna().any()) or bool(
        (~weather_df["weather_condition"].isin(allowed)).any()
    ):
        raise ValueError(
            "File weather_summary.csv có weather_condition lạ "
            "(chỉ chấp nhận Không mưa/Mưa)."
        )
    work = _require_chart_numeric_only(
        weather_df, "weather_summary.csv", ["avg_speed", "avg_congestion_percent"]
    )
    if bool((work["avg_speed"] < 0).any()):
        raise ValueError("File weather_summary.csv: avg_speed phải >= 0.")
    work["_worder"] = work["weather_condition"].map(
        lambda c: allowed.index(c)
    )
    out = work.sort_values("_worder", ascending=True, kind="mergesort").copy()
    out["avg_speed_label"] = out["avg_speed"].map(lambda v: f"{v:.1f}")
    out["avg_congestion_label"] = out["avg_congestion_percent"].map(lambda v: f"{v:.1f}%")
    return out.drop(columns=["_worder"])


def get_latest_road_observations(combined_df, event_utc):
    """Chon dung 1 quan sat moi nhat cho moi road (pure, testable).

    Sap xep theo event instant (UTC) ASC, tie-break deterministic theo
    (road_name, timestamp, lat, lon) — khong dua vao input order.
    NaT (neu co) xep truoc nen khong bao gio duoc chon lam moi nhat.
    Tra ve DataFrame giu nguyen index goc (de tra cuu gio Hanoi).
    """
    work = combined_df.copy()
    work["_event_instant"] = pd.to_datetime(event_utc, utc=True)
    work = work.sort_values(
        ["_event_instant", "road_name", "timestamp", "lat", "lon"],
        ascending=True,
        na_position="first",
        kind="mergesort",
    )
    latest = work.groupby("road_name", sort=False).tail(1)
    return latest.drop(columns=["_event_instant"])


def validate_map_coordinates(latest_df):
    """Validate lat/lon; tra ve (valid_df, invalid_road_names) (pure).

    Toa do la/NULL/ngoai bien -> loai marker do (khong dat o 0,0),
    caller tu canh bao ro ten tuyen.
    """
    lat = pd.to_numeric(latest_df["lat"], errors="coerce")
    lon = pd.to_numeric(latest_df["lon"], errors="coerce")
    ok = (
        lat.notna()
        & lon.notna()
        & lat.between(-90, 90)
        & lon.between(-180, 180)
    )
    invalid_roads = sorted(latest_df.loc[~ok, "road_name"].astype(str).unique().tolist())
    return latest_df[ok], invalid_roads


def validate_congestion_levels(latest_df):
    """Tra ve list congestion_level la/unknown (pure, rong nghia la OK)."""
    vals = latest_df["congestion_level"]
    bad = vals[vals.isna() | ~vals.isin(list(CONGESTION_COLORS))]
    out = []
    if bool(vals.isna().any()):
        out.append("(null)")
    out.extend(sorted({str(v) for v in bad.dropna().tolist()}))
    return out


def congestion_color(level):
    """Mau Folium marker cho congestion_level (pure; la -> KeyError)."""
    return CONGESTION_COLORS[level]


def _optional_number(value):
    """Float hoac None neu thieu/invalid (pure)."""
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(number) else float(number)


def build_popup_html(road_name, current_speed, free_flow_speed,
                     congestion_percent, congestion_level, observed_hanoi,
                     weather_matched, temperature, rain):
    """HTML popup cho 1 marker (pure). Mọi text đều html.escape.

    weather_matched True + temperature/rain hop le -> hien thoi tiet;
    nguoc lai hien "Thời tiết: Không có dữ liệu ghép" (unmatched khong
    bao gio thanh "khong mua").
    """
    import html as _html

    temp = _optional_number(temperature)
    rain_v = _optional_number(rain)
    if weather_matched is True and temp is not None and rain_v is not None:
        weather_line = (
            f"Nhiệt độ: {temp:.1f} °C<br>Mưa: {rain_v:.1f} mm"
        )
    else:
        weather_line = "Thời tiết: Không có dữ liệu ghép"
    return (
        f"<b>{_html.escape(str(road_name))}</b><br>"
        f"Tên tuyến: {_html.escape(str(road_name))}<br>"
        f"Tốc độ hiện tại: {float(current_speed):.1f} km/h<br>"
        f"Tốc độ thông thoáng: {float(free_flow_speed):.1f} km/h<br>"
        f"Mức ùn tắc: {float(congestion_percent):.1f}%<br>"
        f"Trạng thái: {_html.escape(str(congestion_level))}<br>"
        f"Thời điểm quan sát: {_html.escape(str(observed_hanoi))}<br>"
        f"{weather_line}"
    )


def build_traffic_map(latest_df, event_hanoi):
    """Dung folium.Map tu latest rows da validate (pure construction).

    Center = mean lat/lon cua markers (khong hard-code city center);
    zoom 12 cho cum Ha Noi; tile mac dinh (khong API key). Tra ve
    (folium.Map, marker_count, (center_lat, center_lon)).
    """
    matched, _ = normalize_matched(latest_df["weather_matched"])
    center = (float(latest_df["lat"].mean()), float(latest_df["lon"].mean()))
    traffic_map = folium.Map(location=[center[0], center[1]], zoom_start=12)
    count = 0
    for idx, row in latest_df.iterrows():
        observed = event_hanoi.loc[idx].strftime("%d/%m/%Y %H:%M")
        popup = build_popup_html(
            row["road_name"], row["current_speed"], row["free_flow_speed"],
            row["congestion_percent"], row["congestion_level"], observed,
            bool(matched.loc[idx]), row.get("temperature"), row.get("rain"),
        )
        folium.Marker(
            location=[float(row["lat"]), float(row["lon"])],
            tooltip=str(row["road_name"]),
            popup=folium.Popup(popup, max_width=280),
            icon=folium.Icon(color=congestion_color(row["congestion_level"])),
        ).add_to(traffic_map)
        count += 1
    legend = MacroElement()
    legend._template = Template(
        """{% macro html(this, kwargs) %}
<div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999;
            background: white; padding: 8px 10px; font-size: 12px;
            border: 1px solid #ccc; border-radius: 4px;">
<b>Mức ùn tắc</b><br>
<span style="color: green;">&#9679;</span> Thông thoáng<br>
<span style="color: orange;">&#9679;</span> Đông<br>
<span style="color: red;">&#9679;</span> Ùn tắc<br>
<span style="color: darkred;">&#9679;</span> Ùn tắc nghiêm trọng
</div>
{% endmacro %}"""
    )
    traffic_map.get_root().add_child(legend)
    return traffic_map, count, center


ROAD_FLASH_KEY = "road_flash"


def _set_road_flash(kind, message):
    """Luu flash message de hien sau rerun (session_state toi thieu)."""
    st.session_state[ROAD_FLASH_KEY] = (kind, message)


def _show_road_flash():
    """Hien flash message trong sidebar 1 lan roi xoa (pure UI)."""
    flash = st.session_state.pop(ROAD_FLASH_KEY, None)
    if flash is None:
        return
    kind, message = flash
    if kind == "success":
        st.sidebar.success(message)
    else:
        st.sidebar.error(message)


def available_catalog_roads(catalog_entries, monitored_names):
    """Ten catalog CHUA duoc theo doi, giu nguyen thu tu catalog (pure).

    So sanh trim + case-insensitive (tuong thich road_manager duplicate
    protection). Dung cho dropdown Add (khong hien tuyen da co).
    """
    monitored = {str(n).strip().lower() for n in (monitored_names or [])}
    return [
        str(e["road_name"])
        for e in (catalog_entries or [])
        if str(e["road_name"]).strip().lower() not in monitored
    ]


def _handle_add_road(selected_name):
    """Xu ly submit form them tuyen tu catalog dropdown.

    Lay (road_name, lat, lon) tu catalog roi goi road_manager.add_road()
    (khong open/write CSV truc tiep, khong nhap tay toa do).
    road_manager van la lop cuoi validation/duplicate/atomic write.
    """
    entry = find_catalog_entry(selected_name)
    if entry is None:
        _set_road_flash(
            "error", "Tuyến đã chọn không có trong danh mục."
        )
        st.rerun()
    try:
        road_manager.add_road(entry["road_name"], entry["lat"], entry["lon"])
    except road_manager.DuplicateRoadError:
        _set_road_flash("error", "Tuyến này đã có trong danh sách theo dõi.")
        st.rerun()
    except road_manager.ValidationError:
        _set_road_flash(
            "error", "Dữ liệu danh mục không hợp lệ, không thể thêm tuyến."
        )
        st.rerun()
    except road_manager.RoadManagerError as exc:
        _set_road_flash("error", f"Không thêm được tuyến: {exc}")
        st.rerun()
    st.session_state.pop("add_road_select", None)
    _set_road_flash(
        "success",
        f"Đã thêm {entry['road_name']} vào danh sách theo dõi. "
        "Tuyến sẽ có dữ liệu trên dashboard sau lần cập nhật dữ liệu "
        "tiếp theo.",
    )
    st.rerun()


def _handle_remove_road(target):
    """Xu ly xoa tuyen: chi goi road_manager (khong cascade delete).

    Chi thay doi roads.csv; traffic/weather/processed giu nguyen.
    Reset widget keys truoc rerun de selectbox khong giu option da xoa.
    """
    try:
        road_manager.remove_road(target)
    except road_manager.LastRoadError:
        _set_road_flash(
            "error",
            "Không thể xóa tuyến cuối cùng. Hệ thống cần ít nhất một "
            "tuyến được theo dõi.",
        )
        st.rerun()
    except road_manager.RoadManagerError as exc:
        _set_road_flash("error", f"Không xóa được tuyến: {exc}")
        st.rerun()
    for key in ("remove_road_select", "remove_road_confirm"):
        st.session_state.pop(key, None)
    _set_road_flash(
        "success",
        f"Đã xóa tuyến {target} khỏi danh sách theo dõi. "
        "Dữ liệu lịch sử đã thu thập không bị xóa.",
    )
    st.rerun()


def render_road_management_sidebar(analyzed_road_names):
    """Section sidebar Quan ly tuyen duong (Phase 8.2).

    - Danh sach doc TRUC TIEP tu data/roads.csv qua
      road_manager.load_roads() (khong dung road_summary.csv).
    - Add/Remove goi road_manager; khong goi collector/API/Spark;
      khong sua processed data; khong doi KPI/map/charts.
    - analyzed_road_names: set ten tuyen da co trong processed combined
      (chi de hien coverage nhe, khong thay 6 KPI).
    """
    st.sidebar.markdown("---")
    st.sidebar.header("🛣️ Quản lý tuyến đường")

    _show_road_flash()

    try:
        monitored = road_manager.load_roads()
    except road_manager.RoadManagerError as exc:
        st.sidebar.error(
            "Không đọc được danh sách tuyến đang theo dõi "
            f"(data/roads.csv): {exc}"
        )
        return

    monitored_names = [r["road_name"] for r in monitored]
    st.sidebar.markdown(
        f"""
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🛣️</span>Đang theo dõi: {len(monitored_names)} tuyến</div>
        """,
        unsafe_allow_html=True,
    )
    if monitored_names:
        st.sidebar.write(", ".join(monitored_names))

    with st.sidebar.expander("➕ Thêm tuyến"):
        available = available_catalog_roads(HANOI_ROAD_CATALOG, monitored_names)
        if not available:
            st.info("Tất cả tuyến trong danh mục hiện đã được theo dõi.")
        else:
            st.caption("Tọa độ được hệ thống thiết lập tự động.")
            with st.form("add_road_form", clear_on_submit=True):
                selected = st.selectbox(
                    "Chọn tuyến", options=available, key="add_road_select"
                )
                add_submitted = st.form_submit_button("Thêm tuyến")
            if add_submitted:
                _handle_add_road(selected)

    with st.sidebar.expander("🗑️ Xóa tuyến"):
        if not monitored_names:
            st.info("Chưa có tuyến nào được theo dõi.")
        else:
            target = st.selectbox(
                "Chọn tuyến cần xóa",
                options=monitored_names,
                key="remove_road_select",
            )
            confirm = st.checkbox(
                "Tôi xác nhận muốn xóa tuyến này khỏi danh sách theo dõi.",
                key="remove_road_confirm",
            )
            last_only = len(monitored_names) <= 1
            if last_only:
                st.info(
                    "Chỉ còn 1 tuyến được theo dõi — không thể xóa tuyến "
                    "cuối cùng."
                )
            if st.button(
                "Xóa tuyến",
                disabled=(not confirm or last_only),
                key="remove_road_button",
            ):
                _handle_remove_road(target)


UPDATE_FLASH_KEY = "update_flash"


def _set_update_flash(kind, message):
    """Luu update flash de hien sau rerun (tách biệt road_flash)."""
    st.session_state[UPDATE_FLASH_KEY] = (kind, message)


def _show_update_flash():
    """Hien update flash trong sidebar 1 lan roi xoa (pure UI)."""
    flash = st.session_state.pop(UPDATE_FLASH_KEY, None)
    if flash is None:
        return
    kind, message = flash
    if kind == "success":
        st.sidebar.success(message)
    else:
        st.sidebar.warning(message)


def _sanitize_update_error(message):
    """Redact key=... khoi error text truoc khi hien thi (khong lo secret)."""
    import re as _re

    return _re.sub(r"(key=)[^&\s'\"]+", r"\1***", str(message or ""))


def _format_update_summary(result):
    """Chuoi summary ngan tu structured result (pure, so lieu that)."""
    parts = [f"{result.get('roads_loaded', 0)} tuyến"]
    parts.append(
        f"Traffic: {result.get('traffic_collected', 0)} thành công, "
        f"{result.get('traffic_failed', 0)} lỗi"
    )
    parts.append(
        f"Weather: {result.get('weather_collected', 0)} thành công, "
        f"{result.get('weather_failed', 0)} lỗi"
    )
    parts.append(
        "Spark: hoàn thành" if result.get("spark_success") else "Spark: chưa hoàn tất"
    )
    try:
        parts.append(f"Thời gian: {float(result.get('duration_seconds')):.1f}s")
    except (TypeError, ValueError):
        pass
    return " • ".join(parts)


def _handle_data_update():
    """Chay pipeline DUNG 1 LAN khi user bam nut (khong auto-run/retry).

    Gọi data_update.run_data_update() trong spinner; backend tu xu ly
    lock/partial/failure. Success/partial -> flash + rerun de dashboard
    doc snapshot moi; failure/already-running -> thong bao inline,
    khong rerun.
    """
    with st.spinner("Đang thu thập dữ liệu và chạy Apache Spark..."):
        result = data_update.run_data_update()
    error = str(result.get("error") or "")
    if not result.get("success", False) and "already running" in error.lower():
        st.warning("Một lần cập nhật khác đang chạy. Vui lòng chờ hoàn tất.")
        return
    if result.get("success", False):
        partial = (result.get("traffic_failed", 0) > 0) or (
            result.get("weather_failed", 0) > 0
        )
        if partial:
            _set_update_flash(
                "warning",
                "Cập nhật hoàn tất nhưng một số tuyến không thu thập được "
                "dữ liệu. " + _format_update_summary(result),
            )
        else:
            _set_update_flash(
                "success",
                "Đã cập nhật dữ liệu thành công. "
                + _format_update_summary(result),
            )
        st.rerun()
    st.error(
        "Cập nhật dữ liệu thất bại: "
        + (_sanitize_update_error(error) or "lỗi không xác định.")
    )
    st.caption(
        f"Đã lưu: traffic {result.get('traffic_saved', 0)}, "
        f"weather {result.get('weather_saved', 0)}. "
        f"Spark: {'hoàn thành' if result.get('spark_success') else 'chưa hoàn tất'}. "
        "Dữ liệu thô mới (nếu có) vẫn được giữ, không rollback."
    )


def render_data_update_header():
    """Nut cap nhat du lieu o header (backend Phase 9.2 giu nguyen).

    Chi goi orchestrator public run_data_update() qua _handle_data_update();
    khong goi truc tiep TomTom/Open-Meteo/collector/Spark. Pipeline chi chay
    khi user bam nut (khong auto-run).
    """
    _show_update_flash()
    if st.button(
        "🔄 Cập nhật dữ liệu",
        key="update_data_button",
        use_container_width=True,
        type="secondary",
    ):
        _handle_data_update()


def main():
    st.set_page_config(
        page_title="Hanoi Traffic Analytics",
        page_icon="🚗",
        layout="wide",
    )

    inject_css()

    # ---- Near-realtime auto-refresh tick (read-only, khong API/Spark) ----
    # Fragment tu chay moi 30s; chi kich full rerun khi snapshot key doi.
    _auto_refresh_tick()

    # ---- Versioned snapshot guard (authoritative) + legacy bootstrap ----
    # Neu co versioned pointer hop le: khong yeu cau legacy files (dashboard
    # chi doc snapshots/<version>/). Neu chua co pointer: giu guard cu de bao
    # thieu file ro rang truoc khi thu legacy coherent load.
    try:
        _bootstrap_pointer = data_update.read_current_snapshot(CURRENT_SNAPSHOT_JSON)
    except Exception:
        _bootstrap_pointer = None
    if not isinstance(_bootstrap_pointer, dict):
        missing = find_missing_files()
        if missing:
            st.error(
                "Thiếu file dữ liệu đã xử lý: " + ", ".join(missing) + ". "
                "Hãy chạy xử lý Phase 5/6 trước khi mở dashboard."
            )
            st.stop()

    # ---- Coherent snapshot load (versioned-gated, last-known-good fallback) ----
    # Chi render snapshot coherent trong cung version dir; transient build (B)
    # chua publish thi pointer van A -> van render A, khong mixed-version.
    # Diagnostic ra terminal (khong warning banner len UI).
    snapshot = load_coherent_snapshot()
    if snapshot is None:
        st.error("Chưa có dữ liệu hiển thị. Vui lòng thử lại sau ít phút.")
        st.stop()
    combined_df, road_df, hourly_df, weather_df = snapshot["frames"]
    if snapshot.get("syncing"):
        st.caption("Đang đồng bộ dữ liệu mới...")

    # ---- Timestamp presentation (copy in-memory, khong sua CSV) ----
    event_utc, _ = parse_event_timestamps(combined_df)
    event_hanoi = to_hanoi(event_utc)
    latest_hanoi = event_hanoi.max()

    # ---- Sidebar: control panel (status + compact stats + road mgmt) ----
    # KPI luon global; cac filter ngay/tuyen/level chi nam o section
    # Phan tich chi tiet phia duoi.
    st.sidebar.markdown("### 🚗 Traffic Analytics")
    st.sidebar.markdown(
        '<div class="sidebar-status"><span class="status-dot">●</span>'
        "Hệ thống hoạt động</div>",
        unsafe_allow_html=True,
    )
    n_obs = len(combined_df)
    n_roads = int(road_df["road_name"].nunique()) if not road_df.empty else 0
    st.sidebar.markdown(
        f"""
        <div class="sidebar-metric"><span class="sidebar-metric-icon">📊</span>{n_obs} Quan sát</div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🛣️</span>{n_roads} Tuyến</div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🕐</span>{latest_hanoi.strftime('%H:%M')} Cập nhật</div>
        """,
        unsafe_allow_html=True,
    )

    # ---- Sidebar: quan ly tuyen duong (roads.csv, khong cham processed) ----
    render_road_management_sidebar(
        set(combined_df["road_name"].dropna().astype(str).tolist())
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("TomTom · Open-Meteo")

    # ---- Metrics global tu snapshot (validation giu o backend) ----
    # Reconciliation diagnostics chi ra terminal, KHONG warning banner len UI.
    try:
        metrics = calculate_overview_metrics(
            combined_df, road_df, hourly_df, weather_df, event_hanoi
        )
    except ValueError as exc:
        print(f"[dashboard] snapshot diagnostic: metrics failed: {exc}")
        st.error("Chưa có dữ liệu hiển thị. Vui lòng thử lại sau ít phút.")
        st.stop()
    for warning in metrics["warnings"]:
        print(f"[dashboard] snapshot diagnostic: {warning}")

    # ---- Hero header (single title) | live status + update ----
    hdr_left, hdr_right = st.columns([3, 1])
    with hdr_left:
        st.markdown(
            '<div class="dash-title">🚦 Hanoi Traffic Analytics</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="dash-subtitle">Theo dõi giao thông và thời tiết '
            "tại Hà Nội</div>",
            unsafe_allow_html=True,
        )
    with hdr_right:
        st.markdown(
            '<div class="status-line"><span class="status-dot">●</span> '
            "ĐANG THEO DÕI</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="header-meta">Cập nhật: '
            f"<b>{latest_hanoi.strftime('%H:%M')}</b></div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="header-meta">Tự động · {AUTO_REFRESH_SECONDS}s</div>',
            unsafe_allow_html=True,
        )
        render_data_update_header()

    # ---- 4 KPI cards global (khong chiu anh huong detail filters) ----
    kpi_cards = [
        (KPI_ICONS["n_roads"], "TUYẾN ĐƯỜNG", str(metrics['n_roads'])),
        (KPI_ICONS["avg_speed"], "TỐC ĐỘ TRUNG BÌNH", f"{metrics['avg_speed']:.1f} km/h"),
        (KPI_ICONS["n_obs"], "QUAN SÁT", str(metrics['n_obs'])),
        (KPI_ICONS["avg_congestion"], "ÙN TẮC TRUNG BÌNH", f"{metrics['avg_congestion']:.1f}%"),
    ]
    kpi_cols = st.columns(4)
    for i, (icon, label, value) in enumerate(kpi_cards):
        with kpi_cols[i]:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label"><span class="kpi-icon">{icon}</span>{label}</div>
                    <div class="kpi-value">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ---- Du lieu dung chung cho grid (prepare truoc, fail-fast) ----
    latest_df = get_latest_road_observations(combined_df, event_utc)
    valid_df, invalid_roads = validate_map_coordinates(latest_df)
    try:
        speed_df, congestion_df = prepare_road_chart_data(road_df)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    try:
        hourly_chart_df = prepare_hourly_chart_data(hourly_df)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    # ---- Main grid: map 60% | charts 40% ----
    grid_map, grid_charts = st.columns([3, 2])
    with grid_map:
        st.markdown('<div class="section-header"><span class="section-icon">🗺️</span><h2>Bản đồ tình hình giao thông Hà Nội</h2></div>', unsafe_allow_html=True)
        if invalid_roads:
            st.warning(
                "Bỏ qua marker thiếu/sai tọa độ của tuyến: "
                + ", ".join(invalid_roads)
                + " (không đặt ở 0,0)."
            )
        bad_levels = validate_congestion_levels(valid_df)
        if bad_levels:
            st.error(
                "Trạng thái ùn tắc lạ trên quan sát mới nhất (không thể tô màu): "
                + ", ".join(bad_levels)
                + ". Kiểm tra lại Phase 5."
            )
            valid_df = valid_df[
                valid_df["congestion_level"].isin(list(CONGESTION_COLORS))
            ]
        if valid_df.empty:
            st.error("Không còn marker hợp lệ nào để vẽ bản đồ.")
        else:
            traffic_map, n_markers, _ = build_traffic_map(valid_df, event_hanoi)
            st_folium(traffic_map, use_container_width=True, height=650, returned_objects=[])
            st.markdown(
                " • ".join(
                    f"{STATUS_EMOJI.get(lv, '⚪')} {lv}"
                    for lv in CONGESTION_COLORS
                )
            )
    with grid_charts:
        st.markdown('<div class="section-header"><span class="section-icon">📈</span><h2>Tốc độ trung bình theo thời gian</h2></div>', unsafe_allow_html=True)
        fig_hspeed = go.Figure(
            go.Scatter(
                x=hourly_chart_df["local_hour"],
                y=hourly_chart_df["avg_speed"],
                mode="lines+markers",
                marker={"size": 8},
                customdata=hourly_chart_df[
                    ["time_window", "avg_congestion_percent", "observation_count"]
                ].to_numpy(),
                hovertemplate=(
                    "Giờ: %{x}<br>Khung giờ: %{customdata[0]}<br>"
                    "Tốc độ TB: %{y:.1f} km/h<br>Ùn tắc TB: %{customdata[1]:.1f}%<br>"
                    "Quan sát: %{customdata[2]}<extra></extra>"
                ),
            )
        )
        fig_hspeed.update_layout(
            title="Tốc độ trung bình theo thời gian",
            xaxis_title="Giờ trong ngày",
            yaxis_title="Tốc độ trung bình (km/h)",
            xaxis={"dtick": 1},
        )
        style_dark(fig_hspeed)
        st.plotly_chart(fig_hspeed, width="stretch")
        st.markdown('<div class="section-header"><span class="section-icon">📊</span><h2>Mức độ ùn tắc theo tuyến đường</h2></div>', unsafe_allow_html=True)
        fig_cong = go.Figure(
            go.Bar(
                x=congestion_df["avg_congestion_percent"],
                y=congestion_df["road_name"],
                orientation="h",
                text=congestion_df["avg_congestion_label"],
                textposition="outside",
                marker_color="#c96a1b",
                customdata=congestion_df[["observation_count"]].to_numpy(),
                hovertemplate=(
                    "Tuyến: %{y}<br>Ùn tắc TB: %{x:.1f}%<br>"
                    "Quan sát: %{customdata[0]}<extra></extra>"
                ),
            )
        )
        fig_cong.update_layout(
            title="Mức ùn tắc trung bình theo tuyến",
            xaxis_title="Mức ùn tắc trung bình (%)",
            yaxis_title="",
            yaxis={
                "categoryorder": "array",
                "categoryarray": congestion_df["road_name"].tolist(),
            },
        )
        style_dark(fig_cong)
        st.plotly_chart(fig_cong, width="stretch")

    # ---- Second row: latest traffic 65% | weather 35% ----
    row2_left, row2_right = st.columns([2, 1])
    with row2_left:
        st.markdown('<div class="section-header"><span class="section-icon">📋</span><h2>Giao thông mới nhất</h2></div>', unsafe_allow_html=True)
        latest_table_df = prepare_latest_traffic_table(latest_df, event_hanoi)
        st.dataframe(latest_table_df, hide_index=True, width="stretch")
    with row2_right:
        st.markdown('<div class="section-header"><span class="section-icon">☁️</span><h2>Thời tiết hiện tại</h2></div>', unsafe_allow_html=True)
        wx = latest_matched_weather(combined_df, event_hanoi)
        if wx is None:
            st.write("Chưa có quan sát nào ghép được dữ liệu thời tiết.")
        else:
            temp_txt = f"{wx['temperature']:.1f} °C" if wx["temperature"] is not None else "Không có dữ liệu"
            hum_txt = f"{wx['humidity']:.1f} %" if wx["humidity"] is not None else "Không có dữ liệu"
            rain_txt = f"{wx['rain']:.1f} mm" if wx["rain"] is not None else "Không có dữ liệu"
            wind_txt = f"{wx['wind_speed']:.1f} m/s" if wx["wind_speed"] is not None else "Không có dữ liệu"
            code_txt = str(wx["weather_code"]) if wx["weather_code"] is not None else "—"
            st.markdown(
                f"""
                <div class="card">
                    <div class="wx-label">Nhiệt độ</div>
                    <div class="wx-value">{temp_txt}</div>
                    <div class="wx-label">Độ ẩm</div>
                    <div class="wx-value">{hum_txt}</div>
                    <div class="wx-label">Lượng mưa</div>
                    <div class="wx-value">{rain_txt}</div>
                    <div class="wx-label">Tốc độ gió</div>
                    <div class="wx-value">{wind_txt}</div>
                    <div class="wx-label">Mã thời tiết</div>
                    <div class="wx-value">{code_txt}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.caption(
                f"{wx['road_name']} · "
                f"{wx['observed_hanoi'].strftime('%d/%m/%Y %H:%M')}"
            )

    st.markdown('<div class="section-header"><span class="section-icon">📋</span><h2>Phân tích chi tiết</h2></div>', unsafe_allow_html=True)
    date_options = available_observation_dates(event_hanoi)
    selected_date = st.selectbox(
        "Ngày xem",
        options=date_options,
        index=0,
        format_func=lambda d: d.strftime("%d/%m/%Y"),
        key="detail_date",
    )
    roads_options, levels_options = available_filter_options(combined_df)
    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_roads = st.multiselect(
            "Tuyến đường", options=roads_options, default=roads_options,
            key="detail_roads",
        )
    with filter_cols[1]:
        selected_levels = st.multiselect(
            "Trạng thái giao thông", options=levels_options, default=levels_options,
            key="detail_levels",
        )
    try:
        day_df = filter_observations_by_date(
            combined_df, event_hanoi, selected_date
        )
        # Road/status loc tren FULL snapshot (giu validation hien tai);
        # giao voi ngay da chon (AND giao hoan: ket qua giong loc
        # date -> roads -> levels; tuyen hop le nhung vang mat trong ngay
        # cho empty-state than thien thay vi ValueError).
        base_df = filter_detail_observations(
            combined_df, selected_roads, selected_levels
        )
        filtered_df = base_df.loc[base_df.index.isin(day_df.index)].copy()
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    date_label = selected_date.strftime("%d/%m/%Y")
    st.write(
        f"Hiển thị {len(filtered_df)} / {len(day_df)} "
        f"quan sát trong ngày {date_label}"
    )
    if filtered_df.empty:
        st.write("Không có quan sát phù hợp với bộ lọc trong ngày đã chọn.")
    else:
        detail_df = prepare_detail_table(filtered_df, event_hanoi)
        st.dataframe(detail_df, hide_index=True, width="stretch")

    # ---- Phan tich bo sung (expander: giu functionality Phase 7, UI gon) ----
    with st.expander("📊 Phân tích bổ sung", expanded=False):
        if metrics["top_road"] is not None:
            st.markdown(
                f"""
                <div class="insight-card">
                    <div class="insight-title">Tuyến ùn tắc nhất</div>
                    <div class="insight-content">
                        Tuyến có mức ùn tắc trung bình cao nhất trong dữ liệu đã thu thập:
                        {metrics['top_road']['road_name']}
                        ({metrics['top_road']['avg_congestion_percent']:.1f}%).
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        weather_counts = metrics["weather_counts"]
        if "Không mưa" in weather_counts and "Mưa" in weather_counts:
            st.markdown(
                f"""
                <div class="insight-card">
                    <div class="insight-title">Điều kiện thời tiết</div>
                    <div class="insight-content">
                        {weather_counts['Không mưa']} quan sát không mưa,
                        {weather_counts['Mưa']} quan sát mưa.
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif weather_counts:
            only_name, only_count = next(iter(weather_counts.items()))
            st.markdown(
                f"""
                <div class="insight-card">
                    <div class="insight-title">Điều kiện thời tiết</div>
                    <div class="insight-content">
                        {only_count} quan sát {only_name.lower()}.
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        fig_hourly = go.Figure(
            go.Scatter(
                x=hourly_chart_df["local_hour"],
                y=hourly_chart_df["avg_congestion_percent"],
                mode="lines+markers",
                marker={"size": 9},
                text=hourly_chart_df["avg_congestion_label"],
                textposition="top center",
                customdata=hourly_chart_df[
                    ["time_window", "avg_speed", "observation_count"]
                ].to_numpy(),
                hovertemplate=(
                    "Giờ: %{x}<br>Khung giờ: %{customdata[0]}<br>"
                    "Ùn tắc TB: %{y:.1f}%<br>Tốc độ TB: %{customdata[1]:.1f} km/h<br>"
                    "Quan sát: %{customdata[2]}<extra></extra>"
                ),
            )
        )
        fig_hourly.update_layout(
            title="Mức ùn tắc trung bình theo giờ",
            xaxis_title="Giờ trong ngày",
            yaxis_title="Mức ùn tắc trung bình (%)",
            xaxis={"dtick": 1},
        )
        style_dark(fig_hourly)
        st.plotly_chart(fig_hourly, width="stretch")
        fig_speed = go.Figure(
            go.Bar(
                x=speed_df["avg_speed"],
                y=speed_df["road_name"],
                orientation="h",
                text=speed_df["avg_speed_label"],
                textposition="outside",
                marker_color="#1f77b4",
                customdata=speed_df[["observation_count"]].to_numpy(),
                hovertemplate=(
                    "Tuyến: %{y}<br>Tốc độ TB: %{x:.1f} km/h<br>"
                    "Quan sát: %{customdata[0]}<extra></extra>"
                ),
            )
        )
        fig_speed.update_layout(
            title="Tốc độ trung bình theo tuyến",
            xaxis_title="Tốc độ trung bình (km/h)",
            yaxis_title="",
            yaxis={
                "categoryorder": "array",
                "categoryarray": speed_df["road_name"].tolist(),
            },
        )
        style_dark(fig_speed)
        st.plotly_chart(fig_speed, width="stretch")
        try:
            weather_chart_df = prepare_weather_chart_data(weather_df)
        except ValueError as exc:
            st.error(str(exc))
            st.stop()
        weather_cols = st.columns(2)
        with weather_cols[0]:
            fig_wspeed = go.Figure(
                go.Bar(
                    x=weather_chart_df["weather_condition"],
                    y=weather_chart_df["avg_speed"],
                    text=weather_chart_df["avg_speed_label"],
                    textposition="outside",
                    marker_color=["#1f77b4", "#17becf"][
                        : len(weather_chart_df)
                    ],
                    customdata=weather_chart_df[["observation_count"]].to_numpy(),
                    hovertemplate=(
                        "Điều kiện: %{x}<br>Tốc độ TB: %{y:.1f} km/h<br>"
                        "Quan sát: %{customdata[0]}<extra></extra>"
                    ),
                )
            )
            fig_wspeed.update_layout(
                title="Tốc độ trung bình theo điều kiện thời tiết",
                xaxis_title="",
                yaxis_title="Tốc độ trung bình (km/h)",
                xaxis={
                    "categoryorder": "array",
                    "categoryarray": weather_chart_df["weather_condition"].tolist(),
                },
            )
            style_dark(fig_wspeed)
            st.plotly_chart(fig_wspeed, width="stretch")
        with weather_cols[1]:
            fig_wcong = go.Figure(
                go.Bar(
                    x=weather_chart_df["weather_condition"],
                    y=weather_chart_df["avg_congestion_percent"],
                    text=weather_chart_df["avg_congestion_label"],
                    textposition="outside",
                    marker_color=["#1f77b4", "#17becf"][
                        : len(weather_chart_df)
                    ],
                    customdata=weather_chart_df[["observation_count"]].to_numpy(),
                    hovertemplate=(
                        "Điều kiện: %{x}<br>Ùn tắc TB: %{y:.1f}%<br>"
                        "Quan sát: %{customdata[0]}<extra></extra>"
                    ),
                )
            )
            fig_wcong.update_layout(
                title="Mức ùn tắc trung bình theo điều kiện thời tiết",
                xaxis_title="",
                yaxis_title="Mức ùn tắc trung bình (%)",
                xaxis={
                    "categoryorder": "array",
                    "categoryarray": weather_chart_df["weather_condition"].tolist(),
                },
            )
            style_dark(fig_wcong)
            st.plotly_chart(fig_wcong, width="stretch")
        st.caption(
            " • ".join(
                f"{row['weather_condition']}: {int(row['observation_count'])} quan sát"
                for _, row in weather_chart_df.iterrows()
            )
        )


if __name__ == "__main__":
    main()
