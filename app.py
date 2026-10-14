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
from utils import geocoding as geocoding_service

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

# ---- Chuẩn hiển thị tiếng Việt (presentation layer only) ----
# `road_name` trong dữ liệu (CSV/Spark) của các tuyến HIỆN CÓ giữ nguyên
# không dấu để tương thích lịch sử/join/snapshot; chỉ ánh xạ sang tên có
# dấu khi hiển thị. Tuyến MỚI thêm từ tìm kiếm lưu tên có dấu do nguồn
# địa lý cung cấp (chưa có lịch sử nên không xung đột khóa).
ROAD_DISPLAY_NAMES = {
    "Nguyen Trai": "Nguyễn Trãi",
    "Truong Chinh": "Trường Chinh",
    "Giai Phong": "Giải Phóng",
    "Cau Giay": "Cầu Giấy",
    "Xuan Thuy": "Xuân Thủy",
    "Dai Co Viet": "Đại Cồ Việt",
    "Kim Ma": "Kim Mã",
    "Lang Ha": "Láng Hạ",
    "Le Van Luong": "Lê Văn Lương",
    "Minh Khai": "Minh Khai",
    "Nguyen Chi Thanh": "Nguyễn Chí Thanh",
    "Pham Van Dong": "Phạm Văn Đồng",
    "Tay Son": "Tây Sơn",
    "Tran Duy Hung": "Trần Duy Hưng",
    "Pham Hung": "Phạm Hùng",
}

ROAD_NAME_PREFIXES = ("duong", "pho", "dai lo")


def strip_diacritics(text):
    """Bỏ dấu tiếng Việt, chữ thường (pure, dùng so trùng gần đúng)."""
    import unicodedata

    normalized = unicodedata.normalize("NFD", str(text))
    stripped = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    return stripped.replace("đ", "d").replace("Đ", "d").lower()


def canonical_road_key(name):
    """Khóa so trùng gần đúng: bỏ dấu + bỏ tiền tố đường/phố (pure)."""
    key = " ".join(strip_diacritics(name).split())
    for prefix in ROAD_NAME_PREFIXES:
        if key.startswith(prefix + " "):
            key = key[len(prefix) + 1:]
            break
    return key.strip()


def names_match_loose(a, b):
    """Hai tên có thể cùng tuyến (bỏ dấu, tiền tố, hoa/thường) (pure)."""
    return bool(canonical_road_key(a)) and canonical_road_key(a) == canonical_road_key(b)

WEEKDAY_LABELS = {
    0: "Thứ Hai",
    1: "Thứ Ba",
    2: "Thứ Tư",
    3: "Thứ Năm",
    4: "Thứ Sáu",
    5: "Thứ Bảy",
    6: "Chủ Nhật",
}

# Màu trạng thái giao thông (đồng nhất KPI/bản đồ/biểu đồ/chú giải).
# Ngưỡng phân loại KHÔNG đổi (Phase 5): <20 / <=40 / <=60 / còn lại.
STATUS_COLORS_HEX = {
    "Thông thoáng": "#22C55E",
    "Đông": "#EAB308",
    "Ùn tắc": "#F97316",
    "Ùn tắc nghiêm trọng": "#EF4444",
}

CHART_ACCENT = "#38BDF8"
CHART_ACCENT_2 = "#818CF8"
ROAD_TRACE_COLORS = [
    "#38BDF8", "#818CF8", "#22C55E", "#EAB308",
    "#F97316", "#EF4444", "#E879F9",
]


def display_road_name(raw):
    """Tên tuyến có dấu để hiển thị (pure); tên lạ giữ nguyên."""
    return ROAD_DISPLAY_NAMES.get(str(raw).strip(), str(raw))


def congestion_hex_for_value(pct):
    """Màu hex theo giá trị ùn tắc TB, cùng ngưỡng Phase 5 (pure).

    <20 xanh lá / <=40 vàng / <=60 cam / còn lại đỏ.
    """
    value = float(pct)
    if value < 20:
        return STATUS_COLORS_HEX["Thông thoáng"]
    if value <= 40:
        return STATUS_COLORS_HEX["Đông"]
    if value <= 60:
        return STATUS_COLORS_HEX["Ùn tắc"]
    return STATUS_COLORS_HEX["Ùn tắc nghiêm trọng"]


def fmt_1(value):
    """Số 1 thập phân kiểu Việt (24,2) (pure)."""
    return f"{float(value):.1f}".replace(".", ",")


def fmt_pct(value):
    """Phần trăm kiểu Việt (36,9%) (pure)."""
    return f"{float(value):.1f}%".replace(".", ",")


def fmt_int(value):
    """Số nguyên phân tách hàng nghìn kiểu Việt (1.351) (pure)."""
    return f"{int(value):,}".replace(",", ".")


def describe_data_status(latest_hanoi, syncing):
    """Trạng thái dữ liệu trung thực từ snapshot (pure).

    - syncing (đang build snapshot mới) -> "Đang đồng bộ".
    - Ngược lại so tuổi quan sát mới nhất với hiện tại:
      <=120 phút -> "Dữ liệu sẵn sàng", còn lại -> "Dữ liệu đã cũ".
    KHÔNG suy diễn collector đang chạy. Trả về (key, nhãn, màu hex).
    """
    if syncing:
        return ("syncing", "Đang đồng bộ dữ liệu mới", "#EAB308")
    try:
        now = pd.Timestamp.now(tz=HANOI_TZ)
        age_min = (now - latest_hanoi).total_seconds() / 60
    except Exception:
        return ("unknown", "Không xác định được trạng thái", "#94A3B8")
    if age_min <= 120:
        return ("ready", "Dữ liệu sẵn sàng", "#22C55E")
    return ("stale", "Dữ liệu đã cũ", "#EAB308")

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


def filter_combined_by_roads_dates(combined_df, event_hanoi, selected_roads,
                                    selected_dates):
    """Lọc combined theo tuyến + ngày Hanoi đã chọn, trả về COPY (pure).

    selected_dates là các datetime.date (so theo ngày Hanoi). Lựa chọn được
    so với dữ liệu thực tế -> ValueError rõ ràng (không ignore ngầm, không
    tự điền ngày/giờ còn thiếu). Không mutate input, không ghi CSV.
    """
    known_roads = set(combined_df["road_name"].dropna().astype(str).tolist())
    bad_roads = [r for r in selected_roads if r not in known_roads]
    if bad_roads:
        raise ValueError(
            "Tuyến đường không có trong dữ liệu: " + ", ".join(map(str, bad_roads)) + "."
        )
    known_dates = {d for d in event_hanoi.dt.date if pd.notna(d)}
    bad_dates = [d for d in selected_dates if d not in known_dates]
    if bad_dates:
        raise ValueError("Ngày đã chọn không có dữ liệu quan sát.")
    if not selected_roads:
        raise ValueError("Chọn ít nhất một tuyến đường để vẽ biểu đồ.")
    if not selected_dates:
        raise ValueError("Chọn ít nhất một ngày để vẽ biểu đồ.")
    mask = combined_df["road_name"].isin(selected_roads) & event_hanoi.dt.date.isin(
        set(selected_dates)
    )
    return combined_df.loc[mask.fillna(False)].copy()


def resolve_all_or_selected(selected, all_options):
    """Trả về lựa chọn hiệu lực cho bộ lọc (pure).

    Bỏ trống nghĩa là "tất cả" (tránh hàng loạt chip khi không lọc);
    chọn 1 hoặc nhiều giá trị để lọc. Không mutate input.
    """
    if not selected:
        return list(all_options)
    return list(selected)


def prepare_hourly_speed_filtered(work_df):
    """Tốc độ TB theo khung giờ từ snapshot đã lọc (pure, copy mới).

    Gộp nhiều ngày theo cùng giờ -> tốc độ TB theo khung giờ (không phải
    chuỗi thời gian một ngày). Chỉ các giờ có dữ liệu; không nội suy giờ
    thiếu. Trả về DataFrame [local_hour, avg_speed, n_obs] sort giờ ASC.
    """
    if work_df.empty:
        raise ValueError("Không có quan sát nào trong phạm vi bộ lọc.")
    speed = pd.to_numeric(work_df["current_speed"], errors="coerce")
    if int(speed.isna().sum()):
        raise ValueError("Cột current_speed có giá trị không phải số/trống.")
    hour = pd.to_numeric(work_df["local_hour"], errors="coerce")
    if int(hour.isna().sum()) or bool(((hour % 1) != 0).any()) or bool(
        (hour < 0).any() or (hour > 23).any()
    ):
        raise ValueError("Cột local_hour phải là giờ nguyên 0..23.")
    grouped = (
        pd.DataFrame({"local_hour": hour.astype(int), "speed": speed})
        .groupby("local_hour", sort=True)
        .agg(avg_speed=("speed", "mean"), n_obs=("speed", "size"))
        .reset_index()
    )
    return grouped


def prepare_road_congestion_filtered(road_df, selected_roads):
    """Ùn tắc TB theo tuyến từ road_summary, lọc tuyến đã chọn (pure).

    Sort giảm dần theo ùn tắc; kèm màu theo ngưỡng Phase 5 + nhãn hiển thị
    + tên tuyến có dấu. Không đổi ngưỡng.
    """
    speed_df_all, _ = prepare_road_chart_data(road_df)
    work = speed_df_all[speed_df_all["road_name"].isin(selected_roads)].copy()
    if work.empty:
        raise ValueError("Không có tuyến nào trong phạm vi bộ lọc.")
    work = work.sort_values(
        ["avg_congestion_percent", "road_name"], ascending=[False, True],
        kind="mergesort",
    ).copy()
    work["color"] = work["avg_congestion_percent"].map(congestion_hex_for_value)
    work["display_name"] = work["road_name"].map(display_road_name)
    work["avg_congestion_label"] = work["avg_congestion_percent"].map(
        lambda v: fmt_pct(v)
    )
    return work


def prepare_congestion_heatmap_data(work_df):
    """Ma trận ùn tắc TB tuyến × giờ từ snapshot đã lọc (pure).

    Ô thiếu dữ liệu -> NaN (để trống, không gán 0, không nội suy). Trả về
    dict {hours, roads, display_roads, z, counts}.
    """
    if work_df.empty:
        raise ValueError("Không có quan sát nào trong phạm vi bộ lọc.")
    congestion = pd.to_numeric(work_df["congestion_percent"], errors="coerce")
    if int(congestion.isna().sum()):
        raise ValueError("Cột congestion_percent có giá trị không phải số/trống.")
    hour = pd.to_numeric(work_df["local_hour"], errors="coerce")
    if int(hour.isna().sum()):
        raise ValueError("Cột local_hour có giá trị không phải số/trống.")
    frame = pd.DataFrame({
        "road": work_df["road_name"].astype(str),
        "hour": hour.astype(int),
        "congestion": congestion,
    })
    roads = sorted(frame["road"].unique().tolist())
    hours = sorted(frame["hour"].unique().tolist())
    pivot = frame.pivot_table(index="road", columns="hour",
                              values="congestion", aggfunc="mean")
    counts = frame.pivot_table(index="road", columns="hour",
                               values="congestion", aggfunc="size")
    pivot = pivot.reindex(index=roads, columns=hours)
    counts = counts.reindex(index=roads, columns=hours)
    return {
        "hours": hours,
        "roads": roads,
        "display_roads": [display_road_name(r) for r in roads],
        "z": pivot.to_numpy(dtype=float),
        "counts": counts.to_numpy(dtype=float),
    }


def prepare_trend_data(work_df, event_hanoi):
    """Chuỗi ùn tắc theo thời gian quan sát, tách theo tuyến (pure).

    Trả về (points_df, skipped): points [observed_hanoi (naive), road_name,
    congestion_percent] sort thời gian; tuyến <2 điểm bị bỏ qua và liệt kê
    trong skipped (không vẽ xu hướng giả). Không dự đoán tương lai.
    """
    if work_df.empty:
        raise ValueError("Không có quan sát nào trong phạm vi bộ lọc.")
    congestion = pd.to_numeric(work_df["congestion_percent"], errors="coerce")
    if int(congestion.isna().sum()):
        raise ValueError("Cột congestion_percent có giá trị không phải số/trống.")
    instants = pd.to_datetime(event_hanoi.loc[work_df.index], utc=True)
    if int(instants.isna().sum()):
        raise ValueError("Có thời điểm quan sát không đọc được.")
    hanoi_naive = instants.dt.tz_convert(HANOI_TZ).dt.tz_localize(None)
    frame = pd.DataFrame({
        "observed": hanoi_naive,
        "road_name": work_df["road_name"].astype(str).to_numpy(),
        "congestion": congestion.to_numpy(dtype=float),
    }).sort_values(["observed", "road_name"], kind="mergesort").reset_index(drop=True)
    counts = frame["road_name"].value_counts()
    skipped = sorted(counts[counts < 2].index.tolist())
    points = frame[~frame["road_name"].isin(skipped)].copy()
    if points.empty:
        raise ValueError("Mỗi tuyến có ít hơn 2 quan sát — chưa đủ để vẽ xu hướng.")
    return points, skipped


def prepare_status_distribution(work_df):
    """Tỷ lệ trạng thái theo tuyến từ quan sát thực tế (pure).

    Tổng mỗi tuyến xấp xỉ 100% (sai lệch chỉ do làm tròn). Trả về DataFrame
    [road_name, display_name + 4 cột tỷ lệ + n_obs], sort theo tên tuyến.
    """
    if work_df.empty:
        raise ValueError("Không có quan sát nào trong phạm vi bộ lọc.")
    unknown = set(work_df["congestion_level"].dropna().astype(str).tolist()) - set(
        LEVEL_SEMANTIC_ORDER
    )
    if unknown:
        raise ValueError(
            "Trạng thái lạ trong dữ liệu: " + ", ".join(sorted(unknown)) + "."
        )
    table = pd.crosstab(work_df["road_name"].astype(str),
                        work_df["congestion_level"].astype(str))
    for level in LEVEL_SEMANTIC_ORDER:
        if level not in table.columns:
            table[level] = 0
    table = table[LEVEL_SEMANTIC_ORDER]
    totals = table.sum(axis=1)
    pct = table.div(totals, axis=0) * 100.0
    road_list = table.index.tolist()
    out = pd.DataFrame({
        "road_name": road_list,
        "display_name": [display_road_name(r) for r in road_list],
        "n_obs": totals.tolist(),
    })
    out["n_obs"] = out["n_obs"].astype(int)
    for level in LEVEL_SEMANTIC_ORDER:
        out[level] = pct[level].to_numpy(dtype=float)
    return out.sort_values("road_name", kind="mergesort").reset_index(drop=True)


def prepare_weekday_comparison(work_df, event_hanoi):
    """Ùn tắc TB theo ngày trong tuần từ snapshot đã lọc (pure).

    Chỉ các ngày có dữ liệu; trả về (df, missing_labels): df [dow, label,
    avg_congestion, n_obs] theo thứ tự Thứ Hai -> Chủ Nhật; missing_labels
    là các ngày chưa có dữ liệu (không tự điền).
    """
    if work_df.empty:
        raise ValueError("Không có quan sát nào trong phạm vi bộ lọc.")
    congestion = pd.to_numeric(work_df["congestion_percent"], errors="coerce")
    if int(congestion.isna().sum()):
        raise ValueError("Cột congestion_percent có giá trị không phải số/trống.")
    instants = pd.to_datetime(event_hanoi.loc[work_df.index], utc=True)
    if int(instants.isna().sum()):
        raise ValueError("Có thời điểm quan sát không đọc được.")
    dow = instants.dt.tz_convert(HANOI_TZ).dt.dayofweek
    frame = pd.DataFrame({"dow": dow.to_numpy(), "congestion": congestion.to_numpy()})
    grouped = frame.groupby("dow", sort=True).agg(
        avg_congestion=("congestion", "mean"), n_obs=("congestion", "size")
    )
    present = sorted(grouped.index.tolist())
    missing = [WEEKDAY_LABELS[d] for d in range(7) if d not in present]
    rows = [{
        "dow": d,
        "label": WEEKDAY_LABELS[d],
        "avg_congestion": float(grouped.loc[d, "avg_congestion"]),
        "n_obs": int(grouped.loc[d, "n_obs"]),
    } for d in present]
    return pd.DataFrame(rows), missing


def prepare_rain_scatter(work_df, event_hanoi):
    """Điểm (lượng mưa, ùn tắc) từ bản ghi ghép thời tiết hợp lệ (pure).

    Chỉ bản ghi weather_matched hợp lệ và đủ rain/congestion; thiếu không
    được coi là không mưa. Trả về (points_df, rain_pos_count, note).
    """
    mask, _ = normalize_matched(work_df["weather_matched"])
    matched = work_df.loc[mask].copy()
    if matched.empty:
        raise ValueError("Không có bản ghi nào ghép được dữ liệu thời tiết.")
    rain = pd.to_numeric(matched["rain"], errors="coerce")
    congestion = pd.to_numeric(matched["congestion_percent"], errors="coerce")
    valid = rain.notna() & congestion.notna()
    points = matched.loc[valid].copy()
    if points.empty:
        raise ValueError("Các bản ghi ghép thời tiết thiếu lượng mưa/mức ùn tắc.")
    instants = pd.to_datetime(event_hanoi.loc[points.index], utc=True)
    hanoi_naive = instants.dt.tz_convert(HANOI_TZ).dt.tz_localize(None)
    out = pd.DataFrame({
        "rain": rain.loc[valid].to_numpy(dtype=float),
        "congestion": congestion.loc[valid].to_numpy(dtype=float),
        "road_name": points["road_name"].astype(str).to_numpy(),
        "observed": hanoi_naive,
    }).sort_values(["observed", "road_name"], kind="mergesort").reset_index(drop=True)
    rain_pos = int((out["rain"] > 0).sum())
    if rain_pos == 0:
        note = "Chưa ghi nhận lượng mưa > 0 trong phạm vi đã chọn."
    elif rain_pos < 10:
        note = (f"Chỉ có {rain_pos} quan sát có mưa — mẫu còn ít, "
                "chưa đủ để đánh giá mối liên hệ.")
    else:
        note = (f"{rain_pos}/{len(out)} quan sát có mưa. Biểu đồ chỉ mô tả "
                "phân bố, không kết luận quan hệ nhân quả.")
    return out, rain_pos, note


def prepare_temp_scatter(work_df, event_hanoi):
    """Điểm (nhiệt độ, tốc độ) từ bản ghi có nhiệt độ hợp lệ (pure).

    Không vẽ đường hồi quy, không kết luận tương quan mạnh. Trả về
    points_df [temperature, speed, road_name, observed].
    """
    temp = pd.to_numeric(work_df["temperature"], errors="coerce")
    speed = pd.to_numeric(work_df["current_speed"], errors="coerce")
    valid = temp.notna() & speed.notna()
    if int(valid.sum()) == 0:
        raise ValueError("Không có bản ghi nào có nhiệt độ và tốc độ hợp lệ.")
    points = work_df.loc[valid].copy()
    instants = pd.to_datetime(event_hanoi.loc[points.index], utc=True)
    hanoi_naive = instants.dt.tz_convert(HANOI_TZ).dt.tz_localize(None)
    return pd.DataFrame({
        "temperature": temp.loc[valid].to_numpy(dtype=float),
        "speed": speed.loc[valid].to_numpy(dtype=float),
        "road_name": points["road_name"].astype(str).to_numpy(),
        "observed": hanoi_naive,
    }).sort_values(["observed", "road_name"], kind="mergesort").reset_index(drop=True)


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
    zoom 12 cho cum Ha Noi. Nền bản đồ: OpenStreetMap mặc định (tile công
    khai, không API key, attribution tự động của Folium). GIỚI HẠN: đã thử
    CartoDB dark_matter nhưng folium 0.20 yêu cầu API key cho tile CartoDB
    ("CartoDB tiles now require an API key"), trái với yêu cầu không thêm
    key mới — nên dùng fallback bản đồ sáng dịu, không dùng URL tile không
    rõ nguồn để lách key. Marker tại tọa độ đại diện của tuyến (không vẽ
    road geometry vì không có dữ liệu hình học thật). Tra về
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
            background: #F1F5F9; color: #0B1020; padding: 8px 10px;
            font-size: 12px; border: 1px solid #94A3B8; border-radius: 8px;">
<b>Mức ùn tắc</b><br>
<span style="color: #16A34A;">&#9679;</span> Thông thoáng<br>
<span style="color: #CA8A04;">&#9679;</span> Đông<br>
<span style="color: #EA580C;">&#9679;</span> Ùn tắc<br>
<span style="color: #DC2626;">&#9679;</span> Ùn tắc nghiêm trọng
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
        f"Đã thêm {display_road_name(entry['road_name'])} vào danh sách theo dõi. "
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
        f"Đã xóa tuyến {display_road_name(target)} khỏi danh sách theo dõi. "
        "Dữ liệu lịch sử đã thu thập không bị xóa.",
    )
    st.rerun()


ROAD_SEARCH_RESULTS_KEY = "road_search_results"
ROAD_SEARCH_QUERY_KEY = "road_search_query"


def _short_address(address, limit=120):
    """Rut gon dia chi hien thi (pure)."""
    text = str(address or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "…"


def _handle_search_roads(query):
    """Tim tuyen Ha Noi khi user nhan nut (KHONG goi theo tung ky tu).

    Luu ket qua vao session_state de rerun khong goi lai API.
    """
    try:
        with st.spinner("Đang tìm tuyến đường…"):
            results = geocoding_service.search_roads_in_hanoi(query)
    except ValueError as exc:
        _set_road_flash("error", str(exc))
        st.rerun()
    except geocoding_service.GeocodingError as exc:
        _set_road_flash("error", str(exc))
        st.rerun()
    st.session_state[ROAD_SEARCH_RESULTS_KEY] = results
    st.session_state[ROAD_SEARCH_QUERY_KEY] = query.strip()
    st.session_state.pop("road_search_pick", None)
    if not results:
        _set_road_flash(
            "error",
            "Không tìm thấy tuyến đường phù hợp tại Hà Nội. "
            "Hãy thử tên khác hoặc chọn từ danh sách gợi ý.",
        )
    else:
        _set_road_flash(
            "success",
            f"Tìm thấy {len(results)} kết quả tại Hà Nội. "
            "Chọn kết quả chính xác rồi nhấn “Thêm vào danh sách theo dõi”.",
        )
    st.rerun()


def _handle_add_searched_road(pick_index, monitored_names):
    """Them tuyen tu ket qua tim kiem da chon (co toa do that).

    Kiem tra trung khop (road_manager) + trung gan dung bo dau
    (names_match_loose) truoc khi ghi roads.csv. Khong goi TomTom ngay;
    collector nhan tuyen moi o chu ky tiep theo.
    """
    results = st.session_state.get(ROAD_SEARCH_RESULTS_KEY) or []
    try:
        pick = results[int(pick_index)]
    except (TypeError, ValueError, IndexError):
        _set_road_flash(
            "error", "Kết quả đã chọn không còn hợp lệ. Hãy tìm lại."
        )
        st.rerun()
    for existing in monitored_names or []:
        if names_match_loose(pick["name"], existing):
            _set_road_flash(
                "error",
                f"Tuyến này có thể đã được theo dõi dưới tên "
                f"“{display_road_name(existing)}”.",
            )
            st.rerun()
    try:
        road_manager.add_road(pick["name"], pick["lat"], pick["lon"])
    except road_manager.DuplicateRoadError:
        _set_road_flash("error", "Tuyến này đã có trong danh sách theo dõi.")
        st.rerun()
    except road_manager.RoadManagerError as exc:
        _set_road_flash("error", f"Không thêm được tuyến: {exc}")
        st.rerun()
    for key in (ROAD_SEARCH_RESULTS_KEY, ROAD_SEARCH_QUERY_KEY,
                "road_search_input", "road_search_pick"):
        st.session_state.pop(key, None)
    st.session_state.pop("add_road_select", None)
    _set_road_flash(
        "success",
        f"Đã thêm {pick['name']} vào danh sách theo dõi. "
        "Hệ thống theo dõi tọa độ đại diện của tuyến; dữ liệu sẽ xuất hiện "
        "trên dashboard sau chu kỳ thu thập tiếp theo.",
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
        with st.sidebar.expander("Danh sách tuyến đang theo dõi"):
            for name in sorted(monitored_names):
                st.markdown(f"- {display_road_name(name)}")

    with st.sidebar.expander("➕ Thêm tuyến"):
        available = available_catalog_roads(HANOI_ROAD_CATALOG, monitored_names)
        if available:
            st.caption("Thêm nhanh từ danh sách gợi ý.")
            with st.form("add_road_form", clear_on_submit=True):
                selected = st.selectbox(
                    "Chọn tuyến", options=available,
                    format_func=display_road_name, key="add_road_select"
                )
                add_submitted = st.form_submit_button("Thêm tuyến")
            if add_submitted:
                _handle_add_road(selected)
        st.caption("Tìm tuyến khác tại Hà Nội.")
        query = st.text_input(
            "Tên tuyến đường",
            placeholder="Ví dụ: Láng Hạ",
            key="road_search_input",
        )
        if st.button("Tìm tuyến đường", key="road_search_button"):
            _handle_search_roads(query)
        results = st.session_state.get(ROAD_SEARCH_RESULTS_KEY) or []
        if results:
            labels = {
                i: f"{r['name']} — {_short_address(r['display_address'])}"
                for i, r in enumerate(results)
            }
            pick = st.radio(
                "Kết quả tìm kiếm (chọn một)",
                options=list(labels),
                format_func=lambda i: labels[i],
                key="road_search_pick",
            )
            if st.button(
                "Thêm vào danh sách theo dõi", key="road_search_add_button"
            ):
                _handle_add_searched_road(pick, monitored_names)
        st.caption(
            "Hệ thống theo dõi tọa độ đại diện của tuyến, "
            "không phải toàn bộ đoạn đường."
        )

    with st.sidebar.expander("🗑️ Xóa tuyến"):
        if not monitored_names:
            st.info("Chưa có tuyến nào được theo dõi.")
        else:
            target = st.selectbox(
                "Chọn tuyến cần xóa",
                options=monitored_names,
                format_func=display_road_name,
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
                "Hãy chạy xử lý dữ liệu trước khi mở dashboard."
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
    status_key, status_label, status_color = describe_data_status(
        latest_hanoi, bool(snapshot.get("syncing"))
    )
    st.sidebar.markdown("### Phân tích giao thông Hà Nội")
    st.sidebar.markdown(
        f'<div class="sidebar-status"><span style="color: {status_color};">●</span>'
        f"{status_label}</div>",
        unsafe_allow_html=True,
    )
    n_obs = len(combined_df)
    n_roads = int(road_df["road_name"].nunique()) if not road_df.empty else 0
    st.sidebar.markdown(
        f"""
        <div class="sidebar-metric"><span class="sidebar-metric-icon">📊</span>Tổng lượt ghi nhận: {fmt_int(n_obs)}</div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🛣️</span>Số tuyến đang theo dõi: {n_roads}</div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🕐</span>Cập nhật gần nhất: {latest_hanoi.strftime('%d/%m/%Y %H:%M')}</div>
        """,
        unsafe_allow_html=True,
    )

    # ---- Sidebar: quan ly tuyen duong (roads.csv, khong cham processed) ----
    render_road_management_sidebar(
        set(combined_df["road_name"].dropna().astype(str).tolist())
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("Nguồn dữ liệu: TomTom · Open-Meteo")

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
            '<div class="dash-title">Phân tích giao thông Hà Nội</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="dash-subtitle">Hệ thống giám sát và phân tích dữ liệu '
            "giao thông kết hợp thời tiết</div>",
            unsafe_allow_html=True,
        )
    with hdr_right:
        st.markdown(
            f'<div class="status-line"><span style="color: {status_color};">●</span> '
            f"{status_label.upper()}</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="header-meta">Cập nhật gần nhất: '
            f"<b>{latest_hanoi.strftime('%d/%m/%Y %H:%M')}</b></div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="header-meta">Tự động làm mới: {AUTO_REFRESH_SECONDS} giây</div>',
            unsafe_allow_html=True,
        )
        render_data_update_header()

    # ---- 4 KPI cards global (khong chiu anh huong detail filters) ----
    # Tat ca KPI tinh tren TOAN BO du lieu lich su trong snapshot hien tai;
    # ban do + bang "Giao thong moi nhat" dung quan sat moi nhat moi tuyen.
    kpi_cards = [
        (KPI_ICONS["n_roads"], "Tuyến đường theo dõi", fmt_int(metrics['n_roads'])),
        (KPI_ICONS["avg_speed"], "Tốc độ trung bình", f"{fmt_1(metrics['avg_speed'])} km/h"),
        (KPI_ICONS["n_obs"], "Lượt ghi nhận", fmt_int(metrics['n_obs'])),
        (KPI_ICONS["avg_congestion"], "Mức ùn tắc trung bình", fmt_pct(metrics['avg_congestion'])),
    ]
    kpi_cols = st.columns(4)
    for i, (icon, label, value) in enumerate(kpi_cards):
        with kpi_cols[i]:
            display_value = value if metrics['n_obs'] else "—"
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label"><span class="kpi-icon">{icon}</span>{label}</div>
                    <div class="kpi-value">{display_value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ---- Du lieu dung chung: quan sat moi nhat moi tuyen ----
    latest_df = get_latest_road_observations(combined_df, event_utc)
    valid_df, invalid_roads = validate_map_coordinates(latest_df)

    # ---- Ban do giao thong ----
    st.markdown('<div class="section-header"><span class="section-icon">🗺️</span><h2>Bản đồ tình hình giao thông Hà Nội</h2></div>', unsafe_allow_html=True)
    if invalid_roads:
        st.warning(
            "Bỏ qua marker thiếu/sai tọa độ của tuyến: "
            + ", ".join(display_road_name(r) for r in invalid_roads)
            + "."
        )
    bad_levels = validate_congestion_levels(valid_df)
    if bad_levels:
        st.error(
            "Trạng thái ùn tắc lạ trên quan sát mới nhất (không thể tô màu): "
            + ", ".join(bad_levels)
            + ". Vui lòng kiểm tra lại dữ liệu đã xử lý."
        )
        valid_df = valid_df[
            valid_df["congestion_level"].isin(list(CONGESTION_COLORS))
        ]
    if valid_df.empty:
        st.info("Chưa có marker hợp lệ nào để vẽ bản đồ.")
    else:
        traffic_map, n_markers, _ = build_traffic_map(valid_df, event_hanoi)
        st_folium(traffic_map, use_container_width=True, height=520,
                  returned_objects=[], key="traffic_map")
        st.markdown(
            " • ".join(
                f"{STATUS_EMOJI.get(lv, '⚪')} {lv}"
                for lv in CONGESTION_COLORS
            )
        )
        st.caption("Nền bản đồ: © OpenStreetMap contributors.")

    # ---- Bo loc du lieu cho cac bieu do ----
    st.markdown('<div class="section-header"><span class="section-icon">🔍</span><h2>Bộ lọc dữ liệu</h2></div>', unsafe_allow_html=True)
    all_roads_sorted = sorted(combined_df["road_name"].dropna().astype(str).unique().tolist())
    all_dates = available_observation_dates(event_hanoi)
    filt_cols = st.columns(2)
    with filt_cols[0]:
        selected_f_roads = st.multiselect(
            "Tuyến đường", options=all_roads_sorted, default=[],
            format_func=display_road_name, key="f_roads",
            placeholder="Tất cả tuyến đường",
        )
    with filt_cols[1]:
        selected_f_dates = st.multiselect(
            "Ngày quan sát", options=all_dates, default=[],
            format_func=lambda d: d.strftime("%d/%m/%Y"), key="f_dates",
            placeholder="Toàn bộ thời gian có dữ liệu",
        )
    effective_f_roads = resolve_all_or_selected(selected_f_roads, all_roads_sorted)
    effective_f_dates = resolve_all_or_selected(selected_f_dates, all_dates)
    try:
        chart_df = filter_combined_by_roads_dates(
            combined_df, event_hanoi, effective_f_roads, effective_f_dates
        )
        filters_ok = True
        filter_error = ""
    except ValueError as exc:
        chart_df = combined_df.iloc[0:0].copy()
        filters_ok = False
        filter_error = str(exc)
    if not filters_ok:
        st.warning(filter_error)

    def _chart_error(prefix, exc):
        st.info(f"{prefix}: {exc}")

    tab_overview, tab_deep, tab_weather = st.tabs(
        ["Tổng quan giao thông", "Phân tích chuyên sâu", "Phân tích thời tiết"]
    )

    # ==== TAB 1: Tong quan (bieu do 1 + 2) ====
    with tab_overview:
        ov_col1, ov_col2 = st.columns(2)
        with ov_col1:
            try:
                if not filters_ok:
                    raise ValueError(filter_error)
                hourly_speed = prepare_hourly_speed_filtered(chart_df)
                fig_hspeed = go.Figure()
                for road in sorted(chart_df["road_name"].astype(str).unique().tolist()):
                    sub = chart_df[chart_df["road_name"].astype(str) == road]
                    sub_hourly = prepare_hourly_speed_filtered(sub)
                    color = ROAD_TRACE_COLORS[
                        sorted(chart_df["road_name"].astype(str).unique().tolist()).index(road)
                        % len(ROAD_TRACE_COLORS)
                    ]
                    fig_hspeed.add_trace(go.Scatter(
                        x=sub_hourly["local_hour"],
                        y=sub_hourly["avg_speed"],
                        mode="lines+markers",
                        name=display_road_name(road),
                        marker={"size": 7},
                        connectgaps=False,
                        customdata=sub_hourly[["n_obs"]].to_numpy(),
                        hovertemplate=(
                            f"Tuyến: {display_road_name(road)}<br>"
                            "Giờ: %{x}h<br>Tốc độ trung bình: %{y:.1f} km/h<br>"
                            "Số quan sát: %{customdata[0]}<extra></extra>"
                        ),
                    ))
                missing_hours = sorted(
                    set(range(24)) - set(hourly_speed["local_hour"].tolist())
                )
                fig_hspeed.update_layout(
                    title="Tốc độ trung bình theo giờ",
                    xaxis_title="Giờ trong ngày",
                    yaxis_title="Tốc độ trung bình (km/h)",
                    xaxis={"dtick": 1},
                    legend={"title": "Tuyến đường"},
                )
                style_dark(fig_hspeed)
                st.plotly_chart(fig_hspeed, width="stretch")
                if missing_hours:
                    st.caption(
                        "Không có dữ liệu các giờ: "
                        + ", ".join(f"{h}h" for h in missing_hours) + "."
                    )
            except ValueError as exc:
                _chart_error("Biểu đồ tốc độ theo giờ chưa vẽ được", exc)
            except Exception as exc:
                st.error(f"Biểu đồ tốc độ theo giờ gặp lỗi: {exc}")
        with ov_col2:
            try:
                if not filters_ok:
                    raise ValueError(filter_error)
                congestion_roads = prepare_road_congestion_filtered(
                    road_df, effective_f_roads
                )
                fig_cong = go.Figure(
                    go.Bar(
                        x=congestion_roads["avg_congestion_percent"],
                        y=congestion_roads["display_name"],
                        orientation="h",
                        text=congestion_roads["avg_congestion_label"],
                        textposition="outside",
                        marker_color=congestion_roads["color"].tolist(),
                        customdata=congestion_roads[["observation_count"]].to_numpy(),
                        hovertemplate=(
                            "Tuyến: %{y}<br>Mức ùn tắc trung bình: %{x:.1f}%<br>"
                            "Số quan sát: %{customdata[0]}<extra></extra>"
                        ),
                    )
                )
                fig_cong.update_layout(
                    title="Mức ùn tắc trung bình theo tuyến",
                    xaxis_title="Mức ùn tắc trung bình (%)",
                    yaxis_title="",
                    yaxis={
                        "categoryorder": "array",
                        "categoryarray": congestion_roads["display_name"].tolist(),
                    },
                )
                style_dark(fig_cong)
                st.plotly_chart(fig_cong, width="stretch")
            except ValueError as exc:
                _chart_error("Biểu đồ ùn tắc theo tuyến chưa vẽ được", exc)
            except Exception as exc:
                st.error(f"Biểu đồ ùn tắc theo tuyến gặp lỗi: {exc}")

    # ==== TAB 2: Phan tich chuyen sau (bieu do 3 + 4 + 5 + 6) ====
    with tab_deep:
        try:
            if not filters_ok:
                raise ValueError(filter_error)
            heat = prepare_congestion_heatmap_data(chart_df)
            fig_heat = go.Figure(go.Heatmap(
                z=heat["z"],
                x=[f"{h}h" for h in heat["hours"]],
                y=heat["display_roads"],
                customdata=heat["counts"],
                hovertemplate=(
                    "%{y} · %{x}<br>Mức ùn tắc trung bình: %{z:.1f}%<br>"
                    "Số quan sát: %{customdata:.0f}<extra></extra>"
                ),
                colorscale=[
                    [0.0, "#22C55E"], [0.4, "#EAB308"],
                    [0.7, "#F97316"], [1.0, "#EF4444"],
                ],
                colorbar={"title": "Ùn tắc (%)"},
                hoverongaps=False,
            ))
            fig_heat.update_layout(
                title="Ùn tắc theo tuyến và giờ",
                xaxis_title="Giờ trong ngày",
                yaxis_title="",
            )
            style_dark(fig_heat)
            st.plotly_chart(fig_heat, width="stretch")
            st.caption("Ô trống: giờ chưa có dữ liệu.")
        except ValueError as exc:
            _chart_error("Heatmap chưa vẽ được", exc)
        except Exception as exc:
            st.error(f"Heatmap gặp lỗi: {exc}")

        try:
            if not filters_ok:
                raise ValueError(filter_error)
            trend_points, trend_skipped = prepare_trend_data(chart_df, event_hanoi)
            fig_trend = go.Figure()
            trend_roads = sorted(trend_points["road_name"].unique().tolist())
            for idx, road in enumerate(trend_roads):
                sub = trend_points[trend_points["road_name"] == road]
                fig_trend.add_trace(go.Scatter(
                    x=sub["observed"],
                    y=sub["congestion"],
                    mode="lines",
                    name=display_road_name(road),
                    line={"color": ROAD_TRACE_COLORS[idx % len(ROAD_TRACE_COLORS)]},
                    hovertemplate=(
                        f"Tuyến: {display_road_name(road)}<br>"
                        "Thời gian: %{x|%d/%m/%Y %H:%M}<br>"
                        "Mức ùn tắc: %{y:.1f}%<extra></extra>"
                    ),
                ))
            fig_trend.update_layout(
                title="Xu hướng ùn tắc theo thời gian",
                xaxis_title="Thời gian quan sát",
                yaxis_title="Mức ùn tắc (%)",
                legend={"title": "Tuyến đường"},
            )
            style_dark(fig_trend)
            st.plotly_chart(fig_trend, width="stretch")
            if trend_skipped:
                st.caption(
                    "Chưa đủ dữ liệu để vẽ xu hướng: "
                    + ", ".join(display_road_name(r) for r in trend_skipped) + "."
                )
        except ValueError as exc:
            _chart_error("Biểu đồ xu hướng chưa vẽ được", exc)
        except Exception as exc:
            st.error(f"Biểu đồ xu hướng gặp lỗi: {exc}")

        deep_cols = st.columns(2)
        with deep_cols[0]:
            try:
                if not filters_ok:
                    raise ValueError(filter_error)
                dist = prepare_status_distribution(chart_df)
                fig_dist = go.Figure()
                for level in LEVEL_SEMANTIC_ORDER:
                    fig_dist.add_trace(go.Bar(
                        x=dist["display_name"],
                        y=dist[level],
                        name=level,
                        marker_color=STATUS_COLORS_HEX[level],
                        customdata=dist[["n_obs"]].to_numpy(),
                        hovertemplate=(
                            "Tuyến: %{x}<br>Trạng thái: " + level + "<br>"
                            "Tỷ lệ: %{y:.1f}%<br>"
                            "Tổng quan sát tuyến: %{customdata[0]}<extra></extra>"
                        ),
                    ))
                fig_dist.update_layout(
                    title="Phân bố trạng thái giao thông",
                    xaxis_title="",
                    yaxis_title="Tỷ lệ quan sát (%)",
                    barmode="stack",
                    legend={"title": "Trạng thái"},
                )
                style_dark(fig_dist)
                st.plotly_chart(fig_dist, width="stretch")
            except ValueError as exc:
                _chart_error("Biểu đồ phân bố trạng thái chưa vẽ được", exc)
            except Exception as exc:
                st.error(f"Biểu đồ phân bố trạng thái gặp lỗi: {exc}")
        with deep_cols[1]:
            try:
                if not filters_ok:
                    raise ValueError(filter_error)
                weekday_df, weekday_missing = prepare_weekday_comparison(
                    chart_df, event_hanoi
                )
                fig_week = go.Figure(go.Bar(
                    x=weekday_df["label"],
                    y=weekday_df["avg_congestion"],
                    text=weekday_df["avg_congestion"].map(fmt_pct),
                    textposition="outside",
                    marker_color=CHART_ACCENT,
                    customdata=weekday_df[["n_obs"]].to_numpy(),
                    hovertemplate=(
                        "Ngày: %{x}<br>Mức ùn tắc trung bình: %{y:.1f}%<br>"
                        "Số quan sát: %{customdata[0]}<extra></extra>"
                    ),
                ))
                fig_week.update_layout(
                    title="So sánh theo ngày trong tuần",
                    xaxis_title="",
                    yaxis_title="Mức ùn tắc trung bình (%)",
                    xaxis={
                        "categoryorder": "array",
                        "categoryarray": weekday_df["label"].tolist(),
                    },
                )
                style_dark(fig_week)
                st.plotly_chart(fig_week, width="stretch")
                if weekday_missing:
                    st.caption(
                        "Chưa có dữ liệu các ngày: " + ", ".join(weekday_missing) + "."
                    )
            except ValueError as exc:
                _chart_error("Biểu đồ so sánh ngày trong tuần chưa vẽ được", exc)
            except Exception as exc:
                st.error(f"Biểu đồ so sánh ngày trong tuần gặp lỗi: {exc}")

    # ==== TAB 3: Phan tich thoi tiet (bieu do 7 + 8) ====
    with tab_weather:
        wx_cols = st.columns(2)
        with wx_cols[0]:
            try:
                if not filters_ok:
                    raise ValueError(filter_error)
                rain_points, rain_pos, rain_note = prepare_rain_scatter(
                    chart_df, event_hanoi
                )
                fig_rain = go.Figure(go.Scatter(
                    x=rain_points["rain"],
                    y=rain_points["congestion"],
                    mode="markers",
                    marker={"size": 8, "color": CHART_ACCENT_2, "opacity": 0.8},
                    customdata=list(zip(
                        rain_points["road_name"].map(display_road_name),
                        rain_points["observed"].dt.strftime("%d/%m/%Y %H:%M"),
                    )),
                    hovertemplate=(
                        "Tuyến: %{customdata[0]}<br>"
                        "Lượng mưa: %{x:.1f} mm<br>Mức ùn tắc: %{y:.1f}%<br>"
                        "Thời gian: %{customdata[1]}<extra></extra>"
                    ),
                ))
                fig_rain.update_layout(
                    title="Lượng mưa và mức ùn tắc",
                    xaxis_title="Lượng mưa (mm)",
                    yaxis_title="Mức ùn tắc (%)",
                )
                style_dark(fig_rain)
                st.plotly_chart(fig_rain, width="stretch")
                st.caption(rain_note)
            except ValueError as exc:
                _chart_error("Biểu đồ mưa và ùn tắc chưa vẽ được", exc)
            except Exception as exc:
                st.error(f"Biểu đồ mưa và ùn tắc gặp lỗi: {exc}")
        with wx_cols[1]:
            try:
                if not filters_ok:
                    raise ValueError(filter_error)
                temp_points = prepare_temp_scatter(chart_df, event_hanoi)
                fig_temp = go.Figure()
                temp_roads = sorted(temp_points["road_name"].unique().tolist())
                for idx, road in enumerate(temp_roads):
                    sub = temp_points[temp_points["road_name"] == road]
                    fig_temp.add_trace(go.Scatter(
                        x=sub["temperature"],
                        y=sub["speed"],
                        mode="markers",
                        name=display_road_name(road),
                        marker={"size": 8,
                                "color": ROAD_TRACE_COLORS[idx % len(ROAD_TRACE_COLORS)],
                                "opacity": 0.8},
                        customdata=sub["observed"].dt.strftime("%d/%m/%Y %H:%M"),
                        hovertemplate=(
                            f"Tuyến: {display_road_name(road)}<br>"
                            "Nhiệt độ: %{x:.1f} °C<br>Tốc độ: %{y:.1f} km/h<br>"
                            "Thời gian: %{customdata}<extra></extra>"
                        ),
                    ))
                fig_temp.update_layout(
                    title="Nhiệt độ và tốc độ giao thông",
                    xaxis_title="Nhiệt độ (°C)",
                    yaxis_title="Tốc độ giao thông (km/h)",
                    legend={"title": "Tuyến đường"},
                )
                style_dark(fig_temp)
                st.plotly_chart(fig_temp, width="stretch")
            except ValueError as exc:
                _chart_error("Biểu đồ nhiệt độ và tốc độ chưa vẽ được", exc)
            except Exception as exc:
                st.error(f"Biểu đồ nhiệt độ và tốc độ gặp lỗi: {exc}")

    # ---- Second row: latest traffic 65% | weather 35% ----
    row2_left, row2_right = st.columns([2, 1])
    with row2_left:
        st.markdown('<div class="section-header"><span class="section-icon">📋</span><h2>Giao thông mới nhất</h2></div>', unsafe_allow_html=True)
        latest_table_df = prepare_latest_traffic_table(latest_df, event_hanoi)
        latest_table_df["Tuyến đường"] = latest_table_df["Tuyến đường"].map(
            display_road_name
        )
        st.dataframe(latest_table_df, hide_index=True, width="stretch")
    with row2_right:
        st.markdown('<div class="section-header"><span class="section-icon">☁️</span><h2>Thời tiết tại điểm quan sát mới nhất</h2></div>', unsafe_allow_html=True)
        wx = latest_matched_weather(combined_df, event_hanoi)
        if wx is None:
            st.info("Chưa có quan sát nào ghép được dữ liệu thời tiết.")
        else:
            temp_txt = f"{fmt_1(wx['temperature'])} °C" if wx["temperature"] is not None else "Không có dữ liệu"
            hum_txt = f"{fmt_1(wx['humidity'])} %" if wx["humidity"] is not None else "Không có dữ liệu"
            rain_txt = f"{fmt_1(wx['rain'])} mm" if wx["rain"] is not None else "Không có dữ liệu"
            wind_txt = f"{fmt_1(wx['wind_speed'])} m/s" if wx["wind_speed"] is not None else "Không có dữ liệu"
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
                f"{display_road_name(wx['road_name'])} · "
                f"{wx['observed_hanoi'].strftime('%d/%m/%Y %H:%M')}"
            )

    st.markdown('<div class="section-header"><span class="section-icon">📋</span><h2>Bảng dữ liệu chi tiết</h2></div>', unsafe_allow_html=True)
    if metrics["top_road"] is not None:
        st.markdown(
            f"""
            <div class="insight-card">
                <div class="insight-title">Tuyến ùn tắc nhất</div>
                <div class="insight-content">
                    {display_road_name(metrics['top_road']['road_name'])}
                    ({fmt_pct(metrics['top_road']['avg_congestion_percent'])}).
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
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
            "Tuyến đường", options=roads_options, default=[],
            format_func=display_road_name, key="detail_roads",
            placeholder="Tất cả tuyến đường",
        )
    with filter_cols[1]:
        selected_levels = st.multiselect(
            "Trạng thái giao thông", options=levels_options, default=[],
            key="detail_levels",
            placeholder="Tất cả trạng thái",
        )
    effective_roads = resolve_all_or_selected(selected_roads, roads_options)
    effective_levels = resolve_all_or_selected(selected_levels, levels_options)
    try:
        day_df = filter_observations_by_date(
            combined_df, event_hanoi, selected_date
        )
        # Road/status loc tren FULL snapshot (giu validation hien tai);
        # giao voi ngay da chon (AND giao hoan: ket qua giong loc
        # date -> roads -> levels; tuyen hop le nhung vang mat trong ngay
        # cho empty-state than thien thay vi ValueError).
        base_df = filter_detail_observations(
            combined_df, effective_roads, effective_levels
        )
        filtered_df = base_df.loc[base_df.index.isin(day_df.index)].copy()
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    date_label = selected_date.strftime("%d/%m/%Y")
    st.write(
        f"Hiển thị {fmt_int(len(filtered_df))} / {fmt_int(len(day_df))} "
        f"quan sát trong ngày {date_label}"
    )
    if filtered_df.empty:
        st.info("Không có quan sát phù hợp với bộ lọc trong ngày đã chọn.")
    else:
        detail_df = prepare_detail_table(filtered_df, event_hanoi)
        detail_df["Tuyến đường"] = detail_df["Tuyến đường"].map(display_road_name)
        st.dataframe(detail_df, hide_index=True, width="stretch")
        st.download_button(
            "Tải dữ liệu đã lọc (CSV)",
            detail_df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"giao_thong_{selected_date.strftime('%Y%m%d')}.csv",
            mime="text/csv",
            key="detail_download",
        )

    weather_counts = metrics["weather_counts"]
    if "Không mưa" in weather_counts and "Mưa" in weather_counts:
        st.markdown(
            f"""
            <div class="insight-card">
                <div class="insight-title">Điều kiện thời tiết</div>
                <div class="insight-content">
                    {fmt_int(weather_counts['Không mưa'])} quan sát không mưa,
                    {fmt_int(weather_counts['Mưa'])} quan sát mưa.
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
                    {fmt_int(only_count)} quan sát {only_name.lower()}.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.expander("Thông tin phương pháp và giới hạn dữ liệu", expanded=False):
        st.markdown(
            "- KPI tính trên toàn bộ quan sát trong dữ liệu hiện tại; "
            "bản đồ và bảng “Giao thông mới nhất” dùng quan sát mới nhất của từng tuyến.\n"
            "- Bộ lọc tuyến/ngày áp dụng cho các biểu đồ; bộ lọc trạng thái "
            "chỉ dùng cho bảng chi tiết.\n"
            "- Bản đồ đặt marker tại vị trí đại diện của tuyến; nền bản đồ OpenStreetMap.\n"
            "- Ngưỡng trạng thái: dưới 20% thông thoáng, đến 40% đông, "
            "đến 60% ùn tắc, trên 60% ùn tắc nghiêm trọng.\n"
            "- Biểu đồ thời tiết chỉ dùng bản ghi ghép được dữ liệu thời tiết "
            "và chỉ mô tả phân bố, không kết luận quan hệ nhân quả.\n"
            "- Thời điểm hiển thị là thời điểm quan sát, không phải thời gian thực."
        )


if __name__ == "__main__":
    main()
