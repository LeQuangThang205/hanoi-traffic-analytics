"""Phase 7.1-7.6 — Streamlit Dashboard (presentation layer only).

Doc 4 processed snapshots cua Phase 5/6 va hien thi thong tin co ban + KPI
tong quan. KHONG Spark/pyspark, KHONG rerun ETL/analytics, KHONG
TomTom/Open-Meteo, KHONG ghi/sua bat ky CSV nao (read-only). Pandas chi dung
o presentation layer cho du lieu nho (KPI don gian, khong thay Spark).
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

# Marker color theo Phase 5 congestion_level (source of truth, khong suy
# tu congestion_percent lam tron, khong random).
CONGESTION_COLORS = {
    "Thông thoáng": "green",
    "Đông": "orange",
    "Ùn tắc": "red",
    "Ùn tắc nghiêm trọng": "darkred",
}

KPI_ICONS = {
    "n_roads": "🛣️",
    "n_obs": "📊",
    "avg_speed": "🚗",
    "avg_congestion": "🚦",
    "matched_pct": "🌦️",
    "latest_hanoi": "🕐",
}

# ---- CSS cho visual polish (scoped, minimal) ----
DASHBOARD_CSS = """
<style>
/* Reduce top padding */
.block-container {
    padding-top: 1.5rem !important;
}

/* KPI card styling */
.kpi-card {
    background: #fafafa;
    border: 1px solid #e8e8e8;
    border-radius: 10px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.5rem;
    transition: box-shadow 0.2s ease;
}
.kpi-card:hover {
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}
.kpi-label {
    font-size: 0.8rem;
    color: #666;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    margin-bottom: 0.25rem;
}
.kpi-value {
    font-size: 1.5rem;
    font-weight: 600;
    color: #1a1a1a;
    line-height: 1.3;
}
.kpi-icon {
    font-size: 1.2rem;
    margin-right: 0.5rem;
}

/* Section heading */
.section-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-top: 2rem;
    margin-bottom: 0.75rem;
}
.section-header h2 {
    margin: 0;
    font-size: 1.4rem;
    font-weight: 600;
    color: #1a1a1a;
}
.section-icon {
    font-size: 1.3rem;
}
.section-caption {
    color: #666;
    font-size: 0.9rem;
    margin-top: -0.25rem;
    margin-bottom: 1rem;
}

/* Insight callout cards */
.insight-card {
    background: #f8f9fa;
    border: 1px solid #e8e8e8;
    border-left: 4px solid #3b82f6;
    border-radius: 8px;
    padding: 0.85rem 1rem;
    margin-bottom: 0.75rem;
}
.insight-title {
    font-size: 0.8rem;
    font-weight: 600;
    color: #3b82f6;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 0.35rem;
}
.insight-content {
    font-size: 0.95rem;
    color: #333;
    line-height: 1.5;
}

/* Sidebar compact */
.sidebar-metric {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.35rem 0;
    font-size: 0.9rem;
}
.sidebar-metric-icon {
    font-size: 1.1rem;
}
.sidebar-divider {
    border-top: 1px solid #e8e8e8;
    margin: 0.75rem 0;
}

/* Detail table spacing */
.detail-count {
    font-size: 0.85rem;
    color: #666;
    margin-bottom: 0.5rem;
}

/* Chart containers */
.chart-container {
    background: transparent;
}

/* Divider */
.hr-subtle {
    border: none;
    border-top: 1px solid #e8e8e8;
    margin: 1.5rem 0;
}

/* Map container */
.map-container {
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid #e8e8e8;
}
</style>
"""

def inject_css():
    """Inject scoped dashboard CSS."""
    st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)


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


@st.cache_data
def load_dashboard_data():
    """Doc 4 processed snapshots (cached, read-only, khong mutate file)."""
    return (
        pd.read_csv(COMBINED_CSV),
        pd.read_csv(ROAD_SUMMARY_CSV),
        pd.read_csv(HOURLY_SUMMARY_CSV),
        pd.read_csv(WEATHER_SUMMARY_CSV),
    )


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
    if analyzed_road_names is not None and monitored_names:
        n_covered = len(set(monitored_names) & set(analyzed_road_names))
        st.sidebar.caption(
            f"{n_covered}/{len(monitored_names)} tuyến hiện có dữ liệu phân tích."
        )
    st.sidebar.caption(
        "Danh sách theo dõi (roads.csv) có thể khác danh sách đã có dữ "
        "liệu phân tích — đây là trạng thái hợp lệ."
    )

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


def render_data_update_sidebar():
    """Section sidebar nut cap nhat du lieu (Phase 9.2).

    Chi goi orchestrator public run_data_update(); khong goi truc tiep
    TomTom/Open-Meteo/collector/Spark. Pipeline chi chay khi user bam nut.
    """
    st.sidebar.markdown("---")
    st.sidebar.header("🔄 Cập nhật dữ liệu")
    _show_update_flash()
    st.sidebar.caption(
        "Thu thập dữ liệu giao thông và thời tiết mới cho các tuyến đang "
        "theo dõi, sau đó chạy Apache Spark để cập nhật dashboard. "
        "Cập nhật theo yêu cầu — quá trình có thể mất khoảng một phút."
    )
    if st.sidebar.button(
        "🔄 Cập nhật dữ liệu",
        key="update_data_button",
        use_container_width=True,
    ):
        _handle_data_update()


def main():
    st.set_page_config(
        page_title="Hanoi Traffic Analytics",
        page_icon="🚗",
        layout="wide",
    )

    inject_css()

    st.title("Phân tích giao thông Hà Nội")
    st.caption(
        "Quan trắc giao thông kết hợp thời tiết tại Hà Nội — "
        "xử lý bằng Apache Spark, dashboard trình bày kết quả trên dữ liệu đã thu thập."
    )

    # ---- Required file guard (khong tu tao artifact) ----
    missing = find_missing_files()
    if missing:
        st.error(
            "Thiếu file dữ liệu đã xử lý: " + ", ".join(missing) + ". "
            "Hãy chạy xử lý Phase 5/6 trước khi mở dashboard."
        )
        st.stop()

    combined_df, road_df, hourly_df, weather_df = load_dashboard_data()

    # ---- Schema guards ----
    problems = find_missing_columns(
        {
            "combined_data.csv": combined_df,
            "road_summary.csv": road_df,
            "hourly_summary.csv": hourly_df,
            "weather_summary.csv": weather_df,
        }
    )
    if problems:
        for name, cols in problems.items():
            st.error(f"File {name} thiếu cột: " + ", ".join(cols) + ".")
        st.stop()

    # ---- Empty guards ----
    if combined_df.empty:
        st.error("combined_data.csv không có dòng dữ liệu nào.")
        st.stop()
    for label, df in (
        ("road_summary.csv", road_df),
        ("hourly_summary.csv", hourly_df),
        ("weather_summary.csv", weather_df),
    ):
        if df.empty:
            st.warning(f"File {label} hiện trống — chưa có dữ liệu tổng hợp để hiển thị.")

    # ---- Timestamp presentation (copy in-memory, khong sua CSV) ----
    event_utc, n_invalid = parse_event_timestamps(combined_df)
    if n_invalid:
        st.error(
            f"Có {n_invalid} event_timestamp không parse được — "
            "kiểm tra lại Phase 5 thay vì bỏ qua ngầm."
        )
        st.stop()
    event_hanoi = to_hanoi(event_utc)
    latest_hanoi = event_hanoi.max()

    # ---- Sidebar: thong tin du lieu (read-only) ----
    st.sidebar.header("Thông tin dữ liệu")
    n_obs = len(combined_df)
    n_roads = int(road_df["road_name"].nunique()) if not road_df.empty else 0
    n_matched = int(matched_mask(combined_df).sum())
    st.sidebar.markdown(
        f"""
        <div class="sidebar-metric"><span class="sidebar-metric-icon">📊</span>Số quan trắc: {n_obs}</div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🛣️</span>Số tuyến: {n_roads}</div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🌦️</span>Ghép thời tiết: {n_matched}</div>
        <div class="sidebar-divider"></div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">📅</span>{event_hanoi.min().strftime('%Y-%m-%d %H:%M')} → {event_hanoi.max().strftime('%Y-%m-%d %H:%M')}</div>
        <div class="sidebar-divider"></div>
        <div class="sidebar-metric"><span class="sidebar-metric-icon">🕐</span>Cập nhật: {latest_hanoi.strftime('%Y-%m-%d %H:%M')} (giờ HN)</div>
        """,
        unsafe_allow_html=True,
    )

    # ---- Sidebar: quan ly tuyen duong (roads.csv, khong cham processed) ----
    render_road_management_sidebar(
        set(combined_df["road_name"].dropna().astype(str).tolist())
    )

    # ---- Sidebar: one-click data update (orchestrator, rerun de refresh) ----
    render_data_update_sidebar()

    # ---- Tong quan: 6 KPI tu snapshot (khong delta, khong live claim) ----
    st.markdown('<div class="section-header"><span class="section-icon">📈</span><h2>Tổng quan</h2></div>', unsafe_allow_html=True)
    try:
        metrics = calculate_overview_metrics(
            combined_df, road_df, hourly_df, weather_df, event_hanoi
        )
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    for warning in metrics["warnings"]:
        st.warning(warning)

    kpi_cards = [
        (KPI_ICONS["n_roads"], "Tuyến đường", str(metrics['n_roads'])),
        (KPI_ICONS["n_obs"], "Quan sát", str(metrics['n_obs'])),
        (KPI_ICONS["avg_speed"], "Tốc độ TB", f"{metrics['avg_speed']:.1f} km/h"),
        (KPI_ICONS["avg_congestion"], "Ùn tắc TB", f"{metrics['avg_congestion']:.1f}%"),
        (KPI_ICONS["matched_pct"], "Ghép thời tiết", f"{metrics['matched_pct']:.1f}%"),
        (KPI_ICONS["latest_hanoi"], "Dữ liệu mới nhất", metrics["latest_hanoi"].strftime("%d/%m/%Y %H:%M")),
    ]
    cols = st.columns(6)
    for i, (icon, label, value) in enumerate(kpi_cards):
        with cols[i]:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label"><span class="kpi-icon">{icon}</span>{label}</div>
                    <div class="kpi-value">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.caption("Các chỉ số được tính từ snapshot dữ liệu đã xử lý gần nhất.")

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

    st.markdown('<div class="section-header"><span class="section-icon">🗺️</span><h2>Bản đồ giao thông</h2></div>', unsafe_allow_html=True)
    latest_df = get_latest_road_observations(combined_df, event_utc)
    valid_df, invalid_roads = validate_map_coordinates(latest_df)
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
        st_folium(traffic_map, use_container_width=True, height=550, returned_objects=[])
        st.caption(
            "Mỗi marker là quan sát mới nhất của một tuyến trong snapshot đã xử lý; "
            "màu sắc thể hiện trạng thái ùn tắc của quan sát đó (không phải trực tiếp)."
        )

    st.markdown('<div class="section-header"><span class="section-icon">🛣️</span><h2>Phân tích theo tuyến đường</h2></div>', unsafe_allow_html=True)
    try:
        speed_df, congestion_df = prepare_road_chart_data(road_df)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    road_cols = st.columns(2)
    with road_cols[0]:
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
        st.plotly_chart(fig_speed, width="stretch")
    with road_cols[1]:
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
        st.plotly_chart(fig_cong, width="stretch")

    st.markdown('<div class="section-header"><span class="section-icon">⏰</span><h2>Phân tích theo thời gian</h2></div>', unsafe_allow_html=True)
    try:
        hourly_chart_df = prepare_hourly_chart_data(hourly_df)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
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
    st.plotly_chart(fig_hourly, width="stretch")
    st.caption("Chỉ hiển thị các giờ có quan sát trong snapshot dữ liệu.")

    st.markdown('<div class="section-header"><span class="section-icon">🌦️</span><h2>Giao thông và thời tiết</h2></div>', unsafe_allow_html=True)
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
        st.plotly_chart(fig_wcong, width="stretch")
    st.caption(
        "Điều kiện thời tiết trong dữ liệu: "
        + " • ".join(
            f"{row['weather_condition']}: {int(row['observation_count'])} quan sát"
            for _, row in weather_chart_df.iterrows()
        )
        + "."
    )
    st.caption(
        "Kết quả chỉ mô tả các quan sát đã thu thập, không chứng minh "
        "quan hệ nhân quả giữa thời tiết và giao thông."
    )

    st.markdown('<div class="section-header"><span class="section-icon">📋</span><h2>Chi tiết quan sát</h2></div>', unsafe_allow_html=True)
    st.caption(
        "Bộ lọc bên dưới chỉ áp dụng cho bảng chi tiết, không thay đổi "
        "các KPI, bản đồ hoặc biểu đồ tổng hợp phía trên."
    )
    roads_options, levels_options = available_filter_options(combined_df)
    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_roads = st.multiselect(
            "Tuyến đường", options=roads_options, default=roads_options
        )
    with filter_cols[1]:
        selected_levels = st.multiselect(
            "Trạng thái giao thông", options=levels_options, default=levels_options
        )
    try:
        filtered_df = filter_detail_observations(
            combined_df, selected_roads, selected_levels
        )
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    st.write(f"Hiển thị {len(filtered_df)} / {len(combined_df)} quan sát")
    if filtered_df.empty:
        st.write("Không có quan sát phù hợp với bộ lọc hiện tại.")
    else:
        detail_df = prepare_detail_table(filtered_df, event_hanoi)
        st.dataframe(detail_df, hide_index=True, width="stretch")


if __name__ == "__main__":
    main()
