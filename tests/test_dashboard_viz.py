"""Test nho cho dashboard visualization helpers (mock/readonly).

KHONG goi TomTom/Open-Meteo, KHONG chay Spark/collector, KHONG sua
production CSV. Du lieu fixture tao trong bo nho; chi import pure helpers
tu app.py (import app khong co side-effect: khong API/Spark/append).

Chay:  python tests/test_dashboard_viz.py
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app


def _fixture():
    """Fixture 6 quan sat: 2 tuyen x 2 gio x ngay, co 1 ban ghi unmatched."""
    rows = [
        # Nguyen Trai, gio 8
        dict(timestamp="2026-10-06T01:00:00+00:00", event_timestamp="2026-10-06T01:00:00+00:00",
             road_name="Nguyen Trai", lat=20.9983, lon=105.7929,
             current_speed=20.0, free_flow_speed=40.0, congestion_percent=50.0,
             congestion_level="Ùn tắc", local_hour=8,
             weather_matched="true", temperature=30.0, humidity=70.0,
             precipitation=0.0, rain=0.0, wind_speed=3.0, weather_code=3),
        # Nguyen Trai, gio 9, co mua
        dict(timestamp="2026-10-06T02:00:00+00:00", event_timestamp="2026-10-06T02:00:00+00:00",
             road_name="Nguyen Trai", lat=20.9983, lon=105.7929,
             current_speed=30.0, free_flow_speed=40.0, congestion_percent=25.0,
             congestion_level="Đông", local_hour=9,
             weather_matched="true", temperature=28.0, humidity=85.0,
             precipitation=1.0, rain=2.5, wind_speed=5.0, weather_code=51),
        # Cau Giay, gio 8, thong thoang
        dict(timestamp="2026-10-06T01:05:00+00:00", event_timestamp="2026-10-06T01:05:00+00:00",
             road_name="Cau Giay", lat=21.0285, lon=105.8003,
             current_speed=38.0, free_flow_speed=40.0, congestion_percent=5.0,
             congestion_level="Thông thoáng", local_hour=8,
             weather_matched="true", temperature=31.0, humidity=65.0,
             precipitation=0.0, rain=0.0, wind_speed=2.0, weather_code=1),
        # Cau Giay, gio 9, unmatched (thoi tiet thieu -> "Khong co du lieu")
        dict(timestamp="2026-10-06T02:05:00+00:00", event_timestamp="2026-10-06T02:05:00+00:00",
             road_name="Cau Giay", lat=21.0285, lon=105.8003,
             current_speed=10.0, free_flow_speed=40.0, congestion_percent=75.0,
             congestion_level="Ùn tắc nghiêm trọng", local_hour=9,
             weather_matched="false", temperature=float("nan"), humidity=float("nan"),
             precipitation=float("nan"), rain=float("nan"), wind_speed=float("nan"),
             weather_code=float("nan")),
        # Ngay khac de test loc ngay
        dict(timestamp="2026-10-05T01:00:00+00:00", event_timestamp="2026-10-05T01:00:00+00:00",
             road_name="Nguyen Trai", lat=20.9983, lon=105.7929,
             current_speed=22.0, free_flow_speed=40.0, congestion_percent=45.0,
             congestion_level="Ùn tắc", local_hour=8,
             weather_matched="true", temperature=29.0, humidity=75.0,
             precipitation=0.0, rain=0.0, wind_speed=4.0, weather_code=3),
        dict(timestamp="2026-10-05T01:05:00+00:00", event_timestamp="2026-10-05T01:05:00+00:00",
             road_name="Cau Giay", lat=21.0285, lon=105.8003,
             current_speed=36.0, free_flow_speed=40.0, congestion_percent=10.0,
             congestion_level="Thông thoáng", local_hour=8,
             weather_matched="true", temperature=30.5, humidity=68.0,
             precipitation=0.0, rain=0.0, wind_speed=3.5, weather_code=0),
    ]
    df = pd.DataFrame(rows)
    utc = pd.to_datetime(df["event_timestamp"], utc=True)
    hanoi = utc.dt.tz_convert(app.HANOI_TZ)
    return df, utc, hanoi


def _road_summary():
    return pd.DataFrame([
        dict(road_name="Nguyen Trai", observation_count=3, avg_speed=24.0,
             avg_congestion_percent=40.0),
        dict(road_name="Cau Giay", observation_count=3, avg_speed=28.0,
             avg_congestion_percent=30.0),
    ])


CHECKS = []


def check(name, fn):
    try:
        fn()
        CHECKS.append((name, True, ""))
    except Exception as exc:  # noqa: BLE001 - bao loi day du theo Rule 12
        CHECKS.append((name, False, f"{type(exc).__name__}: {exc}"))


def t_display_names():
    assert app.display_road_name("Nguyen Trai") == "Nguyễn Trãi"
    assert app.display_road_name("Truong Chinh") == "Trường Chinh"
    assert app.display_road_name("Giai Phong") == "Giải Phóng"
    assert app.display_road_name("Cau Giay") == "Cầu Giấy"
    assert app.display_road_name("Xuan Thuy") == "Xuân Thủy"
    assert app.display_road_name("Dai Co Viet") == "Đại Cồ Việt"
    assert app.display_road_name("Kim Ma") == "Kim Mã"
    assert app.display_road_name("Tuyen La") == "Tuyen La"


def t_format_vi():
    assert app.fmt_1(24.2) == "24,2"
    assert app.fmt_pct(36.9) == "36,9%"
    assert app.fmt_int(1351) == "1.351"
    assert app.fmt_int(1358) == "1.358"


def t_congestion_hex_thresholds():
    assert app.congestion_hex_for_value(19.9) == "#22C55E"
    assert app.congestion_hex_for_value(20.0) == "#EAB308"
    assert app.congestion_hex_for_value(40.0) == "#EAB308"
    assert app.congestion_hex_for_value(40.1) == "#F97316"
    assert app.congestion_hex_for_value(60.0) == "#F97316"
    assert app.congestion_hex_for_value(60.1) == "#EF4444"


def t_data_status():
    now = pd.Timestamp.now(tz=app.HANOI_TZ)
    key, label, _ = app.describe_data_status(now, False)
    assert key == "ready" and "sẵn sàng" in label
    key, label, _ = app.describe_data_status(now, True)
    assert key == "syncing" and "đồng bộ" in label
    old = now - pd.Timedelta(hours=5)
    key, label, _ = app.describe_data_status(old, False)
    assert key == "stale" and "cũ" in label


def t_filter_roads_dates():
    df, _, hanoi = _fixture()
    out = app.filter_combined_by_roads_dates(
        df, hanoi, ["Cau Giay"], [date(2026, 10, 6)])
    assert len(out) == 2 and set(out["road_name"]) == {"Cau Giay"}
    try:
        app.filter_combined_by_roads_dates(df, hanoi, ["Tuyen La"], [date(2026, 10, 6)])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    try:
        app.filter_combined_by_roads_dates(df, hanoi, ["Cau Giay"], [date(2020, 1, 1)])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    try:
        app.filter_combined_by_roads_dates(df, hanoi, [], [date(2026, 10, 6)])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def t_hourly_speed_gaps():
    df, _, hanoi = _fixture()
    work = app.filter_combined_by_roads_dates(
        df, hanoi, ["Nguyen Trai", "Cau Giay"], [date(2026, 10, 6)])
    out = app.prepare_hourly_speed_filtered(work)
    # Gio 8: (20+38)/2=29 ; gio 9: (30+10)/2=20 ; gio thieu khong noi suy
    assert out["local_hour"].tolist() == [8, 9]
    assert abs(out.loc[out.local_hour == 8, "avg_speed"].iloc[0] - 29.0) < 1e-9
    assert out.loc[out.local_hour == 8, "n_obs"].iloc[0] == 2


def t_road_congestion_sorted_vn():
    out = app.prepare_road_congestion_filtered(
        _road_summary(), ["Nguyen Trai", "Cau Giay"])
    assert out["road_name"].tolist() == ["Nguyen Trai", "Cau Giay"]  # 40 > 30
    assert out["display_name"].tolist() == ["Nguyễn Trãi", "Cầu Giấy"]
    assert out["color"].tolist() == ["#EAB308", "#EAB308"]


def t_heatmap_blank_not_zero():
    df, _, hanoi = _fixture()
    work = app.filter_combined_by_roads_dates(
        df, hanoi, ["Nguyen Trai"], [date(2026, 10, 6)])
    heat = app.prepare_congestion_heatmap_data(work)
    assert heat["hours"] == [8, 9]
    assert heat["display_roads"] == ["Nguyễn Trãi"]
    import math
    assert heat["z"][0][0] == 50.0 and heat["z"][0][1] == 25.0
    # O thieu du lieu phai NaN (trong), khong phai 0
    df2 = work.iloc[[0]].copy()
    heat2 = app.prepare_congestion_heatmap_data(df2)
    assert heat2["hours"] == [8]
    assert math.isnan(heat2["z"][0][0]) is False
    try:
        app.prepare_congestion_heatmap_data(work.iloc[0:0].copy())
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def t_trend_min_points():
    df, _, hanoi = _fixture()
    one = df.iloc[[0]].copy()
    try:
        app.prepare_trend_data(one, hanoi)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    points, skipped = app.prepare_trend_data(df, hanoi)
    assert skipped == []  # moi tuyen 3 diem
    assert len(points) == 6


def t_distribution_sums_100():
    df, _, hanoi = _fixture()
    work = app.filter_combined_by_roads_dates(
        df, hanoi, ["Nguyen Trai", "Cau Giay"], [date(2026, 10, 6)])
    dist = app.prepare_status_distribution(work)
    for _, row in dist.iterrows():
        total = sum(row[lv] for lv in app.LEVEL_SEMANTIC_ORDER)
        assert abs(total - 100.0) < 0.01, total
    nt = dist[dist.road_name == "Nguyen Trai"].iloc[0]
    assert abs(nt["Ùn tắc"] - 50.0) < 1e-9 and abs(nt["Đông"] - 50.0) < 1e-9


def t_weekday_missing_days():
    df, _, hanoi = _fixture()
    out, missing = app.prepare_weekday_comparison(df, hanoi)
    assert len(out) == 2  # T2 (5/10) + T3 (6/10/2026)
    assert out["label"].tolist() == ["Thứ Hai", "Thứ Ba"]
    assert "Thứ Bảy" in missing and len(missing) == 5


def t_rain_matched_only():
    df, _, hanoi = _fixture()
    work = app.filter_combined_by_roads_dates(
        df, hanoi, ["Nguyen Trai", "Cau Giay"], [date(2026, 10, 6)])
    points, rain_pos, _ = app.prepare_rain_scatter(work, hanoi)
    assert len(points) == 3  # loai ban ghi unmatched
    assert rain_pos == 1
    assert set(points["road_name"]) <= {"Nguyen Trai", "Cau Giay"}


def t_temp_scatter_no_regression():
    df, _, hanoi = _fixture()
    points = app.prepare_temp_scatter(df, hanoi)
    assert len(points) == 5  # loai 1 ban ghi thieu nhiet do
    assert {"temperature", "speed", "road_name", "observed"} <= set(points.columns)


def t_no_hardcoded_numbers_in_helpers():
    import inspect
    src = inspect.getsource(app.prepare_status_distribution)
    assert "100.0" in src  # ty le phan tram tu du lieu, khong so lieu cung


def t_map_dark_tiles():
    # Gioi han da xac minh: folium 0.20 yeu cau API key cho tile CartoDB
    # -> fallback OpenStreetMap mac dinh (khong key, attribution tu dong).
    df, utc, hanoi = _fixture()
    latest = app.get_latest_road_observations(df, utc)
    valid, invalid = app.validate_map_coordinates(latest)
    assert invalid == []
    fmap, count, _ = app.build_traffic_map(valid, hanoi)
    html = fmap.get_root().render()
    assert "openstreetmap" in html.lower()
    assert "cartocdn" not in html.lower()  # khong lach key bang URL custom
    assert "Mức ùn tắc" in html  # legend tieng Viet


def t_resolve_all_or_selected():
    assert app.resolve_all_or_selected([], ["a", "b"]) == ["a", "b"]
    assert app.resolve_all_or_selected(["b"], ["a", "b"]) == ["b"]
    assert app.resolve_all_or_selected(["b", "a"], ["a", "b"]) == ["b", "a"]
    out = app.resolve_all_or_selected([], ["a"])
    out.append("x")
    assert app.resolve_all_or_selected([], ["a"]) == ["a"]  # khong mutate input


def t_no_banned_tech_phrases_in_ui():
    import re
    src = open("app.py", encoding="utf-8").read()
    ui_pat = re.compile(r"st\.(error|warning|info|success|write|caption|button|"
                        r"selectbox|multiselect|download_button|header|subheader|"
                        r"markdown|expander|tabs)\b")
    banned = ["Phase", "snapshot", "pointer", "pipeline", "collector",
              "Parquet", "cache", "rerun", "version", "API key", "api key",
              "nội suy", "thời gian thực", "mô tả dữ liệu"]
    allowed = "Nguồn dữ liệu"
    hits = []
    for i, line in enumerate(src.splitlines(), 1):
        if ui_pat.search(line.strip()):
            low = line.lower()
            for b in banned:
                if b.lower() in low and allowed not in line:
                    hits.append((i, b))
    assert not hits, hits


def main():
    check("ten-tuyen-co-dau", t_display_names)
    check("dinh-dang-so-viet", t_format_vi)
    check("mau-nguong-u-tac", t_congestion_hex_thresholds)
    check("trang-thai-du-lieu", t_data_status)
    check("loc-tuyen-ngay", t_filter_roads_dates)
    check("toc-do-theo-gio-khong-noi-suy", t_hourly_speed_gaps)
    check("un-tac-tuyen-sap-xep-tv", t_road_congestion_sorted_vn)
    check("heatmap-o-trong-khong-0", t_heatmap_blank_not_zero)
    check("xu-huong-du-lieu-it", t_trend_min_points)
    check("phan-bo-tong-100", t_distribution_sums_100)
    check("ngay-tuan-thieu-ngay", t_weekday_missing_days)
    check("mua-chi-matched", t_rain_matched_only)
    check("nhiet-do-khong-hoi-quy", t_temp_scatter_no_regression)
    check("khong-hardcode", t_no_hardcoded_numbers_in_helpers)
    check("loc-rong-nghia-la-tat-ca", t_resolve_all_or_selected)
    check("khong-chu-thich-ky-thuat", t_no_banned_tech_phrases_in_ui)
    check("ban-do-nen-toi", t_map_dark_tiles)
    failed = [c for c in CHECKS if not c[1]]
    for name, ok, err in CHECKS:
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else " -> " + err))
    print(f"{len(CHECKS) - len(failed)}/{len(CHECKS)} checks PASS")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
