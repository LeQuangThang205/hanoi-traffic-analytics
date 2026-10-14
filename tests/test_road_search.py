"""Test tim kiem + them tuyen duong Ha Noi (mock/read-only).

KHONG goi Nominatim that, KHONG goi TomTom/Open-Meteo, KHONG chay
Spark/collector production, KHONG sua production CSV. Geocoding duoc
mock qua http_get injectable; road_manager/collector chay tren CSV tam.

Chay:  python tests/test_road_search.py
"""

import csv
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app
import collector
from utils import geocoding
from utils import road_manager

geocoding.MIN_INTERVAL_SECONDS = 0  # test khong sleep rate-limit

CHECKS = []


def check(name, fn):
    try:
        fn()
        CHECKS.append((name, True, ""))
    except Exception as exc:  # noqa: BLE001 - bao loi day du
        CHECKS.append((name, False, f"{type(exc).__name__}: {exc}"))


def _hanoi_road(name, lat="21.0156", lon="105.8144", cls="highway"):
    return {
        "name": name,
        "class": cls,
        "lat": lat,
        "lon": lon,
        "display_name": f"{name}, Quận Đống Đa, Hà Nội, Việt Nam",
        "address": {"road": name, "city": "Hà Nội", "country": "Việt Nam"},
    }


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def t_display_names_12():
    expected = {
        "Lang Ha": "Láng Hạ",
        "Nguyen Chi Thanh": "Nguyễn Chí Thanh",
        "Tran Duy Hung": "Trần Duy Hưng",
        "Le Van Luong": "Lê Văn Lương",
        "Pham Hung": "Phạm Hùng",
        "Nguyen Trai": "Nguyễn Trãi",
        "Truong Chinh": "Trường Chinh",
        "Giai Phong": "Giải Phóng",
        "Cau Giay": "Cầu Giấy",
        "Xuan Thuy": "Xuân Thủy",
        "Dai Co Viet": "Đại Cồ Việt",
        "Kim Ma": "Kim Mã",
    }
    for raw, want in expected.items():
        assert app.display_road_name(raw) == want, raw


def t_diacritics_loose_match():
    assert app.strip_diacritics("Láng Hạ") == "lang ha"
    assert app.names_match_loose("Láng Hạ", "Lang Ha")
    assert app.names_match_loose("Đường Láng Hạ", "lang ha")
    assert app.names_match_loose("Phố Nguyễn Chí Thanh", "Nguyen Chi Thanh")
    assert not app.names_match_loose("Láng Hạ", "Lê Văn Lương")
    assert not app.names_match_loose("", "Lang Ha")


def t_parse_keeps_hanoi_roads():
    raw = [
        _hanoi_road("Láng Hạ"),
        _hanoi_road("Láng Hạ", lat="21.0160", lon="105.8150"),  # trung ten giu ca 2
        dict(_hanoi_road("cephalopod"), **{"class": "amenity"}),  # khong phai duong
    ]
    out = geocoding.parse_search_results(raw)
    assert len(out) == 2
    assert out[0]["name"] == "Láng Hạ"  # ten co dau giu nguyen
    assert out[0]["lat"] == 21.0156 and out[0]["lon"] == 105.8144


def t_parse_drops_outside_hanoi():
    outside = _hanoi_road("Láng Hạ", lat="10.7626", lon="106.6602")
    outside["address"] = {"road": "Láng Hạ", "city": "Hồ Chí Minh"}
    outside["display_name"] = "Láng Hạ, Hồ Chí Minh, Việt Nam"
    assert geocoding.parse_search_results([outside]) == []


def t_parse_drops_bad_coords():
    assert geocoding.parse_search_results(
        [_hanoi_road("Láng Hạ", lat="abc", lon="105.8")]) == []
    assert geocoding.parse_search_results(
        [_hanoi_road("Láng Hạ", lat="95.0", lon="105.8")]) == []
    assert geocoding.parse_search_results([_hanoi_road("  ")]) == []


def t_search_success_and_cache():
    geocoding.clear_cache()
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append((url, params, headers))
        return _FakeResp([_hanoi_road("Trần Duy Hưng")])

    out = geocoding.search_roads_in_hanoi("tran duy hung", http_get=fake_get)
    assert len(out) == 1 and out[0]["name"] == "Trần Duy Hưng"
    url, params, headers = calls[0]
    assert "nominatim.openstreetmap.org" in url
    assert params["bounded"] == 1 and "viewbox" in params
    assert "User-Agent" in headers and headers["User-Agent"]
    out2 = geocoding.search_roads_in_hanoi("Trần Duy Hưng", http_get=fake_get)
    assert out2 == out and len(calls) == 1  # cache, khong goi lai
    geocoding.clear_cache()


def t_search_errors():
    try:
        geocoding.search_roads_in_hanoi("   ", http_get=lambda *a, **k: None)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass

    def boom(*a, **k):
        raise ConnectionError("net down")

    try:
        geocoding.search_roads_in_hanoi("lang ha", http_get=boom)
        raise AssertionError("expected GeocodingError")
    except geocoding.GeocodingError:
        pass

    def weird(*a, **k):
        return _FakeResp({"error": "bad"})

    try:
        geocoding.search_roads_in_hanoi("lang ha", http_get=weird)
        raise AssertionError("expected GeocodingError")
    except geocoding.GeocodingError:
        pass


def _tmp_roads(names):
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8")
    writer = csv.writer(tmp)
    writer.writerow(["road_name", "lat", "lon"])
    for i, n in enumerate(names):
        writer.writerow([n, 21.0 + i * 0.001, 105.8])
    tmp.close()
    return Path(tmp.name)


def t_add_accented_and_duplicates():
    path = _tmp_roads(["Lang Ha"])
    try:
        road_manager.add_road("Láng Hạ", 21.0156, 105.8144, path=path)
        names = [r["road_name"] for r in road_manager.load_roads(path)]
        assert "Láng Hạ" in names and "Lang Ha" in names  # lich su giu nguyen
        try:
            road_manager.add_road("láng hạ", 21.0, 105.8, path=path)
            raise AssertionError("expected DuplicateRoadError")
        except road_manager.DuplicateRoadError:
            pass
        # Trung gan dung bo dau duoc phat hien o tang UI
        assert app.names_match_loose("Đường Láng Hạ", "Lang Ha")
        updated = road_manager.remove_road("Láng Hạ", path=path)
        assert [r["road_name"] for r in updated] == ["Lang Ha"]
    finally:
        path.unlink(missing_ok=True)


def t_collector_reads_new_road():
    path = _tmp_roads(["Lang Ha", "Trần Duy Hưng"])
    try:
        roads = collector.load_roads(path)
        by_name = {r["road_name"]: r for r in roads}
        assert by_name["Trần Duy Hưng"]["lat"] == 21.001
        assert by_name["Lang Ha"]["lon"] == 105.8
    finally:
        path.unlink(missing_ok=True)


def t_short_address():
    assert app._short_address("a" * 200).endswith("…")
    assert app._short_address("Ngắn") == "Ngắn"


def main():
    check("ten-12-tuyen-co-dau", t_display_names_12)
    check("trung-gan-dung-bo-dau", t_diacritics_loose_match)
    check("giua-duong-ha-noi", t_parse_keeps_hanoi_roads)
    check("loai-ngoai-ha-noi", t_parse_drops_outside_hanoi)
    check("loai-toa-do-sai", t_parse_drops_bad_coords)
    check("tim-kiem-va-cache", t_search_success_and_cache)
    check("loi-tim-kiem", t_search_errors)
    check("them-trung-xoa-tuyen", t_add_accented_and_duplicates)
    check("collector-doc-tuyen-moi", t_collector_reads_new_road)
    check("rut-gon-dia-chi", t_short_address)
    failed = [c for c in CHECKS if not c[1]]
    for name, ok, err in CHECKS:
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else " -> " + err))
    print(f"{len(CHECKS) - len(failed)}/{len(CHECKS)} checks PASS")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
