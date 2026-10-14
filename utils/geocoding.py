"""Tim kiem tuyen duong Ha Noi qua Nominatim (OpenStreetMap) cong khai.

Quy tac su dung (tuan thu chinh sach Nominatim Usage Policy):
- CHI goi khi nguoi dung nhan nut "Tim tuyen duong" (khong autocomplete
  theo tung ky tu, khong goi khi render/filter dashboard).
- User-Agent dinh danh day du; gioi han toc do toi thieu 1 giay/call
  (rate limit phia client).
- Ket qua cache trong process (toi da 100 query) de tranh goi lap.
- Khong API key, khong database, khong sua roads.csv o day.
- Toa do KHONG tu suy doan: chi dung lat/lon Nominatim tra ve (lam tron
  4 decimals nhu catalog hien tai).

Gioi han Ha Noi: viewbox + bounded=1 phia server, dong thoi loc lai
phia client theo address (Ha Noi/Hanoi) hoac bbox du phong.
"""

import time
import unicodedata

import requests

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "HanoiTrafficAnalytics/1.0 (university course project)"
REQUEST_TIMEOUT_SECONDS = 10
MIN_INTERVAL_SECONDS = 1.1
RESULT_LIMIT = 8

# Bbox du phong Ha Noi (lon/lat): bao ca ngoai thanh.
HANOI_VIEWBOX = (105.15, 20.55, 106.05, 21.65)
HANOI_LAT_RANGE = (20.55, 21.65)
HANOI_LON_RANGE = (105.15, 106.05)

# Chi nhan dia diem la duong (tranh them truong hoc, cong vien...).
ROAD_CLASSES = {"highway"}

_last_call_ts = 0.0
_SEARCH_CACHE = {}
CACHE_MAX_ENTRIES = 100


class GeocodingError(Exception):
    """Loi geocoding (mang/timeout/API) — UI hien thong bao than thien."""


def _throttle():
    """Dam bao khoang cach toi thieu giua 2 request (client-side)."""
    global _last_call_ts
    now = time.monotonic()
    wait = MIN_INTERVAL_SECONDS - (now - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.monotonic()


def _cache_key(query):
    """Khoa cache: bo dau + chu thuong + gon khoang trang (pure)."""
    normalized = unicodedata.normalize("NFD", query.strip())
    stripped = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    key = stripped.replace("đ", "d").replace("Đ", "d").lower()
    return " ".join(key.split())


def _cache_get(query):
    return _SEARCH_CACHE.get(_cache_key(query))


def _cache_put(query, results):
    if len(_SEARCH_CACHE) >= CACHE_MAX_ENTRIES:
        _SEARCH_CACHE.pop(next(iter(_SEARCH_CACHE)))
    _SEARCH_CACHE[_cache_key(query)] = list(results)


def clear_cache():
    """Xoa cache process (chu yeu cho test)."""
    _SEARCH_CACHE.clear()


def _is_hanoi(item):
    """True neu ket qua thuoc Ha Noi (address hoac bbox du phong)."""
    address = item.get("address") or {}
    haystack = " ".join(str(v) for v in address.values()).lower()
    if "hà nội" in haystack or "hanoi" in haystack:
        return True
    try:
        lat = float(item.get("lat"))
        lon = float(item.get("lon"))
    except (TypeError, ValueError):
        return False
    return (HANOI_LAT_RANGE[0] <= lat <= HANOI_LAT_RANGE[1]
            and HANOI_LON_RANGE[0] <= lon <= HANOI_LON_RANGE[1])


def _is_road(item):
    """True neu ket qua la duong (class=highway)."""
    return str(item.get("class") or "").strip().lower() in ROAD_CLASSES


def _valid_coords(item):
    """(lat, lon) lam tron 4 decimals, hoac None neu khong hop le."""
    try:
        lat = round(float(item.get("lat")), 4)
        lon = round(float(item.get("lon")), 4)
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return lat, lon


def parse_search_results(raw_items):
    """Loc + chuan hoa ket qua Nominatim tho (pure, khong IO).

    Giu ket qua: la duong + thuoc Ha Noi + toa do hop le.
    Tra ve list dict {name, display_address, lat, lon}.
    `name`: ten co dau tu Nominatim (nguyen van, strip).
    """
    results = []
    for item in raw_items or []:
        if not isinstance(item, dict):
            continue
        if not _is_road(item):
            continue
        if not _is_hanoi(item):
            continue
        coords = _valid_coords(item)
        if coords is None:
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        lat, lon = coords
        results.append({
            "name": name,
            "display_address": str(item.get("display_name") or "").strip(),
            "lat": lat,
            "lon": lon,
        })
    return results


def search_roads_in_hanoi(query, http_get=None):
    """Tim tuyen duong Ha Noi theo ten (coMang, co cache).

    Args:
        query: ten duong tieng Viet (co dau hoac khong).
        http_get: ham GET de test mock (mac dinh requests.get).

    Returns:
        List dict {name, display_address, lat, lon} da loc Ha Noi/duong.

    Raises:
        ValueError: query rong.
        GeocodingError: loi mang/timeout/API.
    """
    if not isinstance(query, str) or not query.strip():
        raise ValueError("Nhập tên tuyến đường cần tìm.")
    cached = _cache_get(query)
    if cached is not None:
        return [dict(r) for r in cached]

    get = http_get if http_get is not None else requests.get
    params = {
        "q": f"{query.strip()}, Hà Nội, Việt Nam",
        "format": "json",
        "addressdetails": 1,
        "limit": RESULT_LIMIT,
        "viewbox": ",".join(map(str, HANOI_VIEWBOX)),
        "bounded": 1,
        "accept-language": "vi",
    }
    headers = {"User-Agent": USER_AGENT}
    _throttle()
    try:
        resp = get(NOMINATIM_URL, params=params, headers=headers,
                   timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        raw = resp.json()
    except GeocodingError:
        raise
    except Exception as exc:
        raise GeocodingError(
            "Không kết nối được dịch vụ tìm kiếm. Vui lòng thử lại sau."
        ) from exc
    if not isinstance(raw, list):
        raise GeocodingError("Dịch vụ tìm kiếm trả về dữ liệu không hợp lệ.")
    results = parse_search_results(raw)
    _cache_put(query, results)
    return [dict(r) for r in results]
