"""Phase 4.4 - Controlled historical collection scheduler (MVP).

- Pure scheduling helpers dung cadence constants tu config.py
  (KHONG hard-code 15 / 06:00 / 10:00 / 16:00 / 20:00 o logic).
- Timezone-aware datetimes, Asia/Ho_Chi_Minh (zoneinfo, khong +7 tay).
- Slot semantics: start inclusive, end exclusive
  (06:00..09:45, 16:00..19:45; khong co 10:00/20:00).
- Runner co max_cycles bat buoc (> 0); KHONG infinite/daemon mode.
- Missed slots bi skip (khong catch-up/backfill).
- Fatal cycle exception: ghi error an toan, count attempt, tiep tuc slot
  tiep theo (khong retry ngay, khong fake summary).
- KHONG CLI that, KHONG sleep that trong tests (inject now/sleep/cycle).
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config import (
    COLLECTION_INTERVAL_MINUTES,
    COLLECTION_TIMEZONE,
    COLLECTION_WINDOWS,
)


def _hanoi_now():
    """Default clock: current time (timezone-aware)."""
    return datetime.now(ZoneInfo(COLLECTION_TIMEZONE))


def _to_collection_tz(dt):
    """Convert ve COLLECTION_TIMEZONE; reject naive datetime."""
    if not isinstance(dt, datetime):
        raise ValueError(f"Can datetime, nhan: {dt!r}.")
    if dt.tzinfo is None:
        raise ValueError(f"Naive datetime khong duoc chap nhan: {dt!r}.")
    return dt.astimezone(ZoneInfo(COLLECTION_TIMEZONE))


def _parse_hhmm(value):
    """Parse 'HH:MM' -> (hour, minute); sai format -> ValueError."""
    try:
        parsed = datetime.strptime(value, "%H:%M")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Window time khong hop le: {value!r}.") from exc
    return parsed.hour, parsed.minute


def _day_slots(day):
    """Tat ca scheduled slots (aware) cua 1 ngay (date), end exclusive."""
    interval = COLLECTION_INTERVAL_MINUTES
    slots = []
    for start, end in COLLECTION_WINDOWS:
        sh, sm = _parse_hhmm(start)
        eh, em = _parse_hhmm(end)
        cur = day.replace(hour=sh, minute=sm, second=0, microsecond=0)
        stop = day.replace(hour=eh, minute=em, second=0, microsecond=0)
        while cur < stop:
            slots.append(cur)
            cur += timedelta(minutes=interval)
    return slots


def _candidate_slots(anchor):
    """Slots cua hom nay + ngay mai (du cover gap toi -> sang)."""
    local = _to_collection_tz(anchor)
    base = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return _day_slots(base) + _day_slots(base + timedelta(days=1))


def is_collection_time(dt):
    """True neu dt trung KHOP 1 scheduled slot (exact, tinh theo Hanoi)."""
    local = _to_collection_tz(dt)
    if local.second != 0 or local.microsecond != 0:
        return False
    day = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return local in _day_slots(day)


def next_collection_time(now):
    """Slot Gan nhat >= now (documented >= semantics).

    Vd: 06:00 -> 06:00; 06:01 -> 06:15; 19:46 -> hom sau 06:00.
    """
    local = _to_collection_tz(now)
    for slot in _candidate_slots(local):
        if slot >= local:
            return slot
    raise RuntimeError("Khong tim duoc slot ke tiep (config rong?).")


def _next_strict(after_dt, now_dt):
    """Slot nho nhat vua > after_dt vua >= now_dt.

    Dung sau moi cycle de tranh duplicate slot + skip slot da lo
    (missed slots are skipped, khong catch-up/backfill).
    """
    after = _to_collection_tz(after_dt)
    now = _to_collection_tz(now_dt)
    for slot in _candidate_slots(max(after, now)):
        if slot > after and slot >= now:
            return slot
    raise RuntimeError("Khong tim duoc slot ke tiep (config rong?).")


def run_historical_collection(max_cycles, now_fn=None, sleep_fn=None,
                              cycle_fn=None):
    """Chay toi da max_cycles collection cycles theo cadence (Phase 4.4).

    - max_cycles bat buoc > 0 (int); KHONG infinite mode.
    - now_fn/sleep_fn/cycle_fn inject duoc cho tests; defaults production:
      clock Hanoi hien tai / time.sleep / collector.run_collection_cycle.
    - Moi slot goi cycle_fn() dung 1 lan; sleep dung wait (khong busy-wait).
    - Per-road failures nam trong summary (cycle coi nhu da thuc hien).
    - Fatal exception tu cycle_fn: ghi error, count attempt, sang slot tiep.
    - Tra ve list [{"scheduled_time": ISO aware, "summary"|"error": ...}].
    """
    if isinstance(max_cycles, bool) or not isinstance(max_cycles, int):
        raise ValueError(f"max_cycles phai la int > 0: {max_cycles!r}.")
    if max_cycles <= 0:
        raise ValueError(f"max_cycles phai > 0: {max_cycles!r}.")

    import time as _time

    from collector import run_collection_cycle as _default_cycle

    now_fn = now_fn or _hanoi_now
    sleep_fn = sleep_fn or _time.sleep
    cycle_fn = cycle_fn or _default_cycle

    results = []
    target = next_collection_time(now_fn())
    completed = 0
    while completed < max_cycles:
        wait = (target - _to_collection_tz(now_fn())).total_seconds()
        if wait > 0:
            sleep_fn(wait)
        try:
            summary = cycle_fn()
            results.append({"scheduled_time": target.isoformat(),
                            "summary": summary})
        except Exception as exc:
            results.append({"scheduled_time": target.isoformat(),
                            "error": str(exc)})
        completed += 1
        if completed >= max_cycles:
            break
        target = _next_strict(target, now_fn())
    return results


# --- Phase 10.4: Real Data Collection Campaign layer (orchestration only) ---
# Chi lap lich + goi collector.run_collection_cycle(); KHONG duplicate
# TomTom/Open-Meteo logic. Campaign chi API -> raw CSV (KHONG Spark moi slot;
# Spark chay batch/on-demand: cuoi ngay / khi can / truoc ML training).
# Resume: append-only, khong truncate; restart dung cung command, tiep tuc
# slot tuong lai (>= now, strict-after slot cuoi), missed slots khong backfill.


def _count_data_rows(path):
    """Dem data rows (tru header) bang stdlib csv; missing/empty -> 0."""
    import csv

    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return 0
            return sum(1 for _ in reader)
    except FileNotFoundError:
        return 0


def campaign_progress(traffic_path=None, weather_path=None):
    """Read-only campaign progress (stdlib csv, KHONG Pandas/Spark/API).

    Tra ve dict: traffic_rows, weather_rows, date_min/max (traffic ISO),
    days (so ngay calendar Hanoi), roads (Counter dict), local_hours
    (sorted), weekday/weekend counts (traffic, theo Hanoi), rain_yes/no
    (weather rain > 0). File missing -> rows 0, cac truong coverage None/{}.
    """
    import csv
    from collections import Counter

    try:
        from config import TRAFFIC_DATA_CSV as _T, WEATHER_DATA_CSV as _W
    except ImportError:
        _T = _W = None

    t_path = traffic_path if traffic_path is not None else _T
    w_path = weather_path if weather_path is not None else _W
    tz = ZoneInfo(COLLECTION_TIMEZONE)

    traffic_rows = _count_data_rows(t_path) if t_path else 0
    weather_rows = _count_data_rows(w_path) if w_path else 0

    roads = Counter()
    hours = set()
    days = set()
    weekday = weekend = 0
    stamps = []
    try:
        with open(t_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    ts = datetime.fromisoformat(row.get("timestamp", ""))
                except (TypeError, ValueError):
                    continue
                if ts.tzinfo is None:
                    continue
                local = ts.astimezone(tz)
                stamps.append(ts)
                days.add(local.date().isoformat())
                hours.add(local.hour)
                roads[row.get("road_name", "?")] += 1
                if local.weekday() < 5:
                    weekday += 1
                else:
                    weekend += 1
    except (FileNotFoundError, TypeError):
        pass

    rain_yes = rain_no = 0
    try:
        with open(w_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    rain = float(row.get("rain", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if rain > 0:
                    rain_yes += 1
                else:
                    rain_no += 1
    except (FileNotFoundError, TypeError):
        pass

    return {
        "traffic_rows": traffic_rows,
        "weather_rows": weather_rows,
        "date_min": min(stamps).isoformat() if stamps else None,
        "date_max": max(stamps).isoformat() if stamps else None,
        "days": len(days),
        "roads": dict(roads),
        "local_hours": sorted(hours),
        "weekday": weekday,
        "weekend": weekend,
        "rain_yes": rain_yes,
        "rain_no": rain_no,
    }


def _print_cycle_status(index, total, scheduled, result, next_slot,
                        traffic_rows, weather_rows):
    """Mot cycle status toi thieu (KHONG log API key)."""
    now_hanoi = _hanoi_now().isoformat(timespec="seconds")
    print(f"[{index}/{total}] Hanoi now={now_hanoi} slot={scheduled}")
    if "summary" in result:
        s = result["summary"]
        print(f"  roads_loaded={s.get('roads_loaded')} "
              f"traffic={s.get('traffic_collected')}/{s.get('traffic_failed')}fail/"
              f"{s.get('traffic_saved')}saved "
              f"weather={s.get('weather_collected')}/{s.get('weather_failed')}fail/"
              f"{s.get('weather_saved')}saved")
    else:
        print(f"  cycle FAILED: {result.get('error')} (cho slot tiep theo)")
    print(f"  totals: traffic_rows={traffic_rows} weather_rows={weather_rows} "
          f"next_slot={next_slot}")


def _sanitize_process_error(exc):
    """Redact key=... khoi process error truoc khi in (khong lo secret)."""
    import re as _re

    return _re.sub(r"(key=)[^&\s'\"]+", r"\1***", str(exc or "unknown error"))


# --- Continuous background collection mode (Phase 10, operational) ---
# Service chay lien tuc qua nhieu ngay (khong max_cycles), nhung chi goi
# traffic/weather API trong collection window 06:00-22:00 Asia/Ho_Chi_Minh
# (giong campaign hien tai). Ngoai window: KHONG goi API, chi doi toi slot
# ke tiep. Khong backfill slot da lo. Khong database/checkpoint cho counter.

# Chunk cho overnight wait: moi chunk sleep toi da 60s roi tinh lai
# remaining. Ly do: tranh 1 uninterruptible sleep 8 tieng lam Ctrl+C kho
# phan hoi tren Windows; 60s >> "vai giay" nen khong busy-loop, khong spam.
_CONTINUOUS_WAIT_CHUNK_SECONDS = 60


def _wait_until(target, now_fn, sleep_fn,
                chunk_seconds=_CONTINUOUS_WAIT_CHUNK_SECONDS):
    """Doi toi `target` bang cac sleep ngan (interruptible, khong busy-loop).

    - target/now deu timezone-aware (qua _to_collection_tz).
    - Moi vong: remaining = (target - now).total_seconds(); neu <= 0 thi ve.
    - Sleep min(remaining, chunk_seconds) roi lap lai (re-read clock).
    - KeyboardInterrupt duoc propagate (Ctrl+C dung sach o caller).
    - Khong goi API, khong Spark, khong in log o day (caller in 1 lan).
    """
    end = _to_collection_tz(target)
    step = chunk_seconds if chunk_seconds and chunk_seconds > 0 else 60
    while True:
        remaining = (end - _to_collection_tz(now_fn())).total_seconds()
        if remaining <= 0:
            return
        sleep_fn(min(remaining, step))


def _is_overnight_gap(wait_seconds):
    """True neu wait dai hon 1 collection interval (nghi ngoai window).

    Trong window, khoang cach toi slot ke chi <= interval (5 phut).
    Overnight 22:00-06:00 wait ~6-8 tieng >> interval -> True.
    """
    try:
        return float(wait_seconds) > COLLECTION_INTERVAL_MINUTES * 60
    except (TypeError, ValueError):
        return False


def _print_continuous_banner(auto_on):
    """Banner khi start continuous service (KHONG log API key)."""
    from config import AUTO_PROCESS_EVERY_CYCLES as _EVERY

    auto_state = "ON" if auto_on else "OFF"
    print("Mode: CONTINUOUS")
    print(f"Collection interval: {COLLECTION_INTERVAL_MINUTES} minutes")
    print(f"Collection window: 06:00-22:00 {COLLECTION_TIMEZONE}")
    print(f"Auto-process: {auto_state}")
    print(f"Auto-process every: {_EVERY} cycles")


def _print_outside_window(next_slot):
    """Thong bao ngoai window 1 lan (khong spam moi chunk)."""
    nxt = next_slot.isoformat() if hasattr(next_slot, "isoformat") \
        else str(next_slot)
    print("Outside collection window. "
          f"Next collection slot: {nxt} {COLLECTION_TIMEZONE}")


def _print_continuous_status(index, total, scheduled, result, next_slot,
                             traffic_rows, weather_rows):
    """Mot cycle status cho continuous mode (KHONG log API key)."""
    now_hanoi = _hanoi_now().isoformat(timespec="seconds")
    print(f"[{now_hanoi}] Cycle {index} (continuous) slot={scheduled}")
    if "summary" in result:
        s = result["summary"]
        print(f"  traffic success={s.get('traffic_saved')} "
              f"(collected={s.get('traffic_collected')} "
              f"failed={s.get('traffic_failed')}) "
              f"weather success={s.get('weather_saved')} "
              f"(collected={s.get('weather_collected')} "
              f"failed={s.get('weather_failed')})")
    else:
        print(f"  cycle FAILED: {result.get('error')} (cho slot tiep theo)")
    print(f"  totals: traffic_rows={traffic_rows} "
          f"weather_rows={weather_rows} next slot={next_slot}")


def run_continuous(now_fn=None, sleep_fn=None, cycle_fn=None,
                   traffic_path=None, weather_path=None, progress_fn=None,
                   auto_process=None, process_fn=None):
    """Chay collection service lien tuc qua nhieu ngay (khong max_cycles).

    - Khong co max_cycles; chi dung khi Ctrl+C hoac fatal scheduling error.
    - Collection window GIU NGUYEN 06:00-22:00 Asia/Ho_Chi_Minh
      (start-inclusive/end-exclusive, slots 06:00..21:55). Ngoai window
      (22:00-06:00): KHONG goi cycle_fn/API; chi doi toi slot ke tiep bang
      _wait_until() chunked (khong busy-loop, khong wake vai giay, khong
      backfill slot da lo). Moi overnight gap chi in 1 dong outside-window.
    - Day transition: khong restart process, khong reset raw files (raw CSV
      append-only), counter attempts GIU NGUYEN qua ngay (attempt-based,
      khong reset luc 00:00; voi EVERY=1 moi attempt deu process ngay
      sau khi raw persistence hoan tat).
    - Auto-process TAI SU DUNG data_update.run_process_only() (mac dinh theo
      config AUTO_PROCESS_ENABLED; tat bang auto_process=False hoac CLI
      --no-auto-process): sau moi AUTO_PROCESS_EVERY_CYCLES collection
      ATTEMPTS thuc te trong valid window (summary hay error deu dem) chay
      1 periodic process tuan tu (raw da dong/flush). Overnight idle KHONG
      dem thanh cycles, KHONG Spark chi vi clock troi qua, KHONG them API.
    - Restart semantics (limitation, documented): stop roi start lai
      `python historical_collector.py --continuous` thi counter session moi
      BAT DAU LAI TU 0 (khong reconstruct tu raw CSV, khong state DB/
      checkpoint file). Raw cu giu nguyen. Fallback luon co:
      `python data_update.py --process-only`.
    - Error recovery: collection cycle exception -> ghi error entry, raw da
      ghi truoc do giu nguyen, KHONG crash service (neu best-effort hien tai
      cho phep), slot ke tiep thu lai. Auto-process fail -> sanitize, in
      "Raw data preserved", service tiep tuc, KHONG retry ngay; boundary ke
      tiep thu lai. Fatal programming/config error (scheduling/valuable):
      KHONG swallow vo han — de propagate (khac voi transient operational
      failure duoc ghi nhan va tiep tuc). Khong framework retry phuc tap.
    - Ctrl+C: thoat sach, giu raw da append, khong truncate/temp sot, KHONG
      bat buoc final Spark. Muon snapshot moi nhat sau Ctrl+C thi chay
      manual `python data_update.py --process-only`.
    - Tra ve dict {mode, results, stopped, completed, auto_processes,
      auto_process_enabled}. `results` append theo attempts (chu y memory
      khi chay rat lau; MVP chap nhan vi moi entry nho).
    """
    import time as _time

    from collector import run_collection_cycle as _default_cycle

    try:
        from config import TRAFFIC_DATA_CSV as _T, WEATHER_DATA_CSV as _W
        from config import AUTO_PROCESS_ENABLED as _AP_DEFAULT
        from config import AUTO_PROCESS_EVERY_CYCLES as _AP_EVERY
    except ImportError:
        _T = _W = None
        _AP_DEFAULT = True
        _AP_EVERY = 1

    now_fn = now_fn or _hanoi_now
    sleep_fn = sleep_fn or _time.sleep
    cycle_fn = cycle_fn or _default_cycle
    t_path = traffic_path if traffic_path is not None else _T
    w_path = weather_path if weather_path is not None else _W
    progress_fn = progress_fn or _print_continuous_status
    auto_enabled = _AP_DEFAULT if auto_process is None else bool(auto_process)
    if process_fn is None:
        from data_update import run_process_only as _default_process
        process_fn = _default_process

    _print_continuous_banner(auto_enabled)

    auto_processes = []

    def _run_auto_process(after_cycle, tag):
        """Chay 1 process-only, report, KHONG bao gio raise ve scheduler."""
        print(f"Auto-process starting... (after cycle {after_cycle}, {tag})")
        try:
            pres = process_fn()
        except Exception as exc:
            err = _sanitize_process_error(exc)
            print(f"Auto-process FAILED: {err} "
                  f"Raw data preserved. Campaign continues.")
            auto_processes.append({"after_cycle": after_cycle, "tag": tag,
                                   "success": False, "error": err})
            return
        ok = bool(pres.get("success")) if isinstance(pres, dict) else False
        if ok:
            print(f"Auto-process success: "
                  f"combined_rows={pres.get('combined_output_rows')} "
                  f"duration={pres.get('duration_seconds')}s")
            auto_processes.append({"after_cycle": after_cycle, "tag": tag,
                                   "success": True,
                                   "combined_rows": pres.get("combined_output_rows"),
                                   "duration_seconds": pres.get("duration_seconds")})
        else:
            err = _sanitize_process_error(
                (pres.get("error") if isinstance(pres, dict) else None)
                or "unknown error")
            print(f"Auto-process FAILED: {err} "
                  f"Raw data preserved. Campaign continues.")
            auto_processes.append({"after_cycle": after_cycle, "tag": tag,
                                   "success": False, "error": err})

    results = []
    stopped = False
    attempts = 0
    try:
        target = next_collection_time(now_fn())
        while True:
            wait = (target - _to_collection_tz(now_fn())).total_seconds()
            if wait > 0:
                if _is_overnight_gap(wait):
                    _print_outside_window(target)
                _wait_until(target, now_fn, sleep_fn)
            try:
                summary = cycle_fn()
                entry = {"scheduled_time": target.isoformat(),
                         "summary": summary}
            except Exception as exc:
                entry = {"scheduled_time": target.isoformat(),
                         "error": str(exc)}
            results.append(entry)
            attempts += 1
            if auto_enabled and attempts % _AP_EVERY == 0:
                _run_auto_process(attempts, "periodic")
            nxt = _next_strict(target, now_fn())
            progress_fn(attempts, None, target.isoformat(), entry,
                        nxt.isoformat(), _count_data_rows(t_path),
                        _count_data_rows(w_path))
            target = nxt
    except KeyboardInterrupt:
        stopped = True
        print("Campaign stopped by user (Ctrl+C). "
              "Du lieu da append duoc giu; khong temp file; "
              "khong final Spark. "
              "Chay manual `python data_update.py --process-only` khi can.")
    return {"mode": "continuous", "results": results, "stopped": stopped,
            "completed": len(results),
            "auto_processes": auto_processes,
            "auto_process_enabled": auto_enabled}


def run_campaign(max_cycles, now_fn=None, sleep_fn=None, cycle_fn=None,
                 traffic_path=None, weather_path=None, progress_fn=None,
                 auto_process=None, process_fn=None):
    """Chay campaign thu thap du lieu that (bounded, append-only, best-effort).

    - max_cycles bat buoc > 0; KHONG infinite mode (restart = goi lai).
    - Mac dinh cycle_fn = collector.run_collection_cycle (tai su dung, khong
      duplicate API logic). Per-road fail nam trong summary; fatal cycle
      exception -> ghi error, sang slot tiep (khong retry spam ngay).
    - Resume-safe: khong truncate/overwrite; sau moi cycle chi append raw CSV;
      restart tinh slot moi >= now (missed slots khong backfill).
    - Ctrl+C (KeyboardInterrupt): dung sach, giu du lieu da append, khong temp
      file; ket qua tra ve kem 'stopped': True.
    - progress_fn(index, total, scheduled_iso, result, next_iso,
      traffic_rows, weather_rows): default in status CLI toi thieu.
    - Auto-process (mac dinh theo config.AUTO_PROCESS_ENABLED; override bang
      auto_process True/False; tat han bang CLI --no-auto-process):
      sau moi AUTO_PROCESS_EVERY_CYCLES cycle ATTEMPTS da hoan tat (summary
      hay error deu dem, deterministic), chay process_fn() — mac dinh
      data_update.run_process_only() (TAI SU DUNG, khong duplicate Spark
      pipeline, khong them API call) — tuan tu: cycle da xong, raw files da
      dong/flush; Spark xong scheduler moi tinh slot tiep. KHONG Spark song
      song collection (1 Spark job tai 1 thoi diem; near-realtime batch).
    - Failure isolation: process fail -> ghi error, raw GIU NGUYEN (khong
      rollback/xoa), campaign TIEP TUC slot tuong lai, khong retry ngay;
      boundary ke tiep (hoac final process) co the thu lai.
    - Scheduler giu contract: slot tiep = _next_strict(target, now H IEN TAI
      sau khi process xong) nen slot bi lo trong luc Spark chay duoc skip,
      khong backfill/catch-up spam.
    - Final process: ket thuc BINH THUONG do dat max_cycles, da co it nhat 1
      periodic process, va con raw cycles moi tu lan process gan nhat ->
      chay them 1 process cuoi. Voi EVERY=1 moi attempt da process ngay nen
      final thuong khong can (last_process == attempts). Campaign ngan hon
      1 interval (chua tung process) thi KHONG final. Ctrl+C -> KHONG final
      Spark (uu tien stop
      nhanh/an toan; user chay manual `python data_update.py --process-only`
      sau). Limitation: Ctrl+C dung luc Spark dang chay thi khong kill phuc
      tap; current process xu ly exception/cleanup theo behavior an toan hien
      co roi stop.
    - Tra ve dict cu + 'auto_processes' (list {after_cycle, tag, success,
      combined_rows/duration_seconds hoac error}) + 'auto_process_enabled'.
    """
    if isinstance(max_cycles, bool) or not isinstance(max_cycles, int):
        raise ValueError(f"max_cycles phai la int > 0: {max_cycles!r}.")
    if max_cycles <= 0:
        raise ValueError(f"max_cycles phai > 0: {max_cycles!r}.")

    import time as _time

    from collector import run_collection_cycle as _default_cycle

    try:
        from config import TRAFFIC_DATA_CSV as _T, WEATHER_DATA_CSV as _W
        from config import AUTO_PROCESS_ENABLED as _AP_DEFAULT
        from config import AUTO_PROCESS_EVERY_CYCLES as _AP_EVERY
    except ImportError:
        _T = _W = None
        _AP_DEFAULT = True
        _AP_EVERY = 1

    now_fn = now_fn or _hanoi_now
    sleep_fn = sleep_fn or _time.sleep
    cycle_fn = cycle_fn or _default_cycle
    t_path = traffic_path if traffic_path is not None else _T
    w_path = weather_path if weather_path is not None else _W
    progress_fn = progress_fn or _print_cycle_status
    auto_enabled = _AP_DEFAULT if auto_process is None else bool(auto_process)
    if process_fn is None:
        from data_update import run_process_only as _default_process
        process_fn = _default_process

    auto_processes = []

    def _run_auto_process(after_cycle, tag):
        """Chay 1 process-only, report, KHONG bao gio raise ve scheduler."""
        print(f"Auto-process starting... (after cycle {after_cycle}, {tag})")
        try:
            pres = process_fn()
        except Exception as exc:
            err = _sanitize_process_error(exc)
            print(f"Auto-process FAILED: {err} "
                  f"Raw data preserved. Campaign continues.")
            auto_processes.append({"after_cycle": after_cycle, "tag": tag,
                                   "success": False, "error": err})
            return
        ok = bool(pres.get("success")) if isinstance(pres, dict) else False
        if ok:
            print(f"Auto-process success: "
                  f"combined_rows={pres.get('combined_output_rows')} "
                  f"duration={pres.get('duration_seconds')}s")
            auto_processes.append({"after_cycle": after_cycle, "tag": tag,
                                   "success": True,
                                   "combined_rows": pres.get("combined_output_rows"),
                                   "duration_seconds": pres.get("duration_seconds")})
        else:
            err = _sanitize_process_error(
                (pres.get("error") if isinstance(pres, dict) else None)
                or "unknown error")
            print(f"Auto-process FAILED: {err} "
                  f"Raw data preserved. Campaign continues.")
            auto_processes.append({"after_cycle": after_cycle, "tag": tag,
                                   "success": False, "error": err})

    results = []
    stopped = False
    attempts = 0
    last_process_attempt = 0
    try:
        target = next_collection_time(now_fn())
        for index in range(1, max_cycles + 1):
            wait = (target - _to_collection_tz(now_fn())).total_seconds()
            if wait > 0:
                sleep_fn(wait)
            try:
                summary = cycle_fn()
                entry = {"scheduled_time": target.isoformat(),
                         "summary": summary}
            except Exception as exc:
                entry = {"scheduled_time": target.isoformat(),
                         "error": str(exc)}
            results.append(entry)
            attempts += 1
            # Boundary tuan tu: cycle da xong (raw dong/flush) moi process.
            if auto_enabled and attempts % _AP_EVERY == 0:
                _run_auto_process(index, "periodic")
                last_process_attempt = attempts
            if index >= max_cycles:
                progress_fn(index, max_cycles, target.isoformat(), entry,
                            "done", _count_data_rows(t_path),
                            _count_data_rows(w_path))
                break
            # now() MOI sau process: slot lo trong luc Spark chay bi skip.
            nxt = _next_strict(target, now_fn())
            progress_fn(index, max_cycles, target.isoformat(), entry,
                        nxt.isoformat(), _count_data_rows(t_path),
                        _count_data_rows(w_path))
            target = nxt
    except KeyboardInterrupt:
        stopped = True
        print("Campaign stopped by user (Ctrl+C). "
              "Du lieu da append duoc giu; khong temp file; "
              "restart bang cung command de tiep tuc slot tuong lai. "
              "Chay manual `python data_update.py --process-only` khi can.")
    # Final process: chi khi ket thuc binh thuong + da co it nhat 1 periodic
    # process + con cycles moi sau lan process gan nhat (vd 17 cycles: process
    # o 12 + final o 17). Campaign ngan hon 1 interval (vd 11 cycles, chua bao
    # gio process) thi KHONG final — user chay manual process-only khi can.
    if (not stopped and auto_enabled and last_process_attempt > 0
            and attempts > last_process_attempt):
        _run_auto_process(attempts, "final")
    return {"results": results, "stopped": stopped,
            "completed": len(results), "max_cycles": max_cycles,
            "auto_processes": auto_processes,
            "auto_process_enabled": auto_enabled}


def _print_progress_summary(p):
    """In read-only progress summary (KHONG mutation)."""
    print(f"traffic_rows={p['traffic_rows']} weather_rows={p['weather_rows']}")
    print(f"date_range={p['date_min']} .. {p['date_max']} days={p['days']}")
    print(f"roads={p['roads']}")
    print(f"local_hours={p['local_hours']} "
          f"weekday={p['weekday']} weekend={p['weekend']} "
          f"rain_yes={p['rain_yes']} rain_no={p['rain_no']}")


def main(argv=None):
    """Campaign CLI (doc lap Streamlit).

    --max-cycles N    : chay toi da N scheduled cycles (bat buoc > 0).
    --continuous      : service lien tuc qua nhieu ngay (khong max_cycles;
                        MUTUALLY EXCLUSIVE voi --max-cycles).
    --progress        : chi in tien do hien tai (read-only, khong thu thap).
    --no-auto-process : tat auto Spark process-only dinh ky (mac dinh BAT
                        theo config AUTO_PROCESS_ENABLED; khi tat, user tu chay
                        manual `python data_update.py --process-only` sau).
    Ctrl+C dung sach, giu raw da append, khong temp file (khong final Spark).
    """
    import argparse

    from config import AUTO_PROCESS_ENABLED, AUTO_PROCESS_EVERY_CYCLES

    parser = argparse.ArgumentParser(
        description="Hanoi Traffic Analytics - real data collection campaign "
                    "(API -> raw CSV, append-only; auto Spark process-only "
                    "dinh ky).")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--max-cycles", type=int, default=192,
                      help="So scheduled cycles toi da (> 0). "
                           "192 = ~1 ngay campaign 06:00-22:00 moi 5 phut.")
    mode.add_argument("--continuous", action="store_true",
                      help="Chay service lien tuc qua nhieu ngay (khong gioi "
                           "han cycles; chi goi API 06:00-22:00 "
                           "Asia/Ho_Chi_Minh; Ctrl+C de dung sach).")
    parser.add_argument("--progress", action="store_true",
                        help="Chi in tien do (read-only), khong thu thap.")
    parser.add_argument("--no-auto-process", action="store_true",
                        help="Tat auto-process (mac dinh bat: Spark process-only "
                             "ngay sau moi collection cycle + final khi ket thuc).")
    args = parser.parse_args(argv)

    if args.progress:
        _print_progress_summary(campaign_progress())
        return 0

    auto_on = bool(AUTO_PROCESS_ENABLED) and not args.no_auto_process

    if args.continuous:
        print("=== Hanoi Traffic Analytics - Continuous Collection Service ===")
        print(f"Hanoi now: {_hanoi_now().isoformat(timespec='seconds')}")
        print(f"Schedule: {COLLECTION_INTERVAL_MINUTES} min, windows "
              f"{list(COLLECTION_WINDOWS)} ({COLLECTION_TIMEZONE}); "
              f"mode=CONTINUOUS (Ctrl+C de dung sach).")
        try:
            outcome = run_continuous(auto_process=auto_on)
        except Exception as exc:
            print(f"Continuous service failed: {exc}")
            return 1
        ok = sum(1 for r in outcome["results"] if "summary" in r)
        fail = len(outcome["results"]) - ok
        aps = outcome.get("auto_processes", [])
        ap_ok = sum(1 for a in aps if a.get("success"))
        print(f"Continuous service stopped: {ok} ok / {fail} failed "
              f"({len(outcome['results'])} cycles). "
              f"Raw CSV append-only, khong truncate. "
              f"Auto-process runs: {len(aps)} ({ap_ok} success).")
        return 0

    if args.max_cycles is None or args.max_cycles <= 0:
        print("Collection campaign failed: --max-cycles phai > 0.")
        return 1

    print("=== Hanoi Traffic Analytics - Collection Campaign ===")
    print(f"Hanoi now: {_hanoi_now().isoformat(timespec='seconds')}")
    print(f"Schedule: {COLLECTION_INTERVAL_MINUTES} min, windows "
          f"{list(COLLECTION_WINDOWS)} ({COLLECTION_TIMEZONE}); "
          f"max_cycles={args.max_cycles}. Ctrl+C de dung sach.")
    if auto_on:
        print(f"Auto-process: ON (Spark process-only ngay sau moi "
              f"collection cycle (every {AUTO_PROCESS_EVERY_CYCLES} cycle) "
              f"+ final khi xong; fail thi giu raw va tiep tuc).")
    else:
        print("Auto-process: OFF (chay manual "
              "`python data_update.py --process-only` khi can).")
    try:
        outcome = run_campaign(args.max_cycles, auto_process=auto_on)
    except Exception as exc:
        print(f"Collection campaign failed: {exc}")
        return 1
    ok = sum(1 for r in outcome["results"] if "summary" in r)
    fail = len(outcome["results"]) - ok
    state = "STOPPED by user" if outcome["stopped"] else "finished"
    aps = outcome.get("auto_processes", [])
    ap_ok = sum(1 for a in aps if a.get("success"))
    print(f"Campaign {state}: {ok} ok / {fail} failed "
          f"({len(outcome['results'])}/{args.max_cycles} cycles). "
          f"Raw CSV append-only, khong truncate. "
          f"Auto-process runs: {len(aps)} ({ap_ok} success).")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
