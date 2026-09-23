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
