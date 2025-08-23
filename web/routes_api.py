# API routes: /api/logs, /api/download.csv

from flask import Blueprint, request, jsonify, make_response
from config import DB_ROOT, RETENTION, LOCAL_TZ
from tags import TAGS
from chunks import query_logs, init_family_router, meta_path
import csv, io, os, sqlite3
from typing import Optional, Tuple
from datetime import datetime, timezone, timedelta

try:
    # optional: if you implemented it
    from chunks import query_logs_between
    HAS_QB = True
except Exception:
    HAS_QB = False

# Optional: week start (0=Mon..6=Sun)
try:
    from config import WEEK_START
except Exception:
    WEEK_START = 0

# Build a tzinfo from LOCAL_TZ in config
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    try:
        from backports.zoneinfo import ZoneInfo  # Python 3.8: pip install backports.zoneinfo
    except Exception:
        ZoneInfo = None

_LOCAL_TZ = ZoneInfo(LOCAL_TZ) if ZoneInfo else None

# Route interval/conditional/on_change -> families
init_family_router(TAGS, RETENTION.get("family_overrides"))

api_bp = Blueprint("api", __name__)

# ---------- helpers ----------

_TAGMAP_CACHE = {"mtime": 0, "map": {}}

def _tag_map():
    """Return {tag: {'label': str, 'unit': str}} from meta.db, cached by file mtime."""
    meta = meta_path(DB_ROOT)
    mtime = os.path.getmtime(meta) if os.path.exists(meta) else 0
    if mtime != _TAGMAP_CACHE["mtime"]:
        d = {}
        if mtime:
            con = sqlite3.connect(meta, timeout=10)
            cur = con.cursor()
            try:
                cur.execute("SELECT tag, label, unit FROM tag_meta")
                for t, lbl, unit in cur.fetchall():
                    d[str(t)] = {"label": lbl or str(t), "unit": unit or ""}
            finally:
                cur.close(); con.close()
        _TAGMAP_CACHE["mtime"] = mtime
        _TAGMAP_CACHE["map"] = d
    return _TAGMAP_CACHE["map"]

def _to_utc(dt_local: datetime) -> str:
    if _LOCAL_TZ:
        dt_local = dt_local.replace(tzinfo=_LOCAL_TZ)
        dt_utc = dt_local.astimezone(timezone.utc)
    else:
        dt_utc = dt_local.replace(tzinfo=timezone.utc)
    return dt_utc.isoformat()

def _parse_int(s, default):
    try: return int(s)
    except: return default

def _parse_local_dt(s: str) -> Optional[datetime]:
     if not s: return None
     try:
         return datetime.fromisoformat(s)
     except Exception:
         return None

def _bounds_from_request(cal: str, start_s: Optional[str], end_s: Optional[str]) -> Tuple[str, str]:
     if cal != "custom":
         return _bounds_for_calendar(cal)
     start_dt = _parse_local_dt(start_s) if start_s else None
     end_dt   = _parse_local_dt(end_s)   if end_s   else None
     now_local = datetime.now(_LOCAL_TZ) if _LOCAL_TZ else datetime.now(timezone.utc)
     if not end_dt:
         end_dt = now_local.replace(microsecond=0)
     if not start_dt:
         start_dt = end_dt - timedelta(days=1)
    # If user gave start > end, swap
     if start_dt > end_dt:
         start_dt, end_dt = end_dt, start_dt
     return _to_utc(start_dt), _to_utc(end_dt)

def _filter_by_bounds(rows, start_iso: str, end_iso: str):
    """rows = [(ts, tag, value, unit), ...] — filter by UTC ISO bounds inclusive."""
    def _to_epoch(ts: str) -> float:
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return -1.0
    s_ep = _to_epoch(start_iso); e_ep = _to_epoch(end_iso)
    out = []
    for ts, tg, val, unit in rows:
        ep = _to_epoch(ts)
        if s_ep <= ep <= e_ep:
            out.append((ts, tg, val, unit))
    return out

def _bounds_for_calendar(preset: str) -> Tuple[str, str]:
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(_LOCAL_TZ) if _LOCAL_TZ else now_utc
    today = now_local.date()
    if preset == "today":
        start_local = datetime(today.year, today.month, today.day, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    elif preset == "yesterday":
        y = today - timedelta(days=1)
        start_local = datetime(y.year, y.month, y.day, 0, 0, 0)
        end_local   = datetime(y.year, y.month, y.day, 23, 59, 59)
    elif preset == "week":
        delta = (today.weekday() - WEEK_START) % 7
        week_start = today - timedelta(days=delta)
        start_local = datetime(week_start.year, week_start.month, week_start.day, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    elif preset == "month":
        start_local = datetime(today.year, today.month, 1, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    elif preset == "year":
        start_local = datetime(today.year, 1, 1, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    else:
        start_local = datetime(1970, 1, 1, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    return _to_utc(start_local), _to_utc(end_local)

def _to_iso_utc(dt: datetime) -> str:
    # Ensure ISO in UTC (logger writes UTC-naive; we normalize to UTC-aware ISO)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _floor_to_bucket(ts: datetime, secs: int) -> datetime:
    # treat naive as UTC
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    epoch = int(ts.timestamp())
    floored = epoch - (epoch % secs)
    return datetime.fromtimestamp(floored, tz=timezone.utc)

def _maybe_bucket(rows, bucket_s: int):
    """
    rows: list of tuples (ts_iso, tag, value, unit)
    bucket_s: seconds; returns averaged value per (tag, bucket)
    """
    from collections import defaultdict
    acc = defaultdict(lambda: {"sum": 0.0, "n": 0, "unit": ""})
    for ts_iso, tag, val, unit in rows:
        try:
            dt = datetime.fromisoformat(ts_iso)
        except Exception:
            continue
        bts = _floor_to_bucket(dt, bucket_s)
        key = (tag, bts)
        acc[key]["sum"] += float(val) if val is not None else 0.0
        acc[key]["n"]   += 1
        acc[key]["unit"] = unit or ""
    out = []
    for (tag, bts), v in acc.items():
        avg = (v["sum"] / v["n"]) if v["n"] else None
        out.append((_to_iso_utc(bts), tag, avg, v["unit"]))
    # newest first to match your UI
    out.sort(key=lambda r: r[0], reverse=True)
    return out

def download_csv(rows) -> str:
    """
    rows: iterable of (ts_iso_utc, tag, value, unit)
    Writes CSV with an extra 'label' column using tag_meta.
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts_utc", "tag", "label", "value", "unit"])
    tmap = _tag_map()  # {tag: {'label','unit'}}
    for ts, tg, val, unit in rows:
        label = tmap.get(tg, {}).get("label", tg)
        w.writerow([ts, tg, label, "" if val is None else val, unit or ""])
    return buf.getvalue()

# ---------- routes ----------

@api_bp.route("/logs")
def api_logs():
    tag      = (request.args.get("tag") or "").strip() or None
    cal      = (request.args.get("cal") or "all").strip().lower()
    limit    = _parse_int(request.args.get("limit", "500"), 500)
    bucket_s = _parse_int(request.args.get("bucket_s", "") or 0, 0)
    start_s  = (request.args.get("start") or "").strip() or None
    end_s    = (request.args.get("end") or "").strip() or None

    start_iso, end_iso = _bounds_from_request(cal, start_s, end_s)

    # Expand fetch a bit when bucketing or custom ranges
    fetch_limit = limit if bucket_s == 0 and cal != "custom" else max(limit * 4, 2000)

    if HAS_QB:
        rows = query_logs_between(DB_ROOT, tag=tag, start_iso=start_iso, end_iso=end_iso, limit=fetch_limit)
    else:
        # Fallback: pull broader and filter server-side
        base_rows = query_logs(DB_ROOT, tag=tag, cal="all", limit=fetch_limit)
        rows = _filter_by_bounds(base_rows, start_iso, end_iso)

    if bucket_s > 0:
        rows = _maybe_bucket(rows, bucket_s)
    rows = rows[:limit]

    tmap = {t["name"]: t.get("label", t["name"]) for t in TAGS}
    data = [{"ts": ts, "tag": tg, "label": tmap.get(tg, tg), "value": val, "unit": unit}
            for (ts, tg, val, unit) in rows]
    return jsonify(data)

@api_bp.route("/download.csv")
def api_download_csv():
    tag      = (request.args.get("tag") or "").strip() or None
    cal      = (request.args.get("cal") or "all").strip().lower()
    limit    = _parse_int(request.args.get("limit", "100000"), 100000)
    bucket_s = _parse_int(request.args.get("bucket_s", "") or 0, 0)
    start_s  = (request.args.get("start") or "").strip() or None
    end_s    = (request.args.get("end") or "").strip() or None

    start_iso, end_iso = _bounds_from_request(cal, start_s, end_s)

    if HAS_QB:
        rows = query_logs_between(DB_ROOT, tag=tag, start_iso=start_iso, end_iso=end_iso, limit=limit)
    else:
        base_rows = query_logs(DB_ROOT, tag=tag, cal="all", limit=limit*4)
        rows = _filter_by_bounds(base_rows, start_iso, end_iso)

    if bucket_s > 0:
        rows = _maybe_bucket(rows, bucket_s)

    csv_text = download_csv(rows)  # your existing helper
    resp = make_response(csv_text)
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=logs.csv"
    return resp
