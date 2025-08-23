# API routes: /api/logs, /api/download.csv

from flask import Blueprint, request, jsonify, Response, stream_with_context
from config import DB_ROOT, RETENTION, LOCAL_TZ
from tags import TAGS
from chunks import query_logs, init_family_router, meta_path
import csv
import io
from datetime import datetime, timezone, timedelta

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

def _parse_int(val, default):
    try:
        return int(val)
    except Exception:
        return default

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

# ---------- routes ----------

@api_bp.route("/logs")
def api_logs():
    tag      = (request.args.get("tag") or "").strip() or None
    cal      = (request.args.get("cal") or "all").strip().lower()
    limit    = _parse_int(request.args.get("limit", "500"), 500)
    bucket_s = request.args.get("bucket_s", "").strip()
    bucket_s = _parse_int(bucket_s, 0) if bucket_s else 0

    # Pull from chunk storage (newest→oldest, across the right families)
    fetch_limit = limit if bucket_s == 0 else max(limit * 4, 2000)
    rows = query_logs(DB_ROOT, tag=tag, cal=cal, limit=fetch_limit)

    if bucket_s > 0:
        rows = _maybe_bucket(rows, bucket_s)
        rows = rows[:limit]

    tmap = _tag_map()
    data = [
        {
            "ts": ts,
            "tag": tg,
            "label": (tmap.get(tg, {}).get("label") or tg),
            "value": val,
            "unit": (unit if unit else tmap.get(tg, {}).get("unit", "")),
        }
        for (ts, tg, val, unit) in rows
    ]
    return jsonify(data)

@api_bp.route("/download.csv")
def api_download_csv():
    tag      = (request.args.get("tag") or "").strip() or None
    cal      = (request.args.get("cal") or "all").strip().lower()
    limit    = _parse_int(request.args.get("limit", "100000"), 100000)
    bucket_s = request.args.get("bucket_s", "").strip()
    bucket_s = _parse_int(bucket_s, 0) if bucket_s else 0

    fetch_limit = limit if bucket_s == 0 else max(limit * 2, 5000)
    rows = query_logs(DB_ROOT, tag=tag, cal=cal, limit=fetch_limit)
    if bucket_s > 0:
        rows = _maybe_bucket(rows, bucket_s)
        rows = rows[:limit]

    def generate():
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["ts", "tag", "value", "unit"])
        yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for ts, tg, val, unit in rows:
            w.writerow([ts, tg, val, unit])
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    headers = {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": 'attachment; filename="logs.csv"',
    }
    return Response(stream_with_context(generate()), headers=headers)
