# shared helpers: DB, labels, time, queries

import sqlite3, io, csv
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

# Config import (string name → resolved ZoneInfo below)
try:
    from config import DB, LOCAL_TZ as LOCAL_TZ_NAME
except Exception:
    DB = "/home/ele/plc_logger/plc.db"
    LOCAL_TZ_NAME = "UTC"

try:
    from zoneinfo import ZoneInfo
    _ZONE = ZoneInfo(LOCAL_TZ_NAME)
except Exception:
    _ZONE = None  # will fall back to UTC

def db():
    con = sqlite3.connect(DB, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=2000")
    return con

def tag_label_map() -> Dict[str, str]:
    # read-only: if tag_meta missing/empty, return {}
    try:
        with db() as con:
            return {
                r["name"]: (r["label"] or r["name"])
                for r in con.execute("SELECT name, label FROM tag_meta")
            }
    except Exception:
        return {}

def _pretty_tag_fallback(t: str) -> str:
    s = t.replace('_',' ')
    s = s.replace('P1 ', 'Pump 1 ').replace('P2 ', 'Pump 2 ')
    s = s.replace('DCBus','DC Bus').replace('DrvStatusWord','Drive Status Word')
    s = s.replace('OutV','Output Voltage').replace('TorqueRaw','Torque (raw)')
    s = s.replace('Hours x10','Total Hours (x10)')
    return s.strip()

def list_tags() -> List[str]:
    with db() as con:
        return [r["tag"] for r in con.execute("SELECT DISTINCT tag FROM logs ORDER BY tag")]

def fmt_ts_local_from_iso(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        if _ZONE:
            dt = dt.astimezone(_ZONE)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p")
    except Exception:
        return iso_str

def fmt_local_epoch(sec) -> Optional[str]:
    try:
        sec = float(sec)
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        if _ZONE:
            dt = dt.astimezone(_ZONE)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p")
    except Exception:
        return None

def _since_iso(minutes: int) -> str:
    return (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()

def query_logs(tag: Optional[str], mins: int, limit: int, bucket_s: Optional[int]) -> list[dict]:
    since_ts = _since_iso(mins)
    if bucket_s and bucket_s > 0:
        q = """
        WITH rows AS (
          SELECT substr(ts,1,19) AS s, tag, value, unit
          FROM logs
          WHERE ts >= ?
          {tag_clause}
        ),
        agg AS (
          SELECT s, tag, avg(value) AS value, MAX(unit) AS unit
          FROM rows
          GROUP BY tag, s
        )
        SELECT s AS ts, tag, value, unit
        FROM agg
        ORDER BY ts DESC
        LIMIT ?
        """
        tag_clause = "" if not tag else "AND tag = ?"
        args = [since_ts] + ([tag] if tag else []) + [limit]
        q = q.format(tag_clause=tag_clause)
    else:
        q = """
        SELECT ts, tag, value, unit
        FROM logs
        WHERE ts >= ?
        {tag_clause}
        ORDER BY ts DESC
        LIMIT ?
        """
        tag_clause = "" if not tag else "AND tag = ?"
        args = [since_ts] + ([tag] if tag else []) + [limit]
        q = q.format(tag_clause=tag_clause)

    with db() as con:
        rows = [dict(r) for r in con.execute(q, args)]

    for r in rows:
        r["ts_fmt"] = fmt_ts_local_from_iso(r["ts"])
    return rows

def download_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts","tag","value","unit"])
    for r in rows:
        w.writerow([r.get("ts"), r.get("tag"), r.get("value"), r.get("unit","")])
    return buf.getvalue()

def read_state() -> Dict[str, Any]:
    try:
        with db() as con:
            return {k: v for (k, v) in con.execute("SELECT key, value FROM state")}
    except Exception:
        return {}

def fetch_setpoints() -> list[dict]:
    with db() as con:
        return [dict(r) for r in con.execute("""
            SELECT name, label, unit, mw, dtype
            FROM tag_meta
            WHERE is_setpoint = 1
            ORDER BY name
        """)]
