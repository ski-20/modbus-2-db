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
    """Return {tag: label} from tag_meta. Falls back to {} if table missing."""
    try:
        with db() as con:
            return {
                r["tag"]: (r["label"] or r["tag"])
                for r in con.execute("SELECT tag, label FROM tag_meta")
            }
    except Exception:
        return {}

def list_tags_with_labels():
    with db() as con:
        rows = con.execute("""
            SELECT tag, COALESCE(label, tag) AS label
            FROM tag_meta
            ORDER BY label COLLATE NOCASE
        """).fetchall()
        return [dict(r) for r in rows]

def list_tags() -> list[str]:
    """If you still need just tag strings somewhere."""
    with db() as con:
        return [r["tag"] for r in con.execute(
            "SELECT tag FROM tag_meta ORDER BY tag COLLATE NOCASE"
        )]

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

def query_logs(tag=None, date_range="all", limit=500, bucket_s=0):
    with db() as con:
        sql = "SELECT ts, tag, value FROM logs WHERE 1=1"
        params = []

        if tag and tag != "":
            sql += " AND tag=?"
            params.append(tag)

        # Handle date range
        if date_range != "all":
            if date_range == "today":
                sql += " AND date(ts) = date('now', 'localtime')"
            elif date_range == "yesterday":
                sql += " AND date(ts) = date('now', '-1 day', 'localtime')"
            elif date_range == "week":
                sql += " AND ts >= date('now', '-7 days', 'localtime')"
            elif date_range == "month":
                sql += " AND ts >= date('now', '-1 month', 'localtime')"
            elif date_range == "year":
                sql += " AND ts >= date('now', '-1 year', 'localtime')"

        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)

        rows = con.execute(sql, params).fetchall()

        return rows
    
def query_logs_between(tag: Optional[str], start_iso: str, end_iso: str,
                       limit: int, bucket_s: Optional[int]) -> list[dict]:
    tag_clause = "" if not tag else "AND l.tag = ?"
    args = [start_iso, end_iso] + ([tag] if tag else []) + [limit]

    if bucket_s and bucket_s > 0:
        q = f"""
        WITH rows AS (
          SELECT l.ts, l.tag, l.value, l.unit
          FROM logs l
          WHERE l.ts >= ? AND l.ts <= ?
          {tag_clause}
        ),
        buck AS (
          SELECT
            strftime('%s', ts) / {bucket_s} AS bucket,
            tag,
            AVG(value) AS value,
            MAX(unit) AS unit,
            MIN(ts) AS ts
          FROM rows
          GROUP BY tag, bucket
        )
        SELECT b.ts AS ts,
               b.tag AS tag,
               COALESCE(m.label, b.tag) AS label,
               b.value AS value,
               b.unit  AS unit
        FROM buck b
        LEFT JOIN tag_meta m ON m.tag = b.tag
        ORDER BY ts DESC
        LIMIT ?
        """
    else:
        q = f"""
        SELECT l.ts AS ts,
               l.tag AS tag,
               COALESCE(m.label, l.tag) AS label,
               l.value AS value,
               l.unit  AS unit
        FROM logs l
        LEFT JOIN tag_meta m ON m.tag = l.tag
        WHERE l.ts >= ? AND l.ts <= ?
        {tag_clause}
        ORDER BY l.ts DESC
        LIMIT ?
        """

    with db() as con:
        rows = [dict(r) for r in con.execute(q, args)]

    for r in rows:
        r["ts_fmt"] = fmt_ts_local_from_iso(r["ts"])
        # Force floats to 1 decimal
        if isinstance(r["value"], float):
            r["value"] = round(r["value"], 1)

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


# --- setpoints import (from tags.py) ---
try:
    from tags import SETPOINTS as TAGS_SETPOINTS
except Exception:
    TAGS_SETPOINTS = []

def fetch_setpoints():
    """Return setpoints as list of dicts for the UI."""
    rows = []
    for sp in TAGS_SETPOINTS:
        rows.append({
            "name": sp["name"],
            "label": sp.get("label", sp["name"]),
            "unit": sp.get("unit", ""),
            "mw": sp["mw"],
            "dtype": sp.get("type", sp.get("dtype", "FLOAT32")),
        })
    return rows
