# API routes: /api/logs, /api/download.csv

from flask import Blueprint, request, jsonify, make_response
from .db import db, query_logs, download_csv, tag_label_map, _pretty_tag_fallback

from datetime import datetime, timedelta, timezone, date

try:
    from config import LOCAL_TZ_NAME
except Exception:
    LOCAL_TZ_NAME = "America/Chicago"

try:
    from zoneinfo import ZoneInfo
    _LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)
except Exception:
    _LOCAL_TZ = None

# Monday = 0 ... Sunday = 6
START_OF_WEEK = 0  # change to 6 if you prefer Sunday-start weeks

def _to_utc(dt_local: datetime) -> str:
    """Take a LOCAL naive datetime and return ISO UTC string."""
    if _LOCAL_TZ:
        dt_local = dt_local.replace(tzinfo=_LOCAL_TZ)
        dt_utc = dt_local.astimezone(timezone.utc)
    else:
        # fallback: assume local==UTC
        dt_utc = dt_local.replace(tzinfo=timezone.utc)
    return dt_utc.isoformat()

def _bounds_for_calendar(preset: str) -> tuple[str, str]:
    """
    Returns (start_iso_utc, end_iso_utc) for calendar presets:
    today, yesterday, week, month, year
    """
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
        # calendar week to date, starting on START_OF_WEEK (Mon=0)
        delta = (today.weekday() - START_OF_WEEK) % 7
        week_start = today - timedelta(days=delta)
        start_local = datetime(week_start.year, week_start.month, week_start.day, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    elif preset == "month":
        # first of this month -> now
        start_local = datetime(today.year, today.month, 1, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    elif preset == "year":
        # Jan 1 of this year -> now
        start_local = datetime(today.year, 1, 1, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    elif preset == "all":
        # earliest practical bound; your ts are UTC ISO (naive) so keep it naive too
        start_local = datetime(1970, 1, 1, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)
    else:
        # default to "today" if not recognized
        start_local = datetime(today.year, today.month, today.day, 0, 0, 0)
        end_local   = now_local.replace(microsecond=0)

    return _to_utc(start_local), _to_utc(end_local)

api_bp = Blueprint("api", __name__)

@api_bp.route("/logs")
def api_logs():
    tag      = (request.args.get("tag") or "").strip() or None
    cal      = (request.args.get("cal") or "today").strip().lower()   # today|yesterday|week|month|year
    limit    = int(request.args.get("limit","500") or 500)
    bucket_s = request.args.get("bucket_s","").strip()
    bucket_s = int(bucket_s) if bucket_s.isdigit() else None

    start_iso, end_iso = _bounds_for_calendar(cal)

    if bucket_s and bucket_s > 0:
        q = f"""
          WITH rows AS (
            SELECT substr(ts,1,19) AS s, tag, value, unit
            FROM logs
            WHERE ts >= ? AND ts <= ?
            {{tag_clause}}
          ),
          agg AS (
            SELECT s, tag, avg(value) AS value, MAX(unit) AS unit
            FROM rows
            GROUP BY tag, s
          )
          SELECT
            s AS ts,
            strftime('%Y-%m-%d %I:%M:%S %p', replace(s,'T',' ')) AS ts_fmt,
            tag, value, unit
          FROM agg
          ORDER BY ts DESC
          LIMIT ?
        """
    else:
        q = f"""
          SELECT
            ts,
            strftime('%Y-%m-%d %I:%M:%S %p', replace(ts,'T',' ')) AS ts_fmt,
            tag, value, unit
          FROM logs
          WHERE ts >= ? AND ts <= ?
          {{tag_clause}}
          ORDER BY ts DESC
          LIMIT ?
        """

    tag_clause = "" if not tag else "AND tag = ?"
    q = q.format(tag_clause=tag_clause)
    args = [start_iso, end_iso] + ([tag] if tag else []) + [limit]

    with db() as con:
        rows = [dict(r) for r in con.execute(q, args)]
    return jsonify(rows)

@api_bp.route("/download.csv")
def api_download_csv():
    tag      = (request.args.get("tag") or "").strip() or None
    cal      = (request.args.get("cal") or "today").strip().lower()
    limit    = int(request.args.get("limit","100000") or 100000)
    bucket_s = request.args.get("bucket_s","").strip()
    bucket_s = int(bucket_s) if bucket_s.isdigit() else None

    start_iso, end_iso = _bounds_for_calendar(cal)

    rows = query_logs(tag, mins, limit, bucket_s)
    csv_text = download_csv(rows)
    resp = make_response(csv_text)
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=logs.csv"
    return resp
