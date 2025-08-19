#!/usr/bin/env python3
import sqlite3, time, csv, io, struct, math
from flask import Flask, request, jsonify, make_response
from datetime import datetime, timedelta, timezone
from typing import Optional

# ======= CONFIG (keep in sync with logger.py) =======
DB = "/home/ele/plc_logger/plc.db"

# Timezone for display (DB stays UTC)
try:
    from zoneinfo import ZoneInfo   # Python 3.9+
    LOCAL_TZ = ZoneInfo("America/Chicago")   # change to America/New_York if desired
except Exception:
    from dateutil import tz
    LOCAL_TZ = tz.gettz("America/Chicago")

# Modbus setpoint write support (optional)
USE_MODBUS = True
PLC_IP   = "10.0.0.1"
PLC_PORT = 502
SLAVE_ID = 1
WORD_ORDER = "HL"

SETPOINTS = [
    {"name":"WetWell_Stop_Level",          "mw":300, "type":"FLOAT32"},
    {"name":"WetWell_Lead_Start_Level",    "mw":302, "type":"FLOAT32"},
    {"name":"WetWell_Lag_Start_Level",     "mw":304, "type":"FLOAT32"},
    {"name":"WetWell_High_Level",          "mw":306, "type":"FLOAT32"},
    {"name":"WetWell_Level_Scale_0V",      "mw":308, "type":"FLOAT32"},
    {"name":"WetWell_Level_Scale_10V",     "mw":310, "type":"FLOAT32"},
    {"name":"Spare_Analog_IO_1",           "mw":312, "type":"FLOAT32"},
    {"name":"Spare_Analog_IO_2",           "mw":314, "type":"FLOAT32"},
    {"name":"Pump1_Speed_Setpoint_pct",    "mw":316, "type":"FLOAT32"},
    {"name":"Pump2_Speed_Setpoint_pct",    "mw":318, "type":"FLOAT32"},
    {"name":"Pump1_FailToRun_Delay_sec",   "mw":320, "type":"INT16"},
    {"name":"Pump2_FailToRun_Delay_sec",   "mw":321, "type":"INT16"},
    {"name":"Spare_Analog_IO_HighLevel",   "mw":322, "type":"FLOAT32"},
]
SETPOINT_WINDOW_START = 300
SETPOINT_WINDOW_END   = 323
SETPOINT_COUNT = SETPOINT_WINDOW_END - SETPOINT_WINDOW_START + 1
# =====================================================

app = Flask(__name__, static_folder="static", static_url_path="/static")

# ---------- DB helpers ----------
def db():
    con = sqlite3.connect(DB, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=2000")
    return con

def list_tags():
    with db() as con:
        return [r["tag"] for r in con.execute("SELECT DISTINCT tag FROM logs ORDER BY tag")]

def tag_label_map():
    """Read pretty labels from logger-published tag_meta."""
    with db() as con:
        return {r["name"]: (r["label"] or r["name"]) for r in con.execute("SELECT name, label FROM tag_meta")}

# ---------- Basic pages ----------
@app.route("/")
def home():
    # keep selections on refresh
    cur_tag    = request.args.get("tag", "").strip()
    cur_mins   = request.args.get("mins", "60")
    cur_limit  = request.args.get("limit", "500")
    cur_bucket = request.args.get("bucket_s", "")

    tags = list_tags()
    labels = tag_label_map()
    options = ['<option value="">(all)</option>']
    for t in tags:
        label = labels.get(t, t)
        sel = "selected" if t == cur_tag else ""
        options.append(f'<option value="{t}" {sel}>{label}</option>')

    return f"""
<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/static/bootstrap.min.css" rel="stylesheet">
<title>PLC Logger UI</title>
</head><body class="p-3">
<div class="container-fluid">
  <h3>PLC Logger</h3>

  <div class="mb-3">
    <a class="btn btn-outline-secondary btn-sm" href="/status_page">Status</a>
    <a class="btn btn-outline-secondary btn-sm" href="/setpoints">Setpoints</a>
  </div>

  <form id="f" class="row g-2 mb-3">
    <div class="col-sm-4">
      <label class="form-label">Tag</label>
      <select class="form-select" name="tag">
        {''.join(options)}
      </select>
    </div>
    <div class="col-sm-2">
      <label class="form-label">Last minutes</label>
      <input class="form-control" name="mins" value="{cur_mins}">
    </div>
    <div class="col-sm-2">
      <label class="form-label">Limit</label>
      <input class="form-control" name="limit" value="{cur_limit}">
    </div>
    <div class="col-sm-2">
      <label class="form-label">Bucket (sec, optional)</label>
      <input class="form-control" name="bucket_s" value="{cur_bucket}">
    </div>
    <div class="col-sm-2 align-self-end">
      <button class="btn btn-primary w-100" type="submit">Load</button>
    </div>
  </form>

  <div class="mb-2">
    <a id="dl" class="btn btn-outline-secondary btn-sm" href="#">Download CSV</a>
  </div>

  <div class="table-responsive">
    <table class="table table-sm table-striped" id="tbl">
      <thead><tr><th>Timestamp (Local)</th><th>Tag</th><th>Value</th><th>Unit</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<script>
const f = document.getElementById('f');
const tbody = document.querySelector('#tbl tbody');
const dl = document.getElementById('dl');

async function loadTable() {{
  const p = new URLSearchParams(new FormData(f));
  dl.href = '/api/download.csv?' + p.toString();
  const r = await fetch('/api/logs?' + p.toString());
  const rows = await r.json();
  tbody.innerHTML = rows.map(function(row) {{
    return '<tr>'
      + '<td>' + (row.ts_fmt || row.ts) + '</td>'
      + '<td>' + (row.tag_label || row.tag) + '</td>'
      + '<td>' + row.value + '</td>'
      + '<td>' + (row.unit || '') + '</td>'
      + '</tr>';
  }}).join('');
}}
f.addEventListener('submit', function(e) {{ e.preventDefault(); loadTable(); }});
loadTable();
</script>
</body></html>
"""

# ---------- Status ----------
def _read_state():
    try:
        with db() as con:
            return {k: v for (k, v) in con.execute("SELECT key, value FROM state")}
    except Exception:
        return {}

@app.route("/status")
def status_json():
    s = _read_state()
    def fmt(ts):
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(LOCAL_TZ)
            return dt.strftime("%Y-%m-%d %I:%M:%S %p")
        except:
            return None
    out = {
        "connected": int(s.get("connected", 0)),
        "last_read_ok": int(s.get("last_read_ok", 0)),
        "consecutive_errors": int(s.get("consecutive_errors", 0)),
        "last_read_epoch": s.get("last_read_epoch"),
        "last_read_local": fmt(s.get("last_read_epoch")),
        "last_flush_epoch": s.get("last_flush_epoch"),
        "last_flush_local": fmt(s.get("last_flush_epoch")),
        "rows_written_last_flush": int(s.get("rows_written_last_flush", 0)),
        "server_time_local": fmt(time.time()),
    }
    return jsonify(out)

@app.route("/status_page")
def status_page():
    s = _read_state()
    keys = ["connected","last_read_ok","consecutive_errors","last_read_epoch","last_flush_epoch","rows_written_last_flush"]
    rows = "".join([f"<tr><td>{k}</td><td>{s.get(k,'')}</td></tr>" for k in keys])
    return f"""<!doctype html><html><head>
<meta charset="utf-8"><link href="/static/bootstrap.min.css" rel="stylesheet">
<title>Status</title></head><body class="p-3">
<div class="container">
<h3>Logger Status</h3>
<table class="table table-sm table-striped"><tbody>{rows}</tbody></table>
<p>JSON: <a href="/status">/status</a></p>
<p><a class="btn btn-outline-secondary" href="/">Back</a></p>
</div></body></html>"""

# ---------- Logs API with optional bucketing ----------
def _query_logs(tag: str, mins: int, limit: int, bucket_s: Optional[int]):
    since_ts = (datetime.utcnow() - timedelta(minutes=mins)).isoformat()

    if bucket_s and bucket_s > 0:
        q = """
        WITH rows AS (
          SELECT substr(ts,1,19) AS s, tag, value, unit
          FROM logs
          WHERE ts >= ?
          {tag_clause}
        ),
        agg AS (
          SELECT s AS ts, tag, avg(value) AS value, MAX(unit) AS unit
          FROM rows
          GROUP BY tag, s
        )
        SELECT ts, tag, value, unit
        FROM agg
        ORDER BY ts DESC
        LIMIT ?
        """
        tag_clause = "" if not tag else "AND tag = ?"
        q = q.format(tag_clause=tag_clause)
        args = [since_ts] + ([tag] if tag else []) + [limit]
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
        q = q.format(tag_clause=tag_clause)
        args = [since_ts] + ([tag] if tag else []) + [limit]

    with db() as con:
        rows = [dict(r) for r in con.execute(q, args)]

    # Add human-friendly ts_fmt in Python + tag_label from tag_meta
    labels = tag_label_map()
    out = []
    for r in rows:
        raw = r.get("ts", "")
        base = raw.split(".")[0].replace(" ", "T")
        try:
            dt = datetime.fromisoformat(base)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)  # treat stored ts as UTC
            dt_local = dt.astimezone(LOCAL_TZ)
            r["ts_fmt"] = dt_local.strftime("%Y-%m-%d %I:%M:%S %p")
        except Exception:
            r["ts_fmt"] = raw
        r["tag_label"] = labels.get(r["tag"], r["tag"])
        out.append(r)
    return out

@app.route("/api/logs")
def api_logs():
    tag   = request.args.get("tag","").strip() or None
    mins  = int(request.args.get("mins","60") or 60)
    limit = int(request.args.get("limit","500") or 500)
    bucket_s = request.args.get("bucket_s","").strip()
    bucket_s = int(bucket_s) if bucket_s.isdigit() else None
    rows = _query_logs(tag, mins, limit, bucket_s)
    return jsonify(rows)

@app.route("/api/download.csv")
def api_download_csv():
    tag   = request.args.get("tag","").strip() or None
    mins  = int(request.args.get("mins","60") or 60)
    limit = int(request.args.get("limit","100000") or 100000)
    bucket_s = request.args.get("bucket_s","").strip()
    bucket_s = int(bucket_s) if bucket_s.isdigit() else None

    rows = _query_logs(tag, mins, limit, bucket_s)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts","tag","value","unit"])
    for r in rows:
        w.writerow([r["ts"], r["tag"], r["value"], r.get("unit","")])
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=logs.csv"
    return resp

# ---------- Setpoints (safe even if PLC offline) ----------
try:
    from pymodbus.client import ModbusTcpClient
except Exception:
    USE_MODBUS = False

_mb = None
def mb_client():
    if not USE_MODBUS:
        return None
    global _mb
    if _mb is None:
        _mb = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
    if not getattr(_mb, "connected", False):
        try: _mb.connect()
        except Exception: pass
    return _mb

def float_to_words(val):
    hi, lo = struct.unpack(">HH", struct.pack(">f", float(val)))
    return (lo, hi) if WORD_ORDER == "LH" else (hi, lo)

def words_to_float(hi, lo):
    if WORD_ORDER == "LH":
        hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def read_setpoint_block():
    c = mb_client()
    if not c:
        return None, "Modbus client not available"
    try:
        rr = c.read_holding_registers(address=SETPOINT_WINDOW_START, count=SETPOINT_COUNT, slave=SLAVE_ID)
        if hasattr(rr,"isError") and rr.isError():
            return None, f"Modbus read error: {rr}"
        return rr.registers, None
    except Exception as e:
        return None, f"Modbus exception: {e}"

@app.route("/setpoints", methods=["GET","POST"])
def setpoints():
    msg = ""
    labels = tag_label_map()
    if request.method == "POST":
        if not USE_MODBUS:
            return make_response("Modbus not enabled on server", 500)
        name = (request.form.get("name") or (request.json or {}).get("name") or "").strip()
        value = (request.form.get("value") or (request.json or {}).get("value") or "").strip()
        sp = next((s for s in SETPOINTS if s["name"] == name), None)
        try:
            fval = float(value)
        except:
            fval = None
        if not sp or fval is None:
            msg = "Invalid name or value"
        else:
            c = mb_client()
            try:
                if sp["type"] == "INT16":
                    r = c.write_register(address=sp["mw"], value=int(fval), slave=SLAVE_ID)
                    ok = not (hasattr(r,"isError") and r.isError())
                else:
                    hi, lo = float_to_words(fval)
                    r = c.write_registers(address=sp["mw"], values=[hi, lo], slave=SLAVE_ID)
                    ok = not (hasattr(r,"isError") and r.isError())
                msg = f"Updated {labels.get(name,name)}" if ok else f"Write failed for {labels.get(name,name)}"
            except Exception as e:
                msg = f"Write exception: {e}"

    words, emsg = read_setpoint_block()
    rows = []
    if words:
        for sp in SETPOINTS:
            i = sp["mw"] - SETPOINT_WINDOW_START
            val = None
            try:
                if sp["type"] == "INT16":
                    val = words[i]
                elif sp["type"] == "FLOAT32":
                    val = words_to_float(words[i], words[i+1])
            except Exception:
                val = None
            rows.append((sp["name"], sp["mw"], sp["type"], val))
    else:
        msg = emsg or msg or "Unable to read setpoints."

    names_opts = "".join([
        f'<option value="{sp["name"]}">{labels.get(sp["name"], sp["name"])}</option>'
        for sp in SETPOINTS
    ])
    table_rows = "".join([
        f"<tr><td>{labels.get(n,n)}</td><td>%MW{mw}</td><td>{typ}</td><td>{val}</td></tr>"
        for (n,mw,typ,val) in rows
    ])

    return f"""<!doctype html><html><head>
<meta charset="utf-8"><link href="/static/bootstrap.min.css" rel="stylesheet">
<title>Setpoints</title></head><body class="p-3">
<div class="container">
  <h3>Setpoints</h3>
  {"<div class='alert alert-info'>"+msg+"</div>" if msg else ""}
  <form method="post" class="row g-2 mb-3">
    <div class="col-sm-5">
      <label class="form-label">Name</label>
      <select class="form-select" name="name">{names_opts}</select>
    </div>
    <div class="col-sm-3">
      <label class="form-label">Value</label>
      <input class="form-control" name="value" required>
    </div>
    <div class="col-sm-2 align-self-end">
      <button class="btn btn-primary w-100" type="submit">Write</button>
    </div>
    <div class="col-sm-2 align-self-end">
      <a class="btn btn-outline-secondary w-100" href="/setpoints">Refresh</a>
    </div>
  </form>

  <table class="table table-sm table-striped">
    <thead><tr><th>Name</th><th>MW</th><th>Type</th><th>Current</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <p><a class="btn btn-outline-secondary" href="/">Back</a></p>
</div></body></html>"""

# health
@app.route("/health")
def health():
    try:
        with db() as con:
            con.execute("SELECT 1")
        return "ok", 200
    except Exception as e:
        return f"db error: {e}", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
