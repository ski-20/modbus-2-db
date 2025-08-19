#!/usr/bin/env python3
import sqlite3, time, csv, io, struct
from flask import Flask, request, jsonify, make_response
from datetime import datetime, timedelta

from pymodbus.client import ModbusTcpClient

# ======= keep these in sync with logger.py =======
DB = "/home/ele/plc_logger/plc.db"

PLC_IP   = "10.0.0.1"
PLC_PORT = 502
SLAVE_ID = 1
WORD_ORDER = "HL"   # "HL" => HI word then LO word; switch to "LH" if needed

SETPOINT_WINDOW_START = 300
SETPOINT_WINDOW_END   = 323
SETPOINT_COUNT = SETPOINT_WINDOW_END - SETPOINT_WINDOW_START + 1

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
# ==================================================

app = Flask(__name__)

# ---------- DB helpers ----------
def db():
    con = sqlite3.connect(DB, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=2000")
    return con

# ---------- Modbus helpers ----------
_client = None
def mb_client():
    global _client
    if _client is None:
        # IMPORTANT: keyword args for pymodbus 3.x
        _client = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
    if not getattr(_client, "connected", False):
        try:
            _client.connect()
        except Exception:
            pass
    return _client

def float_to_words(val):
    # pack >f then split to >HH; swap if WORD_ORDER == "LH"
    hi, lo = struct.unpack(">HH", struct.pack(">f", float(val)))
    return (lo, hi) if WORD_ORDER == "LH" else (hi, lo)

def words_to_float(hi, lo):
    if WORD_ORDER == "LH":
        hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

# ---------- Pages ----------
@app.route("/")
def home():
    """
    Simple HTML table of recent log rows with filters:
    /?tag=P1_MotorCurrent&mins=60&limit=200
    """
    tag  = request.args.get("tag", "").strip()
    mins = int(request.args.get("mins", "60") or "60")   # last N minutes
    limit = int(request.args.get("limit", "200") or "200")
    since_ts = (datetime.utcnow() - timedelta(minutes=mins)).isoformat()

    q = "SELECT ts, tag, value, unit FROM logs WHERE ts >= ?"
    args = [since_ts]
    if tag:
        q += " AND tag = ?"
        args.append(tag)
    q += " ORDER BY ts DESC LIMIT ?"
    args.append(limit)

    rows = []
    with db() as con:
        rows = list(con.execute(q, args))

    # quick tag list for dropdown
    with db() as con:
        tags = [r["tag"] for r in con.execute("SELECT DISTINCT tag FROM logs ORDER BY tag")]

    # minimal HTML (Bootstrap via CDN)
    html = """
<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<title>PLC Data Log</title>
</head><body class="p-3">
<div class="container-fluid">
  <h3>PLC Data Log</h3>
  <form class="row g-2 mb-3">
    <div class="col-sm-3">
      <label class="form-label">Tag</label>
      <select class="form-select" name="tag">
        <option value="">(all)</option>
        """ + "\n".join([f'<option value="{t}" {"selected" if t==tag else ""}>{t}</option>' for t in tags]) + """
      </select>
    </div>
    <div class="col-sm-2">
      <label class="form-label">Last (mins)</label>
      <input class="form-control" name="mins" value='""" + str(mins) + """'>
    </div>
    <div class="col-sm-2">
      <label class="form-label">Limit</label>
      <input class="form-control" name="limit" value='""" + str(limit) + """'>
    </div>
    <div class="col-sm-2 align-self-end">
      <button class="btn btn-primary w-100" type="submit">Apply</button>
    </div>
    <div class="col-sm-3 align-self-end">
      <a class="btn btn-outline-secondary w-100" href="/download.csv?tag=""" + tag + "&mins=" + str(mins) + "&limit=" + str(limit) + """">Download CSV</a>
    </div>
  </form>

  <div class="mb-2"><a class="btn btn-sm btn-outline-dark" href="/status_page">Status</a>
  <a class="btn btn-sm btn-outline-dark" href="/setpoints">Setpoints</a></div>

  <table class="table table-sm table-striped">
    <thead><tr><th>Timestamp (UTC)</th><th>Tag</th><th>Value</th><th>Unit</th></tr></thead>
    <tbody>
    """ + "\n".join([f"<tr><td>{r['ts']}</td><td>{r['tag']}</td><td>{r['value']}</td><td>{r['unit'] or ''}</td></tr>" for r in rows]) + """
    </tbody>
  </table>
</div></body></html>
"""
    return html

@app.route("/download.csv")
def download_csv():
    tag  = request.args.get("tag", "").strip()
    mins = int(request.args.get("mins", "60") or "60")
    limit = int(request.args.get("limit", "10000") or "10000")
    since_ts = (datetime.utcnow() - timedelta(minutes=mins)).isoformat()

    q = "SELECT ts, tag, value, unit FROM logs WHERE ts >= ?"
    args = [since_ts]
    if tag:
        q += " AND tag = ?"
        args.append(tag)
    q += " ORDER BY ts DESC LIMIT ?"
    args.append(limit)

    with db() as con:
        rows = list(con.execute(q, args))

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts","tag","value","unit"])
    for r in rows:
        w.writerow([r["ts"], r["tag"], r["value"], r["unit"] or ""])
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=logs.csv"
    return resp

# ---------- Status ----------
def _read_state():
    with db() as con:
        return {k: v for (k, v) in con.execute("SELECT key, value FROM state")}

@app.route("/status")
def status_json():
    s = _read_state()
    def fmt(t):
        try: return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(t)))
        except: return None
    out = {
        "connected": int(s.get("connected", 0)),
        "last_read_ok": int(s.get("last_read_ok", 0)),
        "consecutive_errors": int(s.get("consecutive_errors", 0)),
        "last_read_epoch": s.get("last_read_epoch"),
        "last_read_utc": fmt(s.get("last_read_epoch")),
        "last_flush_epoch": s.get("last_flush_epoch"),
        "last_flush_utc": fmt(s.get("last_flush_epoch")),
        "rows_written_last_flush": int(s.get("rows_written_last_flush", 0)),
        "server_time_utc": fmt(time.time()),
    }
    return jsonify(out)

@app.route("/status_page")
def status_page():
    s = _read_state()
    keys = ["connected","last_read_ok","consecutive_errors","last_read_epoch","last_flush_epoch","rows_written_last_flush"]
    rows = "".join([f"<tr><td>{k}</td><td>{s.get(k,'')}</td></tr>" for k in keys])
    return f"""<!doctype html><html><head>
<meta charset="utf-8"><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<title>Status</title></head><body class="p-3">
<div class="container">
<h3>Logger Status</h3>
<table class="table table-sm table-striped"><tbody>{rows}</tbody></table>
<p>JSON: <a href="/status">/status</a></p>
<p><a class="btn btn-outline-secondary" href="/">Back</a></p>
</div></body></html>"""

# ---------- Setpoints UI ----------
def read_setpoint_block():
    c = mb_client()
    rr = c.read_holding_registers(address=SETPOINT_WINDOW_START, count=SETPOINT_COUNT, slave=SLAVE_ID)
    if hasattr(rr, "isError") and rr.isError():
        return None
    return rr.registers

def get_setpoint_value(words, sp):
    i = sp["mw"] - SETPOINT_WINDOW_START
    if sp["type"] == "INT16":
        return words[i] if i >= 0 else None
    if sp["type"] == "FLOAT32":
        hi, lo = words[i], words[i+1]
        return words_to_float(hi, lo)
    return None

@app.route("/setpoints", methods=["GET", "POST"])
def setpoints():
    msg = ""
    if request.method == "POST":
        # Expect payload like {"name":"WetWell_Stop_Level","value": 5.5}
        name = request.form.get("name") or request.json.get("name")
        value = request.form.get("value") or request.json.get("value")
        try:
            value = float(value)
        except:
            return jsonify({"ok": False, "error": "invalid value"}), 400

        sp = next((s for s in SETPOINTS if s["name"] == name), None)
        if not sp:
            return jsonify({"ok": False, "error": "unknown setpoint"}), 400

        c = mb_client()
        if sp["type"] == "INT16":
            r = c.write_register(address=sp["mw"], value=int(value), slave=SLAVE_ID)
            ok = not (hasattr(r, "isError") and r.isError())
        else:
            hi, lo = float_to_words(value)
            r = c.write_registers(address=sp["mw"], values=[hi, lo], slave=SLAVE_ID)
            ok = not (hasattr(r, "isError") and r.isError())

        if ok:
            msg = f"Updated {name}."
        else:
            msg = f"Write failed for {name}."

    words = read_setpoint_block()
    rows = []
    if words:
        for sp in SETPOINTS:
            val = get_setpoint_value(words, sp)
            rows.append((sp["name"], sp["mw"], sp["type"], val))
    else:
        msg = msg or "Unable to read setpoints (Modbus error)."

    # produce simple HTML form
    table_rows = "".join([f"<tr><td>{n}</td><td>%MW{mw}</td><td>{typ}</td><td>{val}</td></tr>" for (n,mw,typ,val) in rows])
    names_opts = "".join([f'<option value="{sp["name"]}">{sp["name"]}</option>' for sp in SETPOINTS])

    html = f"""<!doctype html><html><head>
<meta charset="utf-8"><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
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
    return html

# (Optional) simple API to get list of tags
@app.route("/tags")
def tags():
    with db() as con:
        rows = [r["tag"] for r in con.execute("SELECT DISTINCT tag FROM logs ORDER BY tag")]
    return jsonify(rows)

if __name__ == "__main__":
    # Run with: python3 web.py
    app.run(host="0.0.0.0", port=8080)
