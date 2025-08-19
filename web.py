#!/usr/bin/env python3
from flask import Flask, request, jsonify, render_template_string
import sqlite3, struct, time
from pymodbus.client import ModbusTcpClient
from logger import (DB, PLC_IP, PLC_PORT, SLAVE_ID, WORD_ORDER,
                    SETPOINTS, SETPOINT_WINDOW_START, SETPOINT_WINDOW_END,
                    get_client, _client_lock)

app = Flask(__name__)

TABLE_HTML = """
<!doctype html><title>PLC Logs</title>
<h3>Latest {{limit}} rows</h3>
<table border="1" cellpadding="4" cellspacing="0">
<tr><th>Timestamp (UTC)</th><th>Tag</th><th>Value</th><th>Unit</th></tr>
{% for r in rows %}
<tr><td>{{r[0]}}</td><td>{{r[1]}}</td><td>{{r[2]}}</td><td>{{r[3]}}</td></tr>
{% endfor %}
</table>
"""

# ---------- DB views ----------
@app.route("/")
def latest():
    limit = int(request.args.get("limit", "500"))
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT ts, tag, value, unit FROM logs ORDER BY ts DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return render_template_string(TABLE_HTML, rows=rows, limit=limit)

@app.route("/aligned")
def aligned():
    # /aligned?tags=P1_MotorCurrent,P2_MotorCurrent&start=YYYY-mm-dd HH:MM:SS&end=...&bucket=60
    tags = request.args.get("tags", "P1_MotorCurrent").split(",")
    start = request.args.get("start", "2025-01-01 00:00:00")
    end   = request.args.get("end",   "2099-12-31 23:59:59")
    bucket = int(request.args.get("bucket", "60"))
    cols = ",\n  ".join(
        [f"(SELECT value FROM logs WHERE tag='{t}' AND ts<=b.t ORDER BY ts DESC LIMIT 1) AS \"{t}\""
         for t in tags]
    )
    sql = f"""
    WITH RECURSIVE buckets(t) AS (
      SELECT datetime('{start}')
      UNION ALL
      SELECT datetime(strftime('%s', t) + {bucket}, 'unixepoch') FROM buckets
      WHERE t < datetime('{end}')
    )
    SELECT b.t AS ts, {cols}
    FROM buckets b
    ORDER BY b.t;
    """
    con = sqlite3.connect(DB); cur = con.cursor()
    rows = cur.execute(sql).fetchall()
    con.close()
    out_cols = ["ts"] + tags
    return jsonify([dict(zip(out_cols, r)) for r in rows])

# ---------- Setpoints (read/write) ----------
def words_to_float32(hi, lo):
    if WORD_ORDER == "LH": hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def float32_to_words(val):
    hb, lb = struct.unpack(">HH", struct.pack(">f", float(val)))
    if WORD_ORDER == "LH": hb, lb = lb, hb
    return hb, lb

@app.route("/setpoints", methods=["GET"])
def get_setpoints():
    cli = get_client()
    # Read the whole window once
    rr = cli.read_holding_registers(SETPOINT_WINDOW_START,
                                    SETPOINT_WINDOW_END-SETPOINT_WINDOW_START+1,
                                    slave=SLAVE_ID)
    if rr.isError():
        return {"error": str(rr)}, 500
    regs = rr.registers
    out = {}
    for sp in SETPOINTS:
        i = sp["mw"] - SETPOINT_WINDOW_START
        t = sp["type"].upper()
        if t == "INT16":
            out[sp["name"]] = regs[i] if regs[i] < 32768 else regs[i]-65536
        elif t == "UINT16":
            out[sp["name"]] = regs[i]
        elif t == "FLOAT32":
            out[sp["name"]] = words_to_float32(regs[i], regs[i+1])
        elif t == "INT32":
            hi, lo = regs[i], regs[i+1]
            v = (hi<<16) | lo
            out[sp["name"]] = v- (1<<32) if (v & 0x80000000) else v
        else:
            out[sp["name"]] = None
    return out

@app.route("/setpoints", methods=["POST"])
def write_setpoints():
    """
    JSON body: {"name":"Pump1_Speed_Setpoint_pct","value":55.0}
    """
    data = request.get_json(force=True)
    name = data.get("name"); val = data.get("value")
    sp = next((s for s in SETPOINTS if s["name"] == name), None)
    if not sp: return {"error":"unknown setpoint"}, 400
    mw = sp["mw"]; typ = sp["type"].upper()
    cli = get_client()
    if typ in ("INT16","UINT16"):
        v = int(val) & 0xFFFF
        rq = cli.write_register(mw, v, slave=SLAVE_ID)
    elif typ == "FLOAT32":
        hi, lo = float32_to_words(val)
        rq = cli.write_registers(mw, [hi, lo], slave=SLAVE_ID)
    elif typ == "INT32":
        v = int(val) & 0xFFFFFFFF
        hi, lo = (v >> 16) & 0xFFFF, v & 0xFFFF
        if WORD_ORDER == "LH": hi, lo = lo, hi
        rq = cli.write_registers(mw, [hi, lo], slave=SLAVE_ID)
    else:
        return {"error":"unsupported type"}, 400
    if rq.isError(): return {"error": str(rq)}, 500
    return {"ok": True}

# ---------- Fault reset (%M0 coil pulsed) ----------
@app.route("/fault_reset", methods=["POST"])
def fault_reset():
    cli = get_client()
    # %M0 is Coil 0 in Modbus (coil addressing)
    rq1 = cli.write_coil(0, True, slave=SLAVE_ID)
    if rq1.isError(): return {"error": str(rq1)}, 500
    time.sleep(0.2)
    rq2 = cli.write_coil(0, False, slave=SLAVE_ID)
    if rq2.isError(): return {"error": str(rq2)}, 500
    return {"ok": True}
    
if __name__ == "__main__":
    # Run: python3 -m flask --app web run --host=0.0.0.0 --port=8080
    app.run(host="0.0.0.0", port=8080, debug=False)
