#!/usr/bin/env python3
import time, struct, sqlite3, threading
from datetime import datetime
from pymodbus.client import ModbusTcpClient

# ------------------ CONFIG ------------------
PLC_IP     = "10.0.0.1"
PLC_PORT   = 502
SLAVE_ID   = 1

DB = "/opt/plc_logger/plc.db"

# "HL" => [%MWn=HI, %MWn+1=LO]; flip to "LH" if PLC uses low-word first
WORD_ORDER = "HL"

# ===== Status block (read every 0.5 s) =====
# Pump 1 spans %MW400..%MW419, Pump 2 spans %MW420..%MW439
P1_BASE = 400
P2_BASE = 420

STATUS_WINDOW_START = 400
STATUS_WINDOW_END   = 449
STATUS_COUNT = STATUS_WINDOW_END - STATUS_WINDOW_START + 1

# Per-pump OUT_DATAWORD (%MW) — SET THESE to your real addresses
P1_OUT_WORD_MW = None   # e.g., 444
P2_OUT_WORD_MW = None   # e.g., 446

# System tag(s) — fixed cadence (independent of pumps)
SYSTEM_TAGS = [
    {"name":"WetWellLevel", "mw":440, "type":"FLOAT32", "scale":1.0, "unit":"level"},
    {"name":"SYS1_OutDataWord", "mw":442, "type":"INT16", "unit":""},
]
SYS_SEC    = 10.0   # system tags every 10 s

# Pump tag builder
def pump_tags(base, pump_name):
    return [
        {"name":f"{pump_name}_DrvStatusWord", "mw":base+0,  "type":"UINT16", "unit":""},
        {"name":f"{pump_name}_SpeedRaw",      "mw":base+1,  "type":"INT16",  "unit":"raw"},
        {"name":f"{pump_name}_MotorCurrent",  "mw":base+2,  "type":"INT16",  "scale":0.1, "unit":"A"},
        {"name":f"{pump_name}_DCBusV",        "mw":base+3,  "type":"INT16",  "unit":"V"},
        {"name":f"{pump_name}_OutV",          "mw":base+4,  "type":"INT16",  "unit":"V"},
        {"name":f"{pump_name}_TorqueRaw",     "mw":base+5,  "type":"INT16",  "unit":"raw"},
        {"name":f"{pump_name}_FaultActive",   "mw":base+6,  "type":"UINT16", "unit":""},
        {"name":f"{pump_name}_FaultPrev",     "mw":base+7,  "type":"UINT16", "unit":""},
        {"name":f"{pump_name}_Starts",        "mw":base+8,  "type":"INT32",  "unit":""},
        {"name":f"{pump_name}_Hours_x10",     "mw":base+10, "type":"INT32",  "unit":"tenth_hr"},
        {"name":f"{pump_name}_Status",        "mw":base+12, "type":"UINT16", "unit":""},   # 1 = running
        {"name":f"{pump_name}_Mode",          "mw":base+13, "type":"UINT16", "unit":""},
        {"name":f"{pump_name}_OutDataWord",   "mw":base+14, "type":"UINT16", "unit":""},
    ]


P1_TAGS = pump_tags(P1_BASE, "P1", P1_OUT_WORD_MW)
P2_TAGS = pump_tags(P2_BASE, "P2", P2_OUT_WORD_MW)

# Logging policy for pumps
FAST_SEC   = 1.0     # fast log cadence when that pump is running
SLOW_SEC   = 600.0   # slow log cadence when that pump is idle (10 min)
SAMPLE_SEC = 0.5     # Modbus sample cadence

# ===== Setpoints block (for web API) =====
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
# --------------------------------------------

# Shared client
_client_lock = threading.Lock()
_client = None
def get_client():
    global _client
    with _client_lock:
        if _client is None:
            _client = ModbusTcpClient(PLC_IP, PLC_PORT, timeout=2)
        if not _client.connected:
            _client.connect()
        return _client

# ------------- DB -------------
def ensure_schema():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS logs (
        ts   TEXT NOT NULL,
        tag  TEXT NOT NULL,
        value REAL,
        unit TEXT
      )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_tag_ts ON logs(tag, ts)")
    cur.execute("""
      CREATE TABLE IF NOT EXISTS state (
        key TEXT PRIMARY KEY,
        value REAL
      )
    """)
    con.commit(); con.close()

def write_rows(rows):
    if not rows: return
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    cur.executemany("INSERT INTO logs (ts, tag, value, unit) VALUES (?,?,?,?)", rows)
    con.commit(); con.close()

# ------------- Modbus helpers -------------
def read_words(start_mw, count):
    cli = get_client()
    rr = cli.read_holding_registers(start_mw, count, slave=SLAVE_ID)
    if rr.isError(): raise RuntimeError(rr)
    return rr.registers

def to_int16(w):   return w - 65536 if w >= 32768 else w
def to_uint16(w):  return w
def to_int32(hi, lo):
    if WORD_ORDER == "LH": hi, lo = lo, hi
    v = (hi << 16) | lo
    return v - (1<<32) if (v & 0x80000000) else v
def to_uint32(hi, lo):
    if WORD_ORDER == "LH": hi, lo = lo, hi
    return (hi << 16) | lo
def to_float32(hi, lo):
    if WORD_ORDER == "LH": hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def decode_in_status_window(words, mw, typ):
    i = mw - STATUS_WINDOW_START
    if i < 0 or i >= len(words): return None
    if typ == "INT16":   return float(to_int16(words[i]))
    if typ == "UINT16":  return float(to_uint16(words[i]))
    if typ in ("INT32","UINT32","FLOAT32"):
        if i+1 >= len(words): return None
        hi, lo = words[i], words[i+1]
        if typ == "INT32":   return float(to_int32(hi, lo))
        if typ == "UINT32":  return float(to_uint32(hi, lo))
        if typ == "FLOAT32": return float(to_float32(hi, lo))
    raise ValueError(f"bad type {typ}")

# ------------- Logging logic -------------
last_log_time = {}   # per-tag last write time (epoch seconds)
def due(tag_name, now_s, period_s):
    return (now_s - last_log_time.get(tag_name, 0.0)) >= period_s
def mark_logged(tag_name, now_s):
    last_log_time[tag_name] = now_s

def read_tag_from_words(words, tag):
    v = decode_in_status_window(words, tag["mw"], tag["type"].upper())
    if v is None: return None
    v *= float(tag.get("scale", 1.0))
    return v

def main():
    ensure_schema()
    pending, last_flush = [], time.time()

    while True:
        try:
            # Read %MW400..%MW449 once per cycle
            words = read_words(STATUS_WINDOW_START, STATUS_COUNT)
            now_iso = datetime.utcnow().isoformat()
            now_s   = time.time()

            # Running status (1 = running)
            p1_status = int(read_tag_from_words(words, {"mw":P1_BASE+12,"type":"UINT16"}) or 0)
            p2_status = int(read_tag_from_words(words, {"mw":P2_BASE+12,"type":"UINT16"}) or 0)

            # Per-pump cadence
            p1_cad = FAST_SEC if p1_status == 1 else SLOW_SEC
            p2_cad = FAST_SEC if p2_status == 1 else SLOW_SEC

            # Log pump 1
            for t in P1_TAGS:
                v = read_tag_from_words(words, t)
                if v is None: continue
                if due(t["name"], now_s, p1_cad):
                    pending.append((now_iso, t["name"], float(v), t.get("unit","")))
                    mark_logged(t["name"], now_s)

            # Log pump 2
            for t in P2_TAGS:
                v = read_tag_from_words(words, t)
                if v is None: continue
                if due(t["name"], now_s, p2_cad):
                    pending.append((now_iso, t["name"], float(v), t.get("unit","")))
                    mark_logged(t["name"], now_s)

            # Log system tags at fixed 10 s cadence
            for t in SYSTEM_TAGS:
                v = read_tag_from_words(words, t)
                if v is None: continue
                if due(t["name"], now_s, SYS_SEC):
                    pending.append((now_iso, t["name"], float(v), t.get("unit","")))
                    mark_logged(t["name"], now_s)

            # Batch flush ~1 Hz
            if time.time() - last_flush >= 1.0 and pending:
                write_rows(pending)
                pending.clear()
                last_flush = time.time()

            time.sleep(SAMPLE_SEC)

        except Exception:
            time.sleep(0.5)
            with _client_lock:
                if _client:
                    try: _client.close()
                    except: pass
                globals()["_client"] = None

if __name__ == "__main__":
    main()
