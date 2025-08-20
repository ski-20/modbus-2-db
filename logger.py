#!/usr/bin/env python3
import time, struct, sqlite3, threading, random
from datetime import datetime
from pymodbus.client import ModbusTcpClient

# ------------------ CONFIG ------------------
PLC_IP     = "10.0.0.1"
PLC_PORT   = 502
SLAVE_ID   = 1

DB = "/home/ele/plc_logger/plc.db"
LOG_NAME = "modbus_logger"

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(LOG_NAME)

# "HL" => [%MWn=HI, %MWn+1=LO]; flip to "LH" if PLC uses low-word first
WORD_ORDER = "HL"

# ===== Status block (read every 0.5 s) =====
P1_BASE = 400
P2_BASE = 420

STATUS_WINDOW_START = 400
STATUS_WINDOW_END   = 449
STATUS_COUNT = STATUS_WINDOW_END - STATUS_WINDOW_START + 1

# Per-pump OUT_DATAWORD placeholders (not used but kept for completeness)
P1_OUT_WORD_MW = None
P2_OUT_WORD_MW = None

# ---------- Tag definitions (with labels) ----------
def pump_tags(base: int, pump_key: str, pump_label: str):
    return [
        {"name":f"{pump_key}_DrvStatusWord", "label":f"{pump_label} Drive Status Word",         "mw":base+0,  "type":"UINT16", "unit":""},
        {"name":f"{pump_key}_SpeedRaw",      "label":f"{pump_label} Speed (raw)",               "mw":base+1,  "type":"INT16",  "unit":"raw"},
        {"name":f"{pump_key}_MotorCurrent",  "label":f"{pump_label} Motor Current",             "mw":base+2,  "type":"INT16",  "scale":0.1, "unit":"A"},
        {"name":f"{pump_key}_DCBusV",        "label":f"{pump_label} DC Bus Voltage",            "mw":base+3,  "type":"INT16",  "unit":"V"},
        {"name":f"{pump_key}_OutV",          "label":f"{pump_label} Output Voltage",            "mw":base+4,  "type":"INT16",  "unit":"V"},
        {"name":f"{pump_key}_TorqueRaw",     "label":f"{pump_label} Torque (raw)",              "mw":base+5,  "type":"INT16",  "unit":"raw"},
        {"name":f"{pump_key}_FaultActive",   "label":f"{pump_label} Active Fault",              "mw":base+6,  "type":"UINT16", "unit":""},
        {"name":f"{pump_key}_FaultPrev",     "label":f"{pump_label} Previous Fault",            "mw":base+7,  "type":"UINT16", "unit":""},
        {"name":f"{pump_key}_Starts",        "label":f"{pump_label} Total Starts",              "mw":base+8,  "type":"INT32",  "unit":""},
        {"name":f"{pump_key}_Hours_x10",     "label":f"{pump_label} Total Hours (x10)",         "mw":base+10, "type":"INT32",  "unit":"tenth_hr"},
        {"name":f"{pump_key}_Status",        "label":f"{pump_label} Status (1=Running)",        "mw":base+12, "type":"UINT16", "unit":""},
        {"name":f"{pump_key}_Mode",          "label":f"{pump_label} Mode",                      "mw":base+13, "type":"UINT16", "unit":""},
        {"name":f"{pump_key}_OutDataWord",   "label":f"{pump_label} Output Data Word",          "mw":base+14, "type":"UINT16", "unit":""},
    ]

P1_TAGS = pump_tags(P1_BASE, "P1", "Pump 1")
P2_TAGS = pump_tags(P2_BASE, "P2", "Pump 2")

SYSTEM_TAGS = [
    {"name":"WetWellLevel",     "label":"Wet Well Level",           "mw":440, "type":"FLOAT32", "scale":1.0, "unit":"level"},
    {"name":"SYS1_OutDataWord", "label":"System Output Data Word",  "mw":442, "type":"INT16",               "unit":""},
]
SYS_SEC    = 10.0   # system tags every 10 s

# Logging policy for pumps
FAST_SEC   = 1.0     # fast log cadence when that pump is running
SLOW_SEC   = 600.0   # slow log cadence when that pump is idle (10 min)
SAMPLE_SEC = 0.5     # Modbus sample cadence

# ===== Setpoints block (for web API) =====
SETPOINTS = [
    {"name":"WetWell_Stop_Level",        "label":"Wet Well Stop Level",                 "mw":300, "type":"FLOAT32"},
    {"name":"WetWell_Lead_Start_Level",  "label":"Wet Well Lead Pump Start Level",      "mw":302, "type":"FLOAT32"},
    {"name":"WetWell_Lag_Start_Level",   "label":"Wet Well Lag Pump Start Level",       "mw":304, "type":"FLOAT32"},
    {"name":"WetWell_High_Level",        "label":"Wet Well High Level",                 "mw":306, "type":"FLOAT32"},
    {"name":"WetWell_Level_Scale_0V",    "label":"Wet Well Level Scaling - 0V",         "mw":308, "type":"FLOAT32"},
    {"name":"WetWell_Level_Scale_10V",   "label":"Wet Well Level Scaling - 10V",        "mw":310, "type":"FLOAT32"},
    {"name":"Spare_Analog_IO_1",         "label":"Spare (future analog IO) 1",          "mw":312, "type":"FLOAT32"},
    {"name":"Spare_Analog_IO_2",         "label":"Spare (future analog IO) 2",          "mw":314, "type":"FLOAT32"},
    {"name":"Pump1_Speed_Setpoint_pct",  "label":"Pump 1 Speed Setpoint (%)",           "mw":316, "type":"FLOAT32"},
    {"name":"Pump2_Speed_Setpoint_pct",  "label":"Pump 2 Speed Setpoint (%)",           "mw":318, "type":"FLOAT32"},
    {"name":"Pump1_FailToRun_Delay_sec", "label":"Pump 1 Fail To Run Delay (sec.)",     "mw":320, "type":"INT16"},
    {"name":"Pump2_FailToRun_Delay_sec", "label":"Pump 2 Fail To Run Delay (sec.)",     "mw":321, "type":"INT16"},
    {"name":"Spare_Analog_IO_HighLevel", "label":"Spare (future analog IO) High Level", "mw":322, "type":"FLOAT32"},
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
            # keyword args for pymodbus 3.x
            _client = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
        connected = getattr(_client, "connected", None)
        if connected is None:
            try:
                connected = _client.is_socket_open()
            except Exception:
                connected = False
        if not connected:
            _client.connect()
        return _client

# ------------- DB -------------
def ensure_schema():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    # Concurrency / durability
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=2000;")
    cur.execute("""
      CREATE TABLE IF NOT EXISTS logs (
        ts   TEXT NOT NULL,
        tag  TEXT NOT NULL,
        value REAL,
        unit TEXT
      )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_tag_ts ON logs(tag, ts)")
    # health/status
    cur.execute("""
      CREATE TABLE IF NOT EXISTS state (
        key TEXT PRIMARY KEY,
        value REAL
      )
    """)
    # tag metadata (stable key + pretty label + unit)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS tag_meta (
        name  TEXT PRIMARY KEY,
        label TEXT,
        unit  TEXT
      )
    """)
    con.commit(); con.close()

def upsert_tag_meta(tag_defs):
    if not tag_defs: return
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    cur.executemany("""
      INSERT INTO tag_meta(name, label, unit) VALUES(?,?,?)
      ON CONFLICT(name) DO UPDATE SET label=excluded.label, unit=excluded.unit
    """, [(t["name"], t.get("label", t["name"]), t.get("unit","")) for t in tag_defs])
    con.commit(); con.close()

def write_rows(rows):
    if not rows: return
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    cur.execute("PRAGMA busy_timeout=2000;")
    cur.executemany("INSERT INTO logs (ts, tag, value, unit) VALUES (?,?,?,?)", rows)
    con.commit(); con.close()

# ------------- Modbus helpers -------------
def read_words(start_mw, count):
    cli = get_client()
    rr = cli.read_holding_registers(address=start_mw, count=count, slave=SLAVE_ID)
    if hasattr(rr, "isError") and rr.isError():
        fc = getattr(rr, "function_code", None)
        ec = getattr(rr, "exception_code", None)
        raise RuntimeError(f"Modbus exception: function={fc} exception={ec} addr={start_mw} count={count}")
    if rr is None or not hasattr(rr, "registers"):
        raise RuntimeError(f"Modbus read returned no data for address={start_mw} count={count}")
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

    # one-time publish of metadata so the web UI can read labels/units
    all_tags = P1_TAGS + P2_TAGS + SYSTEM_TAGS + SETPOINTS
    upsert_tag_meta(all_tags)

    # helpful debug to see counts
    p1_count = sum(1 for t in all_tags if t["name"].startswith("P1_"))
    p2_count = sum(1 for t in all_tags if t["name"].startswith("P2_"))
    sys_count = sum(1 for t in all_tags if not t["name"].startswith(("P1_","P2_")))
    log.info(f"tag_meta upserted: P1={p1_count} P2={p2_count} other={sys_count}")

    pending, last_flush = [], time.time()
    consecutive_errors = 0
    error_backoff_min = 0.5
    error_backoff_max = 5.0

    while True:
        try:
            words = read_words(STATUS_WINDOW_START, STATUS_COUNT)
            now_iso = datetime.utcnow().isoformat()
            now_s   = time.time()

            # determine pump run/idle
            p1_status = int(read_tag_from_words(words, {"mw":P1_BASE+12,"type":"UINT16"}) or 0)
            p2_status = int(read_tag_from_words(words, {"mw":P2_BASE+12,"type":"UINT16"}) or 0)
            p1_cad = FAST_SEC if p1_status == 1 else SLOW_SEC
            p2_cad = FAST_SEC if p2_status == 1 else SLOW_SEC

            # pump 1
            for t in P1_TAGS:
                v = read_tag_from_words(words, t)
                if v is None: continue
                if due(t["name"], now_s, p1_cad):
                    pending.append((now_iso, t["name"], float(v), t.get("unit","")))
                    mark_logged(t["name"], now_s)

            # pump 2
            for t in P2_TAGS:
                v = read_tag_from_words(words, t)
                if v is None: continue
                if due(t["name"], now_s, p2_cad):
                    pending.append((now_iso, t["name"], float(v), t.get("unit","")))
                    mark_logged(t["name"], now_s)

            # system tags fixed cadence
            for t in SYSTEM_TAGS:
                v = read_tag_from_words(words, t)
                if v is None: continue
                if due(t["name"], now_s, SYS_SEC):
                    pending.append((now_iso, t["name"], float(v), t.get("unit","")))
                    mark_logged(t["name"], now_s)

            # flush roughly once per second
            if time.time() - last_flush >= 1.0 and pending:
                write_rows(pending)
                set_state_many = [
                    ("connected", 1),
                    ("last_read_ok", 1),
                    ("consecutive_errors", consecutive_errors),
                    ("last_read_epoch", now_s),
                    ("last_flush_epoch", time.time()),
                    ("rows_written_last_flush", len(pending)),
                ]
                # write to state
                con = sqlite3.connect(DB, timeout=30); cur = con.cursor()
                cur.executemany("""
                    INSERT INTO state(key, value) VALUES(?,?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """, [(k, float(v)) for k, v in set_state_many])
                con.commit(); con.close()

                pending.clear()
                last_flush = time.time()

            consecutive_errors = 0
            time.sleep(SAMPLE_SEC)

        except Exception as e:
            consecutive_errors += 1
            backoff = min(error_backoff_min * (2 ** min(consecutive_errors, 6)), error_backoff_max)
            backoff += random.uniform(0, 0.2)

            if consecutive_errors in (1, 5) or consecutive_errors % 20 == 0:
                log.warning(f"Modbus read error (#{consecutive_errors}): {e} — backing off {backoff:.2f}s")

            # update state on error
            con = sqlite3.connect(DB, timeout=30); cur = con.cursor()
            cur.executemany("""
                INSERT INTO state(key, value) VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, [("connected", 0), ("last_read_ok", 0), ("consecutive_errors", consecutive_errors)])
            con.commit(); con.close()

            time.sleep(backoff)

            # force reconnect
            with _client_lock:
                if _client:
                    try: _client.close()
                    except: pass
                globals()["_client"] = None

if __name__ == "__main__":
    main()
