#!/usr/bin/env python3
import time, struct, sqlite3, threading, random
from datetime import datetime
from pymodbus.client import ModbusTcpClient

# ------------------ CONFIG ------------------
from config import DB, PLC_IP, PLC_PORT, SLAVE_ID, WORD_ORDER
LOG_NAME = "modbus_logger"

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(LOG_NAME)

# Polling intervals
FAST_SEC = 1      # fast cycle for high-priority tags
SLOW_SEC = 10     # slow cycle for less frequent tags

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
from tags import P1_TAGS, P2_TAGS, SYSTEM_TAGS, SETPOINTS, P1_BASE, P2_BASE

def _validate_tags(name, tags):
    req = {"name","mw"}
    for t in tags:
        missing = [k for k in req if k not in t]
        if missing:
            log.error(f"{name}: tag missing {missing}: {t}")
        if "type" not in t and "dtype" not in t:
            log.error(f"{name}: tag missing 'type'/'dtype': {t}")

_validate_tags("P1_TAGS", P1_TAGS)
_validate_tags("P2_TAGS", P2_TAGS)
_validate_tags("SYSTEM_TAGS", SYSTEM_TAGS)

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
    try:
        rr = cli.read_holding_registers(address=start_mw, count=count)
    except Exception as e:
        raise RuntimeError(f"Socket/transport error reading %MW{start_mw}..%MW{start_mw+count-1}: {e}")

    if rr is None:
        raise RuntimeError(f"No response reading %MW{start_mw}..%MW{start_mw+count-1}")
    if hasattr(rr, "isError") and rr.isError():
        fc = getattr(rr, "function_code", None)
        ec = getattr(rr, "exception_code", None)
        raise RuntimeError(
            f"Modbus exception %MW{start_mw}..%MW{start_mw+count-1}: function={fc} exception={ec} ({rr})"
        )
    if not hasattr(rr, "registers") or rr.registers is None:
        raise RuntimeError(f"Malformed response for %MW{start_mw}..%MW{start_mw+count-1}: {rr!r}")
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
    # tolerate either "type" (logger-defined) or "dtype" (DB/tag_meta-defined)
    typ = (tag.get("type") or tag.get("dtype"))
    if not typ:
        raise KeyError(f"Tag missing 'type'/'dtype': name={tag.get('name')} mw={tag.get('mw')} tag={tag}")
    v = decode_in_status_window(words, tag["mw"], typ.upper())
    if v is None:
        return None
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
