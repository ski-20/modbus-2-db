#!/usr/bin/env python3
import time, struct, sqlite3, threading, random, os, shutil
from datetime import datetime, timezone
import logging

# ------------------ CONFIG ------------------
# Prefer config.py if you have it
try:
    from config import DB, PLC_IP, PLC_PORT, WORD_ORDER, RETENTION
except Exception:
    DB = "/home/ele/plc_logger/plc.db"
    PLC_IP, PLC_PORT = "10.0.0.1", 502
    WORD_ORDER = "LH"   # "HL" = hi-word first in %MWn, %MWn+1; use "LH" if low-word first
    RETENTION = {}  # fall back to builtin defaults

# retention/cleanup
from retention import RetentionConfig, enforce_quota_periodic

ret_cfg = RetentionConfig(
    db_path=DB,
    max_db_mb=RETENTION.get("max_db_mb", 512),
    raw_keep_days=RETENTION.get("raw_keep_days", 14),
    delete_batch=RETENTION.get("delete_batch", 10_000),
    enforce_every_s=RETENTION.get("enforce_every_s", 300),
    incremental_vacuum_pages=RETENTION.get("incremental_vacuum_pages", 2000),
    primary_purge_tags=RETENTION.get("primary_purge_tags", ["SYS_WetWellLevel"]),
)

# default absolute deadband for on change tag logging, should never really need to change
DEFAULT_DB_ABS = 0.05

# Tags + cadences come from tags.py
from tags import TAGS, FAST_SEC, SAMPLE_SEC

LOG_NAME = "modbus_logger"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(LOG_NAME)

# ------------------ Modbus client ------------------
from pymodbus.client import ModbusTcpClient
_client_lock = threading.Lock()
_client = None

def get_client():
    """Get or (re)connect Modbus TCP client."""
    global _client
    with _client_lock:
        if _client is None:
            _client = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
        if not getattr(_client, "connected", False):
            _client.connect()
        return _client

# ------------------ DB helpers ------------------
def ensure_schema():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=2000;")

    # Is this a brand-new DB (no tables yet)?
    cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
    fresh_db = (cur.fetchone()[0] == 0)

    # For a brand-new DB, set auto_vacuum BEFORE creating any tables.
    # This writes the setting into the file header; no VACUUM needed.
    if fresh_db:
        cur.execute("PRAGMA auto_vacuum=INCREMENTAL")
        con.commit()  # persist the pragma into the just-created file

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
    # meta for labels/units/addresses
    cur.execute("""
      CREATE TABLE IF NOT EXISTS tag_meta (
        tag    TEXT PRIMARY KEY,
        label  TEXT,
        unit   TEXT,
        mw     INTEGER,
        dtype  TEXT
      )
    """)
    con.commit(); con.close()

def upsert_tag_meta_from_tags():
    """Logger is the sole owner of tag_meta contents."""
    rows = [(t["name"], t.get("label", t["name"]), t.get("unit",""), int(t["mw"]),
             (t.get("dtype") or t.get("type","INT16")).upper()) for t in TAGS]
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    cur.executemany("""
        INSERT INTO tag_meta(tag, label, unit, mw, dtype)
        VALUES(?,?,?,?,?)
        ON CONFLICT(tag) DO UPDATE SET
            label=excluded.label,
            unit =excluded.unit,
            mw   =excluded.mw,
            dtype=excluded.dtype
    """, rows)
    con.commit(); con.close()

def set_state_many(pairs):
    if not pairs: return
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    cur.executemany("""
        INSERT INTO state(key, value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, [(k, float(v)) for k,v in pairs])
    con.commit(); con.close()

def write_rows(rows):
    if not rows: return
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    cur.execute("PRAGMA busy_timeout=2000;")
    cur.executemany("INSERT INTO logs (ts, tag, value, unit) VALUES (?,?,?,?)", rows)
    con.commit(); con.close()

def _iso_to_epoch_utc(ts_text: str) -> float:
    """
    Convert your stored ISO UTC string to epoch seconds.
    Assumes your code stores UTC via datetime.utcnow().isoformat().
    """
    try:
        dt = datetime.fromisoformat(ts_text)
    except Exception:
        return 0.0
    # Treat naive dt as UTC (you use datetime.utcnow())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()

def hydrate_baseline_from_db():
    """
    Seed last_value and last_logged from the latest DB row per tag.
    Prevents 'first scan after restart' logging for on_change/conditional.
    """
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT l.tag, l.value, l.ts
            FROM logs AS l
            JOIN (
              SELECT tag, MAX(ts) AS ts
              FROM logs
              GROUP BY tag
            ) x ON x.tag = l.tag AND x.ts = l.ts
        """)
        rows = cur.fetchall()
    finally:
        cur.close(); con.close()

    for tag, value, ts_text in rows:
        try:
            last_value[str(tag)]  = float(value) if value is not None else None
        except Exception:
            last_value[str(tag)]  = None
        last_logged[str(tag)] = _iso_to_epoch_utc(ts_text) or 0.0

def _filesize_sum(path: str) -> int:
    total = 0
    for p in (path, f"{path}-wal", f"{path}-shm"):
        try: total += os.path.getsize(p)
        except OSError: pass
    return total

def init_storage_on_restart():
    con = sqlite3.connect(DB, timeout=30)
    cur = con.cursor()
    try:
        cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # shrink WAL from last run
        cur.execute("PRAGMA incremental_vacuum(5000)")  # reclaim some free pages
        con.commit()
    finally:
        cur.close(); con.close()

# ------------------ Modbus decode helpers ------------------
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
        raise RuntimeError(f"Modbus exception %MW{start_mw}..%MW{start_mw+count-1}: function={fc} exception={ec} ({rr})")
    regs = getattr(rr, "registers", None)
    if regs is None:
        raise RuntimeError(f"Malformed response for %MW{start_mw}..%MW{start_mw+count-1}: {rr!r}")
    return regs

def decode_from_window(words, win_start_mw, tag):
    """Decode a tag value from a pre-read window."""
    idx = tag["mw"] - win_start_mw
    if idx < 0 or idx >= len(words): return None
    dtype = (tag.get("dtype") or tag.get("type","INT16")).upper()
    if dtype == "INT16":   v = float(to_int16(words[idx]))
    elif dtype == "UINT16": v = float(to_uint16(words[idx]))
    elif dtype in ("INT32", "UINT32", "FLOAT32"):
        if idx+1 >= len(words): return None
        hi, lo = words[idx], words[idx+1]
        if dtype == "INT32":   v = float(to_int32(hi, lo))
        elif dtype == "UINT32": v = float(to_uint32(hi, lo))
        else:                  v = float(to_float32(hi, lo))
    else:
        raise ValueError(f"Unknown dtype {dtype}")
    return v * float(tag.get("scale", 1.0))

# ------------------ Per-tag logging policy ------------------
last_value  = {}  # name -> last numeric value (float)
last_logged = {}  # name -> epoch seconds of last DB write

def due_every(name, now_s, interval_s):
    return (now_s - last_logged.get(name, 0.0)) >= float(interval_s)

def mark_logged(name, now_s):
    last_logged[name] = float(now_s)

def changed_enough(prev, cur, deadband_abs=None, deadband_pct=None):
    # If we don't have any baseline yet, do NOT treat the first sample as a change.
    if prev is None:
        return False
    if deadband_abs is not None and abs(cur - prev) > float(deadband_abs):
        return True
    if deadband_pct is not None:
        base = max(abs(prev), 1e-9)
        if abs(cur - prev) / base * 100.0 > float(deadband_pct):
            return True
    return False

def eval_condition(cond, values_by_name):
    """cond = {'tag':'P1_Status', 'op':'==', 'value':1} -> bool"""
    if not cond: return False
    lhs = values_by_name.get(cond.get("tag"))
    if lhs is None: return False
    rhs = cond.get("value")
    op  = cond.get("op", "==")
    try:
        if op == "==": return lhs == rhs
        if op == "!=": return lhs != rhs
        if op == ">":  return lhs >  rhs
        if op == ">=": return lhs >= rhs
        if op == "<":  return lhs <  rhs
        if op == "<=": return lhs <= rhs
    except Exception:
        return False
    return False

# ------------------ Main loop ------------------
def main():
    ensure_schema()
    upsert_tag_meta_from_tags()

    # Auto storage hygiene on every restart
    init_storage_on_restart()

    # Hydrate baselines so on_change/conditional don't fire immediately after restart
    hydrate_baseline_from_db()

    # For any tags with no DB history, seed their last_logged to "now"
    # so conditional mode won't burst-log on first cycle.
    seed_now = time.time()
    for t in TAGS:
        name = t["name"]
        if name not in last_logged:
            last_logged[name] = float(seed_now)

    # Precompute minimal contiguous %MW window
    def width(t):
        dt = (t.get("dtype") or t.get("type","INT16")).upper()
        return 2 if dt in ("INT32","UINT32","FLOAT32") else 1
    WIN_START = min(t["mw"] for t in TAGS)
    WIN_END   = max(t["mw"] + width(t) - 1 for t in TAGS)
    WIN_COUNT = WIN_END - WIN_START + 1
    log.info(f"Reading window %MW{WIN_START}..%MW{WIN_END} ({WIN_COUNT} regs)")

    pending, last_flush = [], time.time()
    consecutive_errors = 0
    backoff_min, backoff_max = 0.5, 5.0

    while True:
        try:
            regs = read_words(WIN_START, WIN_COUNT)
            if len(regs) != WIN_COUNT:
                log.warning(f"Expected {WIN_COUNT} regs, got {len(regs)}")

            now_iso = datetime.utcnow().isoformat()
            now_s   = time.time()

            # 1) Decode all current values once
            cur_vals = {}
            for t in TAGS:
                try:
                    val = decode_from_window(regs, WIN_START, t)
                except Exception as e:
                    log.warning(f"Decode error {t.get('name')} @%MW{t.get('mw')}: {e}")
                    val = None
                cur_vals[t["name"]] = val

            # 2) Apply per-tag logging policy
            for t in TAGS:
                name = t["name"]
                val  = cur_vals.get(name)
                if val is None:
                    continue

                mode = (t.get("mode") or "interval").lower()

                if mode == "interval":
                    interval = float(t.get("interval_sec", 60.0))
                    if due_every(name, now_s, interval):
                        pending.append((now_iso, name, float(val), t.get("unit","")))
                        mark_logged(name, now_s)

                elif mode == "on_change":
                    prev = last_value.get(name)
                    db_abs = t.get("deadband_abs", DEFAULT_DB_ABS if t.get("mode")=="on_change" else None)
                    db_pct = t.get("deadband_pct", None)
                    min_int = float(t.get("min_interval_sec", 0.0))
                    if changed_enough(prev, float(val), db_abs, db_pct) and due_every(name, now_s, min_int):
                        pending.append((now_iso, name, float(val), t.get("unit","")))
                        mark_logged(name, now_s)

                elif mode == "conditional":
                    cond = t.get("condition", {})
                    if eval_condition(cond, cur_vals):
                        # while condition true, log at FAST_SEC
                        if due_every(name, now_s, float(FAST_SEC)):
                            pending.append((now_iso, name, float(val), t.get("unit","")))
                            mark_logged(name, now_s)
                    else:
                        # when false, optional idle cadence (default 10 min)
                        idle_int = float(t.get("idle_interval_sec", 600.0))
                        if idle_int > 0 and due_every(name, now_s, idle_int):
                            pending.append((now_iso, name, float(val), t.get("unit","")))
                            mark_logged(name, now_s)

                else:
                    log.warning(f"Unknown mode '{mode}' for {name}")

                # update last_value after decision
                last_value[name] = float(val)

            # 3) Flush ~1 Hz
            if pending and (time.time() - last_flush) >= 1.0:
                write_rows(pending)
                set_state_many([
                    ("connected", 1),
                    ("last_read_ok", 1),
                    ("consecutive_errors", consecutive_errors),
                    ("last_read_epoch", now_s),
                    ("last_flush_epoch", time.time()),
                    ("rows_written_last_flush", len(pending)),
                ])
                pending.clear()
                last_flush = time.time()

                # keep DB under cap (self-throttled)
                try:
                    stats = enforce_quota_periodic(ret_cfg)
                    if not stats.get("skipped"):
                        log.info(
                            "Retention: deleted=%s size %.1fMB→%.1fMB",
                            stats["deleted"],
                            stats["start_bytes"]/1024/1024,
                            stats["end_bytes"]/1024/1024
                        )
                except Exception as e:
                    log.warning("Retention check failed: %s", e)

            consecutive_errors = 0
            time.sleep(float(SAMPLE_SEC))

        except Exception as e:
            consecutive_errors += 1
            backoff = min(backoff_min * (2 ** min(consecutive_errors, 6)), backoff_max) + random.uniform(0, 0.2)
            if consecutive_errors in (1, 5) or consecutive_errors % 20 == 0:
                log.warning(f"Modbus read error (#{consecutive_errors}): {e} — backing off {backoff:.2f}s")
            set_state_many([
                ("connected", 0),
                ("last_read_ok", 0),
                ("consecutive_errors", consecutive_errors),
            ])
            time.sleep(backoff)
            # force clean reconnect next loop
            with _client_lock:
                if _client:
                    try: _client.close()
                    except: pass
                globals()["_client"] = None

if __name__ == "__main__":
    main()
