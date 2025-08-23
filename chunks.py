# chunks.py
import os, glob, sqlite3, time
from typing import List, Tuple, Iterable, Dict, Optional, Any, Set
import sqlite3

# Families by logger mode
F_CONTINUOUS  = "continuous"   # interval
F_CONDITIONAL = "conditional"  # conditional
F_ONCHANGE    = "onchange"     # on_change

# Router state (built at startup)
_CONT_TAGS: Set[str] = set()
_COND_TAGS: Set[str] = set()
_ONCHG_TAGS: Set[str] = set()
_OVERRIDES: Dict[str, str] = {}

def init_family_router(tags: List[Dict[str, Any]], overrides: Optional[Dict[str, str]] = None):
    """Build routing sets from TAGS (expects each tag dict to have 'name' and 'mode')."""
    global _CONT_TAGS, _COND_TAGS, _ONCHG_TAGS, _OVERRIDES
    _CONT_TAGS.clear(); _COND_TAGS.clear(); _ONCHG_TAGS.clear()
    _OVERRIDES = overrides or {}
    for t in tags:
        name = t["name"]
        mode = (t.get("mode") or "interval").lower()
        if _OVERRIDES.get(name):
            fam = _OVERRIDES[name]
        else:
            fam = (
                F_CONTINUOUS  if mode == "interval"    else
                F_CONDITIONAL if mode == "conditional" else
                F_ONCHANGE
            )
        if fam == F_CONTINUOUS:   _CONT_TAGS.add(name)
        elif fam == F_CONDITIONAL:_COND_TAGS.add(name)
        else:                     _ONCHG_TAGS.add(name)

def family_for_tag(tag: str) -> str:
    if tag in _OVERRIDES: return _OVERRIDES[tag]
    if tag in _CONT_TAGS: return F_CONTINUOUS
    if tag in _COND_TAGS: return F_CONDITIONAL
    return F_ONCHANGE

def _p(*a): return os.path.join(*a)

def ensure_layout(db_root: str):
    os.makedirs(_p(db_root, "chunks", F_CONTINUOUS),  exist_ok=True)
    os.makedirs(_p(db_root, "chunks", F_CONDITIONAL), exist_ok=True)
    os.makedirs(_p(db_root, "chunks", F_ONCHANGE),    exist_ok=True)
    os.makedirs(db_root, exist_ok=True)

def meta_path(db_root: str) -> str:
    return _p(db_root, "meta.db")  # state + tag_meta live here

def chunk_dir(db_root: str, fam: str) -> str:
    return _p(db_root, "chunks", fam)

def list_chunks(db_root: str, fam: str) -> List[str]:
    return sorted(glob.glob(_p(chunk_dir(db_root, fam), "plc-*.db")))

def chunk_size_bytes(path: str) -> int:
    s = 0
    for ext in ("", "-wal", "-shm"):
        try: s += os.path.getsize(path + ext)
        except OSError: pass
    return s

def _ensure_schema(path: str):
    con = sqlite3.connect(path, timeout=30)
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=2000;")
    cur.execute("""CREATE TABLE IF NOT EXISTS logs (
        ts   TEXT NOT NULL,
        tag  TEXT NOT NULL,
        value REAL,
        unit TEXT
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_tag_ts ON logs(tag, ts)")
    con.commit(); con.close()

def _new_chunk_path(db_root: str, fam: str) -> str:
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    return _p(chunk_dir(db_root, fam), f"plc-{ts}.db")

_active_chunk: Dict[str, str] = {}  # fam -> path

def _select_active_chunk(db_root: str, fam: str) -> str:
    if fam in _active_chunk and os.path.exists(_active_chunk[fam]):
        return _active_chunk[fam]
    chunks = list_chunks(db_root, fam)
    if chunks:
        _active_chunk[fam] = chunks[-1]
    else:
        p = _new_chunk_path(db_root, fam)
        _ensure_schema(p)
        _active_chunk[fam] = p
    return _active_chunk[fam]

def _rotate_chunk_if_needed(db_root: str, fam: str, chunk_max_mb: int):
    p = _select_active_chunk(db_root, fam)
    if chunk_size_bytes(p) >= chunk_max_mb * 1024 * 1024:
        p = _new_chunk_path(db_root, fam)
        _ensure_schema(p)
        _active_chunk[fam] = p

def write_rows_chunked(db_root: str, chunk_max_mb: int,
                       rows: Iterable[Tuple[str, str, float, str]]):
    """rows: (ts_iso, tag, value, unit)"""
    if not rows: return
    buckets: Dict[str, list] = {F_CONTINUOUS: [], F_CONDITIONAL: [], F_ONCHANGE: []}
    for r in rows:
        buckets[family_for_tag(r[1])].append(r)
    for fam, chunk in buckets.items():
        if not chunk: continue
        _rotate_chunk_if_needed(db_root, fam, chunk_max_mb)
        p = _select_active_chunk(db_root, fam)
        con = sqlite3.connect(p, timeout=30)
        cur = con.cursor()
        cur.execute("PRAGMA busy_timeout=2000;")
        cur.executemany("INSERT INTO logs (ts, tag, value, unit) VALUES (?,?,?,?)", chunk)
        con.commit(); con.close()

def enforce_chunk_quota(db_root: str, total_cap_mb: int, fam_caps: Optional[Dict[str,int]] = None):
    """
    Delete whole oldest chunk files to meet caps.
    Global priority: continuous -> conditional -> onchange.
    """
    fam_caps = fam_caps or {}
    out = {"deleted_files": [], "by_fam": {}, "total_mb": 0.0}

    def fam_bytes(fam: str) -> int:
        return sum(chunk_size_bytes(p) for p in list_chunks(db_root, fam))

    def delete_oldest(fam: str) -> bool:
        lst = list_chunks(db_root, fam)
        if not lst: return False
        active = _select_active_chunk(db_root, fam)
        cand = lst[0] if lst[0] != active or len(lst) == 1 else (lst[1] if len(lst) > 1 else lst[0])
        for ext in ("", "-wal", "-shm"):
            try: os.remove(cand + ext)
            except FileNotFoundError: pass
        out["deleted_files"].append(os.path.basename(cand))
        return True

    # Per-family caps (optional)
    for fam in (F_CONTINUOUS, F_CONDITIONAL, F_ONCHANGE):
        cap = fam_caps.get(fam)
        if cap:
            while fam_bytes(fam) > cap * 1048576 and delete_oldest(fam):
                pass

    # Global cap with family priority
    def total_bytes():
        return fam_bytes(F_CONTINUOUS)+fam_bytes(F_CONDITIONAL)+fam_bytes(F_ONCHANGE)

    tot = total_bytes()
    while tot > total_cap_mb * 1048576:
        if not delete_oldest(F_CONTINUOUS):
            if not delete_oldest(F_CONDITIONAL):
                if not delete_oldest(F_ONCHANGE):
                    break
        tot = total_bytes()

    out["by_fam"][F_CONTINUOUS]  = round(fam_bytes(F_CONTINUOUS)/1048576, 3)
    out["by_fam"][F_CONDITIONAL] = round(fam_bytes(F_CONDITIONAL)/1048576, 3)
    out["by_fam"][F_ONCHANGE]    = round(fam_bytes(F_ONCHANGE)/1048576, 3)
    out["total_mb"] = round(sum(out["by_fam"].values()), 3)
    return out

def query_logs(db_root: str, tag: Optional[str], cal: str, limit: int):
    """Read newest→oldest across relevant families; return [(ts, tag, value, unit)]."""
    import datetime as _dt
    fams = [family_for_tag(tag)] if tag else [F_CONTINUOUS, F_CONDITIONAL, F_ONCHANGE]

    now = _dt.datetime.utcnow()
    start = None
    if   cal == "today":      start = now.replace(hour=0,minute=0,second=0,microsecond=0)
    elif cal == "yesterday":  start = (now - _dt.timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
    elif cal == "week":       start = now - _dt.timedelta(days=7)
    elif cal == "month":      start = now - _dt.timedelta(days=31)
    elif cal == "year":       start = now - _dt.timedelta(days=365)
    start_iso = start.isoformat() if start else None

    rows: list[tuple] = []
    for fam in fams:
        for p in list_chunks(db_root, fam)[::-1]:  # newest first
            if len(rows) >= limit: break
            con = sqlite3.connect(p, timeout=15)
            cur = con.cursor()
            if tag and start_iso:
                cur.execute("""SELECT ts, tag, value, unit FROM logs
                               WHERE tag=? AND ts>=? ORDER BY ts DESC LIMIT ?""",
                            (tag, start_iso, limit - len(rows)))
            elif tag:
                cur.execute("""SELECT ts, tag, value, unit FROM logs
                               WHERE tag=? ORDER BY ts DESC LIMIT ?""",
                            (tag, limit - len(rows)))
            elif start_iso:
                cur.execute("""SELECT ts, tag, value, unit FROM logs
                               WHERE ts>=? ORDER BY ts DESC LIMIT ?""",
                            (start_iso, limit - len(rows)))
            else:
                cur.execute("""SELECT ts, tag, value, unit FROM logs
                               ORDER BY ts DESC LIMIT ?""",
                            (limit - len(rows),))
            rows.extend(cur.fetchall()); con.close()
            if len(rows) >= limit: break

    rows.sort(key=lambda r: r[0], reverse=True)
    return rows[:limit]

def query_logs_between(db_root: str,
                       tag: Optional[str],
                       start_iso: Optional[str],
                       end_iso: Optional[str],
                       limit: int):
    """
    Return newest→oldest rows within an explicit UTC-naive ISO range.
    - start_iso / end_iso: strings like '2025-08-22T00:00:00' (no 'Z')
    - tag: filter to a single tag if provided, else search all families
    - limit: max rows returned overall (across families)
    """
    # Pick families to search
    fams = [family_for_tag(tag)] if tag else [F_CONTINUOUS, F_CONDITIONAL, F_ONCHANGE]

    rows: list[tuple[str, str, float | None, str]] = []

    # Search newest chunks first
    for fam in fams:
        for path in list_chunks(db_root, fam)[::-1]:
            if len(rows) >= limit:
                break
            con = sqlite3.connect(path, timeout=15)
            cur = con.cursor()

            # Build the SELECT based on which bounds/tag are provided.
            if tag and start_iso and end_iso:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       WHERE tag=? AND ts>=? AND ts<=?
                       ORDER BY ts DESC LIMIT ?""",
                    (tag, start_iso, end_iso, max(0, limit - len(rows))),
                )
            elif tag and start_iso:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       WHERE tag=? AND ts>=?
                       ORDER BY ts DESC LIMIT ?""",
                    (tag, start_iso, max(0, limit - len(rows))),
                )
            elif tag and end_iso:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       WHERE tag=? AND ts<=?
                       ORDER BY ts DESC LIMIT ?""",
                    (tag, end_iso, max(0, limit - len(rows))),
                )
            elif start_iso and end_iso:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       WHERE ts>=? AND ts<=?
                       ORDER BY ts DESC LIMIT ?""",
                    (start_iso, end_iso, max(0, limit - len(rows))),
                )
            elif start_iso:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       WHERE ts>=?
                       ORDER BY ts DESC LIMIT ?""",
                    (start_iso, max(0, limit - len(rows))),
                )
            elif end_iso:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       WHERE ts<=?
                       ORDER BY ts DESC LIMIT ?""",
                    (end_iso, max(0, limit - len(rows))),
                )
            else:
                cur.execute(
                    """SELECT ts, tag, value, unit FROM logs
                       ORDER BY ts DESC LIMIT ?""",
                    (max(0, limit - len(rows)),),
                )

            rows.extend(cur.fetchall())
            con.close()

            if len(rows) >= limit:
                break

    # Merge + sort newest→oldest, then trim to limit
    rows.sort(key=lambda r: r[0], reverse=True)
    return rows[:limit]
