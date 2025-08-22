# storage_status.py
import os, sqlite3, math
from typing import Dict, Any

def _filesize(path: str) -> int:
    try: return os.path.getsize(path)
    except OSError: return 0

def get_storage_status(db_path: str, max_db_mb: int) -> Dict[str, Any]:
    db_bytes  = _filesize(db_path)
    wal_bytes = _filesize(db_path + "-wal")
    shm_bytes = _filesize(db_path + "-shm")
    total_bytes = db_bytes + wal_bytes + shm_bytes
    cap_bytes   = int(max_db_mb) * 1024 * 1024 if max_db_mb else 0

    av_map = {0: "NONE", 1: "FULL", 2: "INCREMENTAL"}
    with sqlite3.connect(db_path, timeout=30) as con:
        cur = con.cursor()
        cur.execute("PRAGMA auto_vacuum"); av = cur.fetchone()[0]
        cur.execute("PRAGMA journal_mode"); jm = cur.fetchone()[0]
        cur.execute("PRAGMA page_size");    ps = cur.fetchone()[0]
        cur.execute("PRAGMA page_count");   pc = cur.fetchone()[0]
        cur.execute("PRAGMA freelist_count"); fl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM logs")
        row_count, min_ts, max_ts = cur.fetchone()

    pct_of_cap = 0.0
    if cap_bytes > 0:
        pct_of_cap = min(100.0, (total_bytes * 100.0) / cap_bytes)

    def mb(n): return round(n / (1024*1024), 1)

    return {
        "db_mb": mb(db_bytes),
        "wal_mb": mb(wal_bytes),
        "shm_mb": mb(shm_bytes),
        "total_mb": mb(total_bytes),
        "cap_mb": float(max_db_mb or 0),
        "pct_of_cap": round(pct_of_cap, 1),
        "auto_vacuum": av,
        "auto_vacuum_label": av_map.get(av, str(av)),
        "journal_mode": jm,
        "page_size": ps,
        "page_count": pc,
        "freelist_count": fl,
        "row_count": row_count or 0,
        "min_ts": min_ts,
        "max_ts": max_ts,
    }
