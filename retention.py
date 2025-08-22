# retention.py
import os, sqlite3, time, argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List

DEFAULT_PRIMARY_PURGE = ["SYS_WetWellLevel"]  # fallback if none provided

# ---------- Helpers ----------
def _filesize_sum(path: str) -> int:
    """Return size of DB + WAL/SHM if present."""
    total = 0
    for p in (path, f"{path}-wal", f"{path}-shm"):
        try:
            total += os.path.getsize(p)
        except Exception:
            pass
    return total

def _connect(db_path: str):
    return sqlite3.connect(db_path, timeout=30)

# ---------- Public API ----------
@dataclass
class RetentionConfig:
    db_path: str
    max_db_mb: int = 512           # hard cap including wal/shm
    raw_keep_days: int = 14        # keep raw rows at full fidelity
    delete_batch: int = 10_000     # delete in batches
    enforce_every_s: int = 300     # no-op if called more frequently
    incremental_vacuum_pages: int = 2000
    primary_purge_tags: Optional[List[str]] = None

_last_run_ts = 0.0

def enforce_quota_periodic(cfg: RetentionConfig) -> dict:
    """Call this frequently; it self-throttles via cfg.enforce_every_s."""
    global _last_run_ts
    now = time.time()
    if now - _last_run_ts < cfg.enforce_every_s:
        return {"skipped": True}
    _last_run_ts = now
    return enforce_quota_now(cfg)

def enforce_quota_now(cfg: RetentionConfig) -> dict:
    """Enforce size cap immediately. Returns stats dict for logging."""
    stats = {"phase": [], "start_bytes": 0, "end_bytes": 0, "deleted": 0}
    max_bytes = cfg.max_db_mb * 1024 * 1024
    primary = cfg.primary_purge_tags or DEFAULT_PRIMARY_PURGE

    # 1) Checkpoint WAL first so size reflects reality
    with _connect(cfg.db_path) as con:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    size = _filesize_sum(cfg.db_path)
    stats["start_bytes"] = size
    if size <= max_bytes:
        stats["end_bytes"] = size
        return stats

    # Helper deletions
    def delete_before(cutoff_iso: str, limit_rows: int) -> int:
        with _connect(cfg.db_path) as con:
            cur = con.cursor()
            # SQLite-friendly pattern using rowid selection
            cur.execute("""
                DELETE FROM logs
                WHERE rowid IN (
                  SELECT rowid FROM logs
                  WHERE ts < ?
                  ORDER BY ts ASC
                  LIMIT ?
                )
            """, (cutoff_iso, int(limit_rows)))
            n = cur.rowcount if cur.rowcount != -1 else 0
            con.commit()
        return n

    def delete_oldest(limit_rows: int) -> int:
        with _connect(cfg.db_path) as con:
            cur = con.cursor()
            cur.execute("""
                DELETE FROM logs
                WHERE rowid IN (
                  SELECT rowid FROM logs
                  ORDER BY ts ASC
                  LIMIT ?
                )
            """, (int(limit_rows),))
            n = cur.rowcount if cur.rowcount != -1 else 0
            con.commit()
        return n

    def incremental_vacuum():
        with _connect(cfg.db_path) as con:
            con.execute(f"PRAGMA incremental_vacuum({int(cfg.incremental_vacuum_pages)})")

    # 2) Phase 1: delete anything older than raw_keep_days
    cutoff_iso = (datetime.utcnow() - timedelta(days=cfg.raw_keep_days)).isoformat()
    while size > max_bytes:
        deleted = delete_before(cutoff_iso, cfg.delete_batch)
        if deleted == 0:
            break
        stats["deleted"] += deleted
        incremental_vacuum()
        size = _filesize_sum(cfg.db_path)
    stats["phase"].append("older_than_keep_days")

    # ---- Phase 2a: purge oldest from primary tags first ----
    def delete_oldest_for_tag(tag: str, limit_rows: int) -> int:
        with _connect(cfg.db_path) as con:
            cur = con.cursor()
            cur.execute("""
                DELETE FROM logs
                WHERE rowid IN (
                  SELECT rowid FROM logs
                  WHERE tag = ?
                  ORDER BY ts ASC
                  LIMIT ?
                )
            """, (tag, int(limit_rows)))
            n = cur.rowcount if cur.rowcount != -1 else 0
            con.commit()
        return n

    for tag in primary:
        while size > max_bytes:
            deleted = delete_oldest_for_tag(tag, cfg.delete_batch)
            if deleted == 0:
                break
            stats["deleted"] += deleted
            incremental_vacuum()
            size = _filesize_sum(cfg.db_path)
    stats["phase"].append("oldest_primary_tags")

    # ---- Phase 2b: still over? purge oldest globally ----
    while size > max_bytes:
        deleted = delete_oldest(cfg.delete_batch)
        if deleted == 0:
            break
        stats["deleted"] += deleted
        incremental_vacuum()
        size = _filesize_sum(cfg.db_path)
    stats["phase"].append("oldest_any_age")


    # 4) Final checkpoint and size
    with _connect(cfg.db_path) as con:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    stats["end_bytes"] = _filesize_sum(cfg.db_path)
    return stats

# ---------- Optional one-time setup ----------
def enable_incremental_autovacuum(db_path: str):
    """Run once (offline or maintenance window) to enable incremental auto_vacuum."""
    with _connect(db_path) as con:
        con.execute("PRAGMA auto_vacuum=INCREMENTAL")
        con.commit()
        # VACUUM rewrites DB and makes setting take effect
        con.execute("VACUUM")
        con.commit()

# ---------- CLI (optional) ----------
def _fmt_mb(nbytes: int) -> str:
    return f"{nbytes/1024/1024:.1f} MB"

def main():
    ap = argparse.ArgumentParser(description="SQLite retention/size cap enforcement")
    ap.add_argument("--db", required=True)
    ap.add_argument("--max-mb", type=int, default=512)
    ap.add_argument("--raw-keep-days", type=int, default=14)
    ap.add_argument("--batch", type=int, default=10000)
    ap.add_argument("--vacuum-pages", type=int, default=2000)
    ap.add_argument("--once", action="store_true", help="run once and exit")
    args = ap.parse_args()

    cfg = RetentionConfig(
        db_path=args.db,
        max_db_mb=args.max_mb,
        raw_keep_days=args.raw_keep_days,
        delete_batch=args.batch,
        incremental_vacuum_pages=args.vacuum_pages,
        enforce_every_s=0
    )
    stats = enforce_quota_now(cfg)
    print(f"Deleted {stats['deleted']} rows, size {_fmt_mb(stats['start_bytes'])} -> {_fmt_mb(stats['end_bytes'])}")

if __name__ == "__main__":
    main()
