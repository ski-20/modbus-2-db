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
    stats = {"phase": [], "start_bytes": 0, "end_bytes": 0, "deleted": 0}

    def _sumsize():
        return _filesize_sum(cfg.db_path)

    def _checkpoint():
        with _connect(cfg.db_path) as con:
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _incr_vac(pages: int):
        with _connect(cfg.db_path) as con:
            con.execute(f"PRAGMA incremental_vacuum({int(pages)})")

    # Preflight: bail if autovacuum isn't enabled yet
    with _connect(cfg.db_path) as con:
        cur = con.cursor()
        cur.execute("PRAGMA auto_vacuum")
        av = cur.fetchone()[0]  # 0 NONE, 1 FULL, 2 INCREMENTAL
    if av != 2:
        # Don't delete if we can't shrink the file — return early
        stats["phase"].append("skipped_autovacuum_off")
        stats["start_bytes"] = stats["end_bytes"] = _sumsize()
        return stats

    # Start by shrinking WAL so our size measurement is real
    _checkpoint()
    size = _sumsize()
    stats["start_bytes"] = size
    max_bytes = cfg.max_db_mb * 1024 * 1024

    if size <= max_bytes:
        stats["end_bytes"] = size
        return stats

    # ---- Phase 1: delete anything older than raw_keep_days (best-effort) ----
    def delete_before(cutoff_iso: str, limit_rows: int) -> int:
        with _connect(cfg.db_path) as con:
            cur = con.cursor()
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

    if cfg.raw_keep_days and cfg.raw_keep_days > 0:
        cutoff_iso = (datetime.utcnow() - timedelta(days=int(cfg.raw_keep_days))).isoformat()
        # loop while over cap; trim oldest-outside-window first
        while size > max_bytes:
            deleted = delete_before(cutoff_iso, cfg.delete_batch)
            if deleted == 0:
                break
            stats["deleted"] += deleted
            _incr_vac(cfg.incremental_vacuum_pages)
            _checkpoint()
            new_size = _sumsize()
            if new_size >= size - (512 * 1024):  # <0.5MB improvement -> bail
                break
            size = new_size

    stats["phase"].append("older_than_keep_days")

    # ---- Phase 2a: primary tags first ----
    before_bytes = size
    primary = (cfg.primary_purge_tags or ["SYS_WetWellLevel"])

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
            _incr_vac(cfg.incremental_vacuum_pages)
            _checkpoint()
            new_size = _sumsize()

            # Safety: if no shrink after a large delete, stop and warn
            if new_size >= size - (512 * 1024):  # < 0.5 MB improvement
                # avoid wiping tons of rows when shrink isn't happening
                break
            size = new_size

    stats["phase"].append("oldest_primary_tags")

    # ---- Phase 2b: still over? global oldest ----
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

    while size > max_bytes:
        deleted = delete_oldest(cfg.delete_batch)
        if deleted == 0:
            break
        stats["deleted"] += deleted
        _incr_vac(cfg.incremental_vacuum_pages)
        _checkpoint()
        new_size = _sumsize()
        if new_size >= size - (512 * 1024):
            break
        size = new_size

    stats["phase"].append("oldest_any_age")

    # Final reclaim and measurement
    _incr_vac(0)          # reclaim all possible free pages now
    _checkpoint()         # and shrink WAL
    stats["end_bytes"] = _sumsize()
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
