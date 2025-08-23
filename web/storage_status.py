# web/storage_status.py
import os
from typing import Dict, Any

MB = 1024 * 1024

def _sum_files_under(path: str) -> tuple[int, int]:
    """Return (total_bytes, file_count) for all regular files under path."""
    total, count = 0, 0
    if not os.path.isdir(path):
        return 0, 0
    for root, _dirs, files in os.walk(path):
        for fn in files:
            fp = os.path.join(root, fn)
            try:
                total += os.path.getsize(fp)
                count += 1
            except OSError:
                pass
    return total, count

def _family_stats(db_root: str, name: str) -> Dict[str, Any]:
    fam_path = os.path.join(db_root, name)
    bytes_, files_ = _sum_files_under(fam_path)
    return {
        "name": name,
        "mb": round(bytes_ / MB, 1),
        "bytes": bytes_,
        "files": files_,
    }

def get_storage_status(db_root: str, max_total_mb: int) -> Dict[str, Any]:
    """
    Compute storage usage for chunked layout rooted at db_root.
    - Sums all files in immediate subdirectories (families) and top-level files.
    - Returns per-family stats and overall usage vs cap.
    """
    families: Dict[str, Any] = {}
    total_bytes = 0
    files_total = 0

    # Per-family directories (continuous/conditional/onchange and any others)
    if os.path.isdir(db_root):
        for entry in os.scandir(db_root):
            if entry.is_dir():
                st = _family_stats(db_root, entry.name)
                families[entry.name] = {"mb": st["mb"], "files": st["files"], "bytes": st["bytes"]}
                total_bytes += st["bytes"]
                files_total += st["files"]
            elif entry.is_file():
                try:
                    total_bytes += entry.stat().st_size
                    files_total += 1
                except OSError:
                    pass

    cap_bytes = int(max_total_mb or 0) * MB
    pct_of_cap = 0.0
    if cap_bytes > 0:
        pct_of_cap = min(100.0, (total_bytes * 100.0) / cap_bytes)

    def mb(n: int) -> float:
        return round(n / MB, 1)

    # Convenience (so templates can print directly)
    cont = families.get("continuous", {})
    cond = families.get("conditional", {})
    onch = families.get("onchange", {})

    return {
        "root": db_root,
        "total_mb": mb(total_bytes),
        "cap_mb": float(max_total_mb or 0),
        "pct_of_cap": round(pct_of_cap, 1),
        "files_total": files_total,
        "families": {
            k: {"mb": v.get("mb", 0.0), "files": v.get("files", 0)}
            for k, v in families.items()
        },
        # direct fields for common families (nice for simple tables)
        "continuous_mb": cont.get("mb", 0.0),
        "conditional_mb": cond.get("mb", 0.0),
        "onchange_mb": onch.get("mb", 0.0),
        "continuous_files": cont.get("files", 0),
        "conditional_files": cond.get("files", 0),
        "onchange_files": onch.get("files", 0),

        # Back-compat placeholders from the old single-DB status:
        "db_mb": 0.0, "wal_mb": 0.0, "shm_mb": 0.0,
        "auto_vacuum": None, "auto_vacuum_label": "N/A",
        "journal_mode": "N/A",
        "page_size": 0, "page_count": 0, "freelist_count": 0,
        "row_count": 0, "min_ts": None, "max_ts": None,
    }
