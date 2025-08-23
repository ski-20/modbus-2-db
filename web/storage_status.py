# web/storage_status.py
import os
from typing import Dict, Any, Tuple

MB = 1024 * 1024

def _sum_files_under(path: str) -> Tuple[int, int]:
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

def _family_stats(path: str, name: str) -> Dict[str, Any]:
    bytes_, files_ = _sum_files_under(os.path.join(path, name))
    return {"name": name, "bytes": bytes_, "mb": round(bytes_ / MB, 1), "files": files_}

def get_storage_status(db_root: str, max_total_mb: int) -> Dict[str, Any]:
    """
    Supports either:
      DB_ROOT/
        continuous/
        conditional/
        onchange/
    or:
      DB_ROOT/
        chunks/
          continuous/
          conditional/
          onchange/
    """
    families: Dict[str, Any] = {}
    total_bytes = 0
    files_total = 0

    if os.path.isdir(db_root):
        # list immediate subdirs
        level1 = [e for e in os.scandir(db_root) if e.is_dir()]
        # If exactly one wrapper dir (e.g., "chunks"), flatten one level down
        scan_dirs = level1
        if len(level1) == 1:
            inner = [e for e in os.scandir(level1[0].path) if e.is_dir()]
            if inner:
                # treat inner dirs as families
                db_root = level1[0].path
                scan_dirs = inner

        for d in scan_dirs:
            st = _family_stats(db_root, d.name)
            families[d.name] = {"mb": st["mb"], "files": st["files"], "bytes": st["bytes"]}
            total_bytes += st["bytes"]
            files_total += st["files"]

        # count any top-level files as well
        for e in os.scandir(db_root):
            if e.is_file():
                try:
                    total_bytes += e.stat().st_size
                    files_total += 1
                except OSError:
                    pass

    cap_bytes = int(max_total_mb or 0) * MB
    pct_of_cap = min(100.0, (total_bytes * 100.0) / cap_bytes) if cap_bytes > 0 else 0.0

    def mb(n: int) -> float: return round(n / MB, 1)

    return {
        "root": db_root,
        "total_mb": mb(total_bytes),
        "cap_mb": float(max_total_mb or 0),
        "pct_of_cap": round(pct_of_cap, 1),
        "files_total": files_total,
        "families": {k: {"mb": v.get("mb", 0.0), "files": v.get("files", 0)} for k, v in families.items()},
        # no single-DB fields anymore
        "db_mb": 0.0, "wal_mb": 0.0, "shm_mb": 0.0,
        "auto_vacuum": None, "auto_vacuum_label": "N/A",
        "journal_mode": "N/A",
        "page_size": 0, "page_count": 0, "freelist_count": 0,
        "row_count": 0, "min_ts": None, "max_ts": None,
    }
