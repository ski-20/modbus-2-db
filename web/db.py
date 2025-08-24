# web/db.py
# Shared helpers for labels, time formatting, CSV, and lightweight state reads.

import io, csv, sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

# --- Config / timezone ---
try:
    from config import LOCAL_TZ as LOCAL_TZ_NAME, DB_ROOT
except Exception:
    LOCAL_TZ_NAME = "UTC"
    DB_ROOT = "/home/ele/plc_logger/data"

try:
    from zoneinfo import ZoneInfo  # Py3.9+
    _ZONE = ZoneInfo(LOCAL_TZ_NAME)
except Exception:
    _ZONE = None  # fall back to UTC

# --- Tags (source of truth for labels/units) ---
try:
    from tags import TAGS
except Exception:
    TAGS = []

# --- Chunk meta path (for state table only, if present) ---
try:
    from chunks import meta_path
except Exception:
    def meta_path(root: str) -> str:
        # Fallback; adjust if your repo uses a different meta filename
        import os
        return os.path.join(root, "meta.db")


# ----------------- Labels / tags -----------------

def tag_label_map() -> Dict[str, str]:
    """Return {tag: label} directly from tags.py (no DB)."""
    m: Dict[str, str] = {}
    for t in TAGS:
        name = t.get("name")
        if not name:
            continue
        m[str(name)] = t.get("label", name)
    return m


def list_tags_with_labels() -> List[Dict[str, str]]:
    """
    Return [{'tag': name, 'label': label}] sorted by label (case-insensitive),
    sourced from tags.py (no DB).
    """
    rows = [{"tag": t.get("name"), "label": t.get("label", t.get("name", ""))}
            for t in TAGS if t.get("name")]
    rows.sort(key=lambda r: (r["label"] or "").lower())
    return rows


def list_tags() -> List[str]:
    """Just the tag names, from tags.py."""
    return [t.get("name") for t in TAGS if t.get("name")]


# ----------------- Time formatting -----------------

def fmt_ts_local_from_iso(iso_str: str) -> str:
    """
    Convert an ISO timestamp (UTC or naive-UTC) to configured local tz string.
    """
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        if _ZONE:
            dt = dt.astimezone(_ZONE)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p")
    except Exception:
        return iso_str


def fmt_local_epoch(sec) -> Optional[str]:
    try:
        sec = float(sec)
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        if _ZONE:
            dt = dt.astimezone(_ZONE)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p")
    except Exception:
        return None


# ----------------- CSV helper -----------------

def download_csv(rows) -> str:
    """
    rows may be a list of tuples (ts, tag, value, unit) or dicts with those keys.
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts", "tag", "value", "unit"])
    for r in rows:
        if isinstance(r, dict):
            w.writerow([r.get("ts"), r.get("tag"), r.get("value"), r.get("unit", "")])
        else:
            # assume tuple-like
            ts, tag, val, unit = (list(r) + ["", "", "", ""])[:4]
            w.writerow([ts, tag, val, unit])
    return buf.getvalue()


# ----------------- State (best-effort from meta.db) -----------------

def read_state() -> Dict[str, Any]:
    """
    Optional: read runtime state written by logger into meta.db 'state' table.
    Safe if file/table doesn't exist.
    """
    try:
        meta = meta_path(DB_ROOT)
        con = sqlite3.connect(meta, timeout=10)
        try:
            cur = con.cursor()
            cur.execute("SELECT key, value FROM state")
            out = {str(k): float(v) for (k, v) in cur.fetchall()}
            return out
        finally:
            con.close()
    except Exception:
        return {}


# ----------------- Setpoints (from tags.py) -----------------

try:
    from tags import SETPOINTS as TAGS_SETPOINTS
except Exception:
    TAGS_SETPOINTS = []

def fetch_setpoints():
    """Return setpoints as list of dicts for the UI."""
    rows = []
    for sp in TAGS_SETPOINTS:
        rows.append({
            "name": sp["name"],
            "label": sp.get("label", sp["name"]),
            "unit": sp.get("unit", ""),
            "mw": sp["mw"],
            "dtype": sp.get("type", sp.get("dtype", "FLOAT32")),
        })
    return rows
