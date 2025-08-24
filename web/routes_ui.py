# web/routes_ui.py
from flask import Blueprint, request, render_template, jsonify, make_response, redirect, url_for

from .db import (
    list_tags_with_labels,    # list of {'tag','label'}
    tag_label_map,
    read_state,
    fetch_setpoints,
    fmt_local_epoch,
)

from .storage_status import get_storage_status

# New: chunk storage + config (no old DB var)
from config import DB_ROOT, RETENTION, PLC_IP, PLC_PORT, SLAVE_ID, WORD_ORDER, USE_MODBUS

# Fresh-per-request Modbus
from pymodbus.client import ModbusTcpClient
import struct
import inspect  # <-- for robust pymodbus 2.x/3.x kwarg detection

ui_bp = Blueprint("ui", __name__)

# ---------- Modbus helpers (fresh connection, version-compatible) ----------

# Cache which kw the current pymodbus exposes for the "unit/slave" argument.
# We detect once per process and reuse.
_UNIT_KW = None  # "unit", "slave", or None if neither (rare)

def _detect_unit_kw(cli) -> str | None:
    """
    Inspect read_holding_registers signature to see whether it accepts "unit" or "slave".
    Returns "unit", "slave", or None. Caches the result in _UNIT_KW.
    """
    global _UNIT_KW
    if _UNIT_KW is not None:
        return _UNIT_KW
    try:
        sig = inspect.signature(cli.read_holding_registers)
        params = sig.parameters
        if "unit" in params:
            _UNIT_KW = "unit"
        elif "slave" in params:
            _UNIT_KW = "slave"
        else:
            _UNIT_KW = None
    except Exception:
        # Default to "unit" if we can't inspect (most common on 2.x)
        _UNIT_KW = "unit"
    return _UNIT_KW

def _with_modbus(op):
    """
    Open a new Modbus TCP connection, run op(cli), then close.
    Returns (result, error_msg). If op itself returns (result, error_msg),
    we pass that through without nesting.
    """
    if not USE_MODBUS:
        return None, "Modbus not enabled on server"

    cli = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
    try:
        if not cli.connect():
            return None, "Modbus connect failed"

        # Prime detection (once)
        _detect_unit_kw(cli)

        rv = op(cli)

        # If op already returned (result, err), pass it through unchanged.
        if isinstance(rv, tuple) and len(rv) == 2 and isinstance(rv[1], (str, type(None))):
            return rv

        # Otherwise, wrap as (result, "")
        return rv, ""
    except Exception as e:
        return None, str(e)
    finally:
        try:
            cli.close()
        except Exception:
            pass

def _apply_unit_kw(kwargs: dict, kw: str | None) -> dict:
    """Return a copy of kwargs with the correct unit/slave kw injected if available."""
    if kw:
        out = dict(kwargs)
        out[kw] = SLAVE_ID
        return out
    return kwargs

def _mb_read_holding(cli, address: int, count: int):
    """Compat wrapper for pymodbus 2.x (unit=) vs 3.x (slave=) with fallback."""
    kw = _detect_unit_kw(cli)
    base = dict(address=address, count=count)
    try:
        return cli.read_holding_registers(**_apply_unit_kw(base, kw))
    except TypeError:
        # Try the alternate kw once (covers weird installs)
        alt = "slave" if kw == "unit" else "unit"
        try:
            return cli.read_holding_registers(**_apply_unit_kw(base, alt))
        except TypeError:
            # Last resort: call without either kw
            return cli.read_holding_registers(**base)

def _mb_write_register(cli, address: int, value: int):
    kw = _detect_unit_kw(cli)
    base = dict(address=address, value=value)
    try:
        return cli.write_register(**_apply_unit_kw(base, kw))
    except TypeError:
        alt = "slave" if kw == "unit" else "unit"
        try:
            return cli.write_register(**_apply_unit_kw(base, alt))
        except TypeError:
            return cli.write_register(**base)

def _mb_write_registers(cli, address: int, values):
    kw = _detect_unit_kw(cli)
    base = dict(address=address, values=values)
    try:
        return cli.write_registers(**_apply_unit_kw(base, kw))
    except TypeError:
        alt = "slave" if kw == "unit" else "unit"
        try:
            return cli.write_registers(**_apply_unit_kw(base, alt))
        except TypeError:
            return cli.write_registers(**base)

def _float_to_words(val: float):
    """Encode float32 to two 16-bit words with WORD_ORDER ('HL' or 'LH')."""
    hi, lo = struct.unpack(">HH", struct.pack(">f", float(val)))
    if WORD_ORDER.upper() == "LH":
        hi, lo = lo, hi
    return int(hi), int(lo)

def _words_to_float(hi: int, lo: int) -> float:
    """Decode two 16-bit words to float32 with WORD_ORDER."""
    if WORD_ORDER.upper() == "LH":
        hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", int(hi), int(lo)))[0]

def _read_setpoints_values(cli, sps):
    """
    Read current values for each setpoint in sps.
    sps items: {'name','mw','dtype', ...}
    Returns (values_dict, error_msg)
    """
    values = {}
    for sp in sps:
        name = sp.get("name")
        addr = int(sp.get("mw"))
        dtype = (sp.get("dtype") or "INT16").upper()
        try:
            if dtype == "INT16":
                rr = _mb_read_holding(cli, address=addr, count=1)
                if rr is None or (hasattr(rr, "isError") and rr.isError()):
                    return values, f"Read error @%MW{addr}: {getattr(rr, 'exception_code', rr)}"
                values[name] = int(rr.registers[0])
            else:
                rr = _mb_read_holding(cli, address=addr, count=2)
                if rr is None or (hasattr(rr, "isError") and rr.isError()):
                    return values, f"Read error @%MW{addr}..{addr+1}: {getattr(rr, 'exception_code', rr)}"
                hi, lo = rr.registers[0], rr.registers[1]
                values[name] = _words_to_float(hi, lo)
        except Exception as e:
            return values, f"Read exception @%MW{addr}: {e}"
    return values, ""

# ---------- Routes ----------

@ui_bp.route("/")
def home():
    cur_tag    = request.args.get("tag", "").strip()
    cur_limit  = request.args.get("limit", "500")
    cur_bucket = request.args.get("bucket_s", "")
    cur_cal    = request.args.get("cal", "all").strip().lower()

    tags = list_tags_with_labels()  # [{'tag','label'}]
    selections = {
        "tag": cur_tag,
        "limit": cur_limit,
        "bucket_s": cur_bucket,
        "cal": cur_cal,
    }
    return render_template("home.html", title="PLC Logger UI",
                           tags=tags, selections=selections)

@ui_bp.route("/status_page")
def status_page():
    s = read_state()
    if s.get("last_read_epoch") is not None:
        s["last_read_epoch_local"] = fmt_local_epoch(s.get("last_read_epoch"))
    if s.get("last_flush_epoch") is not None:
        s["last_flush_epoch_local"] = fmt_local_epoch(s.get("last_flush_epoch"))

    # Use chunk root + total cap (MB), and pass per-family caps too
    total_cap_mb = RETENTION.get("total_cap_mb", 0)
    storage = get_storage_status(DB_ROOT, total_cap_mb)
    family_caps = RETENTION.get("caps", {})  # e.g. {"conditional":7000, ...}

    return render_template("status.html",
                           title="Status",
                           s=s,
                           storage=storage,
                           family_caps=family_caps)

@ui_bp.route("/setpoints", methods=["GET", "POST"])
def setpoints():
    # carry selection across requests
    selected_name = (request.args.get("sel") or "").strip()

    msg = request.args.get("m", "")  # message carried via redirect
    labels = tag_label_map()
    sps = fetch_setpoints()  # list of dicts describing setpoints

    if not sps:
        msg = "No setpoints configured. Add SETPOINTS in config.py."
        return render_template("setpoints.html", title="Setpoints",
                               msg=msg, labels=labels, sps=[], values={}, selected_name=selected_name)

    # POST (write) — do the write, then redirect back with ?sel=<name>&m=<msg>
    if request.method == "POST":
        name = (request.form.get("name") or (request.json or {}).get("name") or "").strip()
        value = (request.form.get("value") or (request.json or {}).get("value") or "").strip()
        sp = next((x for x in sps if x.get("name") == name), None)
        selected_name = name or selected_name or sps[0]["name"]  # keep current selection

        try:
            fval = float(value)
        except Exception:
            fval = None

        if not sp or fval is None:
            msg = "Invalid name or value"
        else:
            def _write(cli):
                addr = int(sp["mw"])
                dtype = (sp.get("dtype") or "INT16").upper()
                if dtype == "INT16":
                    rq = _mb_write_register(cli, address=addr, value=int(fval))
                else:
                    hi, lo = _float_to_words(fval)
                    rq = _mb_write_registers(cli, address=addr, values=[hi, lo])
                if hasattr(rq, "isError") and rq.isError():
                    raise RuntimeError(f"Modbus write error: {rq}")
                return True

            ok, err = _with_modbus(lambda cli: _write(cli))
            pretty = labels.get(name, sp.get("label") or name)
            msg = f"Updated {pretty}" if (err == "" and ok) else f"Write failed for {pretty}: {err or 'unknown error'}"

        # Redirect so the page reload keeps the selection and shows the message
        return redirect(url_for("ui.setpoints", sel=selected_name, m=msg))

    # GET — read current values and render
    # if no selected_name yet (first load), default to first
    if not selected_name:
        selected_name = sps[0]["name"]

    # Always read fresh values (GET or after POST)
    values, err = _with_modbus(lambda cli: _read_setpoints_values(cli, sps))
    values = values or {}

    if err and not msg:
        msg = err

    return render_template("setpoints.html", title="Setpoints",
                           msg=msg, labels=labels, sps=sps, values=values, selected_name=selected_name)
