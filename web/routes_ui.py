# web/routes_ui.py
from flask import Blueprint, request, jsonify, make_response, render_template

# Keep your existing helpers from web/db.py (assuming they already read meta.db)
from .db import (
    list_tags_with_labels,    # list of {'tag','label'}
    tag_label_map,
    read_state,
    fetch_setpoints,
    fmt_local_epoch,
)

# Storage status for the UI
from .storage_status import get_storage_status

# --- NEW: chunk storage + config imports (replace old DB import) ---
from config import DB_ROOT, RETENTION, PLC_IP, PLC_PORT, SLAVE_ID, WORD_ORDER

# --- NEW: fresh-per-request Modbus client + float helpers ---
from pymodbus.client import ModbusTcpClient
import struct

ui_bp = Blueprint("ui", __name__)

# ---------- Modbus helpers (fresh connection per request) ----------

def _with_modbus(op):
    """
    Open a new Modbus TCP connection, run op(cli), then close.
    Returns (result, error_msg). On success, error_msg is ''.
    """
    cli = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
    try:
        if not cli.connect():
            return None, "Modbus connect failed"
        return op(cli), ""
    except Exception as e:
        return None, f"{e}"
    finally:
        try:
            cli.close()
        except Exception:
            pass

def _float_to_words(val: float):
    """Encode float32 to two 16-bit words with WORD_ORDER ('HL' or 'LH')."""
    hi, lo = struct.unpack(">HH", struct.pack(">f", float(val)))
    if WORD_ORDER.upper() == "LH":
        hi, lo = lo, hi
    return int(hi) if isinstance(hi, bool) is False else int(hi), int(lo)

def _words_to_float(hi: int, lo: int) -> float:
    """Decode two 16-bit words to float32 with WORD_ORDER."""
    if WORD_ORDER.upper() == "LH":
        hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", int(hi), int(lo)))[0]

def _mb_read_holding(cli, address: int, count: int):
    """Compat wrapper for pymodbus 2.x (unit=) vs 3.x (slave=)."""
    try:
        return cli.read_holding_registers(address=address, count=count, unit=SLAVE_ID)
    except TypeError:
        return cli.read_holding_registers(address=address, count=count, slave=SLAVE_ID)

def _mb_write_register(cli, address: int, value: int):
    try:
        return cli.write_register(address=address, value=value, unit=SLAVE_ID)
    except TypeError:
        return cli.write_register(address=address, value=value, slave=SLAVE_ID)

def _mb_write_registers(cli, address: int, values):
    try:
        return cli.write_registers(address=address, values=values, unit=SLAVE_ID)
    except TypeError:
        return cli.write_registers(address=address, values=values, slave=SLAVE_ID)

def _read_setpoints_values(cli, sps):
    values = {}
    for sp in sps:
        name = sp.get("name")
        addr = int(sp.get("mw"))
        dtype = (sp.get("dtype") or "INT16").upper()
        try:
            if dtype == "INT16":
                rr = _mb_read_holding(cli, address=addr, count=1)
                if rr is None or (hasattr(rr, "isError") and rr.isError()):
                    return values, f"Read error @%MW{addr}: {getattr(rr,'exception_code', rr)}"
                values[name] = int(rr.registers[0])
            else:
                rr = _mb_read_holding(cli, address=addr, count=2)
                if rr is None or (hasattr(rr, "isError") and rr.isError()):
                    return values, f"Read error @%MW{addr}..{addr+1}: {getattr(rr,'exception_code', rr)}"
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
    cur_cal    = request.args.get("cal", "all").strip().lower()  # default to (all)

    tags = list_tags_with_labels()  # list of {'tag','label'}
    selections = {
        "tag": cur_tag,
        "limit": cur_limit,
        "bucket_s": cur_bucket,
        "cal": cur_cal,
    }
    return render_template(
        "home.html",
        title="PLC Logger UI",
        tags=tags,
        selections=selections
    )

@ui_bp.route("/status_page")
def status_page():
    s = read_state()
    # add local strings
    if s.get("last_read_epoch") is not None:
        s["last_read_epoch_local"] = fmt_local_epoch(s.get("last_read_epoch"))
    if s.get("last_flush_epoch") is not None:
        s["last_flush_epoch_local"] = fmt_local_epoch(s.get("last_flush_epoch"))

    # CHANGED: use chunk root + new cap key
    storage = get_storage_status(DB_ROOT, RETENTION.get("total_cap_mb", 512))
    return render_template("status.html", title="Status", s=s, storage=storage)

@ui_bp.route("/setpoints", methods=["GET", "POST"])
def setpoints():
    msg = ""
    labels = tag_label_map()
    sps = fetch_setpoints()  # from config.py (list of dicts describing setpoints)

    if not sps:
        msg = "No setpoints configured. Add SETPOINTS in config.py."
        return render_template("setpoints.html", title="Setpoints",
                               msg=msg, labels=labels, sps=[], values={})

    # Handle write (POST)
    if request.method == "POST":
        name = (request.form.get("name") or (request.json or {}).get("name") or "").strip()
        value = (request.form.get("value") or (request.json or {}).get("value") or "").strip()
        sp = next((x for x in sps if x.get("name") == name), None)

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
            if err:
                msg = f"Write failed for {pretty}: {err}"
            else:
                msg = f"Updated {pretty}"

    # Always read fresh values (GET or after POST)
    values, emsg = _with_modbus(lambda cli: _read_setpoints_values(cli, sps))
    if isinstance(values, tuple):  # (_vals, err) from helper
        values, read_err = values
        emsg = emsg or read_err
    if emsg and not msg:
        msg = emsg

    return render_template("setpoints.html", title="Setpoints",
                           msg=msg, labels=labels, sps=sps, values=values or {})
