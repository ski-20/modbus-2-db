# routes_ui.py
from flask import Blueprint, request, jsonify, make_response, render_template

from .db import (
    list_tags_with_labels,    # <-- use objects (tag,label)
    tag_label_map,
    query_logs, download_csv, read_state, fetch_setpoints, fmt_local_epoch
)
from .modbus import mb_client, float_to_words, read_setpoint_block_dyn

#strage status imports
from .storage_status import get_storage_status
try:
    from config import DB, RETENTION
except Exception:
    DB = "/home/ele/plc_logger/plc.db"
    RETENTION = {}

ui_bp = Blueprint("ui", __name__)

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

    storage = get_storage_status(DB, RETENTION.get("max_db_mb", 512))
    return render_template("status.html", title="Status", s=s, storage=storage)

@ui_bp.route("/setpoints", methods=["GET", "POST"])
def setpoints():
    msg = ""
    labels = tag_label_map()
    sps = fetch_setpoints()  # from config.py

    if not sps:
        msg = "No setpoints configured. Add SETPOINTS in config.py."
        return render_template("setpoints.html", title="Setpoints",
                               msg=msg, labels=labels, sps=[], values={})

    if request.method == "POST":
        name = (request.form.get("name") or (request.json or {}).get("name") or "").strip()
        value = (request.form.get("value") or (request.json or {}).get("value") or "").strip()
        sp = next((x for x in sps if x["name"] == name), None)
        try:
            fval = float(value)
        except Exception:
            fval = None

        if not sp or fval is None:
            msg = "Invalid name or value"
        else:
            c = mb_client()
            if not c:
                return make_response("Modbus not enabled on server", 500)
            try:
                if sp["dtype"].upper() == "INT16":
                    r = c.write_register(address=sp["mw"], value=int(fval))
                    ok = not (hasattr(r, "isError") and r.isError())
                else:
                    hi, lo = float_to_words(fval)
                    r = c.write_registers(address=sp["mw"], values=[hi, lo])
                    ok = not (hasattr(r, "isError") and r.isError())
                pretty = labels.get(name, sp.get("label") or name)
                msg = f"Updated {pretty}" if ok else f"Write failed for {pretty}"
            except Exception as e:
                msg = f"Write exception: {e}"

    values, emsg = read_setpoint_block_dyn(sps)
    if emsg and not msg:
        msg = emsg

    return render_template("setpoints.html", title="Setpoints",
                           msg=msg, labels=labels, sps=sps, values=values)