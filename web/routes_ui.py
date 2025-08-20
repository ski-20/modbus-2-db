# routes_ui.py
from flask import Blueprint, request, jsonify, make_response, render_template
from db_utils import (
    list_tags, tag_label_map, query_logs, download_csv,
    read_state, fetch_setpoints, fmt_local_epoch
)
from modbus_utils import mb_client, float_to_words, read_setpoint_block_dyn

ui_bp = Blueprint("ui", __name__)

@ui_bp.route("/")
def home():
    cur_tag    = request.args.get("tag", "").strip()
    cur_mins   = request.args.get("mins", "60")
    cur_limit  = request.args.get("limit", "500")
    cur_bucket = request.args.get("bucket_s", "")

    tags = list_tags()
    labels = tag_label_map()
    selections = {
        "tag": cur_tag,
        "mins": cur_mins,
        "limit": cur_limit,
        "bucket_s": cur_bucket,
    }
    return render_template("home.html",
                           title="PLC Logger UI",
                           tags=tags,
                           labels=labels,
                           selections=selections)

@ui_bp.route("/status_page")
def status_page():
    s = read_state()
    # add local strings
    if s.get("last_read_epoch") is not None:
        s["last_read_epoch_local"] = fmt_local_epoch(s.get("last_read_epoch"))
    if s.get("last_flush_epoch") is not None:
        s["last_flush_epoch_local"] = fmt_local_epoch(s.get("last_flush_epoch"))
    return render_template("status.html", title="Status", s=s)

@ui_bp.route("/setpoints", methods=["GET","POST"])
def setpoints():
    msg = ""
    labels = tag_label_map()
    sps = fetch_setpoints()  # [{'name','label','unit','mw','dtype'}, ...]

    if request.method == "POST":
        name = (request.form.get("name") or (request.json or {}).get("name") or "").strip()
        value = (request.form.get("value") or (request.json or {}).get("value") or "").strip()
        sp = next((x for x in sps if x["name"] == name), None)
        try:
            fval = float(value)
        except:
            fval = None

        if not sp or fval is None:
            msg = "Invalid name or value"
        else:
            c = mb_client()
            if not c:
                return make_response("Modbus not enabled on server", 500)
            try:
                if sp["dtype"].upper() == "INT16":
                    r = c.write_register(address=sp["mw"], value=int(fval), slave=1)
                    ok = not (hasattr(r,"isError") and r.isError())
                else:
                    hi, lo = float_to_words(fval)
                    r = c.write_registers(address=sp["mw"], values=[hi, lo], slave=1)
                    ok = not (hasattr(r,"isError") and r.isError())
                pretty = labels.get(name, sp.get("label") or name)
                msg = f"Updated {pretty}" if ok else f"Write failed for {pretty}"
            except Exception as e:
                msg = f"Write exception: {e}"

    values, emsg = read_setpoint_block_dyn(sps)
    if emsg and not msg:
        msg = emsg

    return render_template("setpoints.html",
                           title="Setpoints",
                           msg=msg,
                           labels=labels,
                           sps=sps,
                           values=values)
