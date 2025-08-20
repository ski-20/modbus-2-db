# API routes: /api/logs, /api/download.csv

from flask import Blueprint, request, jsonify, make_response
from .db import query_logs, download_csv, tag_label_map, _pretty_tag_fallback

api_bp = Blueprint("api", __name__)

@api_bp.route("/logs")
def api_logs():
    tag   = request.args.get("tag","").strip() or None
    mins  = int(request.args.get("mins","60") or 60)
    limit = int(request.args.get("limit","500") or 500)
    bucket_s = request.args.get("bucket_s","").strip()
    bucket_s = int(bucket_s) if bucket_s.isdigit() else None

    rows = query_logs(tag, mins, limit, bucket_s)

    # attach pretty display name
    labels = tag_label_map()
    for r in rows:
        r["tag_label"] = labels.get(r["tag"]) or _pretty_tag_fallback(r["tag"])
    return jsonify(rows)

@api_bp.route("/download.csv")
def api_download_csv():
    tag   = request.args.get("tag","").strip() or None
    mins  = int(request.args.get("mins","60") or 60)
    limit = int(request.args.get("limit","100000") or 100000)
    bucket_s = request.args.get("bucket_s","").strip()
    bucket_s = int(bucket_s) if bucket_s.isdigit() else None

    rows = query_logs(tag, mins, limit, bucket_s)
    csv_text = download_csv(rows)
    resp = make_response(csv_text)
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=logs.csv"
    return resp
